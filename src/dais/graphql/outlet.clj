(ns dais.graphql.outlet
  (:require [clojure.string :as str]
            [clojure.core.async :as a]
            [taoensso.timbre :refer [trace debug info warn error]]
            [ring.adapter.jetty9.websocket :as ws]
            [cheshire.core :as json]
            [io.aviso.ansi :refer [reset-font magenta red bold-blue bold-red blue bold-white cyan bold-cyan yellow]]
            [com.walmartlabs.lacinia.executor :as executor]
            [com.walmartlabs.lacinia.constants :as constants]
            [com.walmartlabs.lacinia.resolve :as resolve]
            [com.walmartlabs.lacinia :as lacinia]
            [com.walmartlabs.lacinia.parser :as parser])
  (:import (clojure.lang ExceptionInfo)
           (java.util UUID)))

(defn- pack-data-complete
  [id data]
  [{:type    :data
    :id      id
    :payload data}
   {:type :complete
    :id   id}])

(defn- identifier
  [obj]
  (System/identityHashCode obj))


(defn- to-message
  [^Throwable t]
  (let [m (.getMessage t)]
    (if-not (str/blank? m)
      m
      (-> t .getClass .getName))))

(defn- as-error-map
  ([^Throwable t]
   (as-error-map t nil))
  ([^Throwable t more-data]
   (merge {:message (to-message t)}
          (ex-data t)
          more-data)))

(defn- as-errors
  [exception]
  {:errors [(as-error-map exception)]})

(defn- invoke-streamer
  "Given a parsed and prepared query (inside the context, as with [[execute-query]]),
  this will locate the streamer for a subscription
  and invoke it, passing it the context, the subscription arguments, and the source stream."
  {:added "0.19.0"}
  [context args source-stream]
  (let [parsed-query (get context constants/parsed-query-key)
        {:keys [selections operation-type]} parsed-query
        selection (do
                    (assert (= :subscription operation-type))
                    (first selections))
        streamer (get-in selection [:field-definition :stream])]
    (streamer context args source-stream)))


(defn- execute-subscription
  [send-ch msg-id parsed-query {:keys [query variables operationName]} conn-context conn-id]
  (info (yellow (str "gql-sub: " operationName)) msg-id variables conn-id)
  (let [collect-ch (a/chan 1)
        clean-up-fn (invoke-streamer
                      (assoc conn-context constants/parsed-query-key parsed-query)
                      variables
                      collect-ch)
        stop-ch (a/promise-chan)]
    (a/go-loop []
      (let [[value port] (a/alts! [stop-ch collect-ch] :priority true)]
        (if value
          (do
            (debug "obtained subscription data" value)
            (-> (executor/execute-query
                  (assoc conn-context constants/parsed-query-key parsed-query))
                (resolve/on-deliver! (fn [response]
                                       (try
                                         (a/put! send-ch
                                                 {:type    :data
                                                  :id      msg-id
                                                  :payload response})
                                         (debug "ws resp" send-ch msg-id response)
                                         (catch Throwable ex
                                           (error "bad loop")
                                           (error ex))))))
            (recur))
          (do
            (info (red (str "gql-sub-stop: " operationName)) msg-id variables conn-id)
            (a/close! collect-ch)
            (clean-up-fn)))))
    stop-ch))

(defn- display-error
  [result {:keys [query variables operationName]} conn-id]
  (when-let [errs (:errors result)]
    (let [e-lines (into {} (for [{:keys [line column]} (mapcat :locations errs)]
                             [line column]))]
      (error (red (str "GraphQL Error: " operationName)) variables conn-id)
      (error (red "Details:") (json/generate-string errs {:pretty true}))
      (error (str (red "Query:\n")
                  (str/join "\n"
                            (map-indexed
                              (fn [idx l]
                                (let [index (inc idx)]
                                  (if-let [column (e-lines index)]
                                    (str (red (str (when (< idx 9) " ") index ": "))
                                         (subs l 0 column)
                                         (red (subs l column)))
                                    (str (cyan (str (when (< idx 9) " ") index ": ")) l))))
                              (str/split-lines query))))))))

(defn- execute-query-socket
  [send-ch msg-id parsed-query {:keys [query variables operationName]} conn-context conn-id]
  (let [start-at (System/currentTimeMillis)
        result (lacinia/execute-parsed-query parsed-query variables conn-context)]
    (display-error result {:query         query
                           :variables     variables
                           :operationName operationName} conn-id)
    (a/put! send-ch (pack-data-complete msg-id result))
    (let [time (- (System/currentTimeMillis) start-at)]
      (info (cyan (str "ws-gql: " operationName)) msg-id variables conn-id
            (str (cond-> time
                         (> time 50) (red))
                 "ms")))))

(defn execute-query
  [model {:keys [operationName query variables] :as params} conn-context conn-id]
  (trace "called internal API with" conn-id operationName query variables)
  (let [result (lacinia/execute model query variables
                                conn-context
                                {:operation-name operationName})]
    (display-error result params nil)
    result))

(defn socket-api-handler
  [{:keys [graphql-model
           authenticate-connection
           idle-timeout
           make-conn-id
           make-conn-context]}]
  (let [connected-clients (atom {})]
    {:on-connect (fn [ws]
                   (debug "socket connect" (identifier ws))
                   (ws/idle-timeout! ws 1000))
     :on-error   (fn [ws e]
                   (warn "socket error" (identifier ws) e))
     :on-close   (fn [ws status-code reason]
                   (debug "socket close" (identifier ws) status-code reason)
                   (when-let [{:keys [send-ch stop-chs creds]} (get @connected-clients ws)]
                     (a/close! send-ch)
                     (doseq [[_ c] stop-chs]
                       (a/close! c))
                     (info (magenta "ws-disconnect") creds))
                   (swap! connected-clients dissoc ws))
     :on-text    (fn [ws text-message]
                   (let [{msg-type :type msg-id :id payload :payload :as parsed-msg} (json/parse-string-strict text-message true)]
                     (if-let [socket-data (get @connected-clients ws)]
                       ;; has socket data
                       (case msg-type
                         "start" (let [{:keys [query variables operationName]} payload
                                       {:keys [creds user roles send-ch stop-chs]} socket-data]
                                   (debug "socket process start" (identifier ws) msg-id payload socket-data)
                                   (let [[parsed-query error-result] (try
                                                                       (debug query operationName)
                                                                       [(parser/parse-query graphql-model query operationName)]
                                                                       (catch ExceptionInfo e
                                                                         (error e)
                                                                         [nil (as-errors e)]))]
                                     (if (some? error-result)
                                       (do
                                         (a/put! send-ch (pack-data-complete msg-id error-result)))
                                       (if (= (-> parsed-query parser/operations :type) :subscription)
                                         (let [stop-ch (execute-subscription send-ch msg-id parsed-query payload
                                                                             (make-conn-context socket-data)
                                                                             (make-conn-id socket-data))]
                                           (swap! connected-clients assoc-in [ws :stop-chs msg-id] stop-ch))
                                         (execute-query-socket send-ch msg-id parsed-query payload
                                                               (make-conn-context socket-data)
                                                               (make-conn-id socket-data))))))
                         "stop" (when-let [stop-ch (get-in @connected-clients [ws :stop-chs msg-id])]
                                  (a/close! stop-ch)
                                  (debug "socket process stop" (identifier ws) msg-id payload socket-data))
                         "connection_terminate" (do
                                                  (info "client required termination" (identifier ws))
                                                  (ws/close! ws))
                         (do
                           (a/put! (:send-ch socket-data)
                                   (cond-> {:type    :error
                                            :payload {:message "Unrecognized message type."
                                                      :type    type}}
                                           msg-id (assoc :id msg-id)))))
                       ;; no socket data found, try initiation
                       (if-let [context-map (authenticate-connection payload)]
                         (let [send-ch (a/chan 1)]
                           (a/go-loop []
                             (when-let [data (a/<! send-ch)]
                               (if (vector? data)
                                 (doseq [datum data]
                                   (ws/send! ws (json/generate-string datum)))
                                 (ws/send! ws (json/generate-string data)))
                               (recur)))
                           (info (magenta "ws-connect") (make-conn-id context-map))
                           (swap! connected-clients assoc ws
                                  (merge context-map
                                         {:send-ch  send-ch
                                          :stop-chs {}}))
                           (ws/idle-timeout! ws idle-timeout)
                           (a/put! send-ch {:type :connection_ack}))
                         (do
                           (warn "authentication failed" (identifier ws))
                           (ws/close! ws))))))
     :on-bytes   (fn [ws bytes offset len]
                   (warn "socket bytes" (identifier ws) bytes offset len))}))