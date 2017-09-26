(ns
  ^{:author "Ziyang Hu"}
  dais.graphql.dataloader
  (:require [clojure.core.async :as a]
            [clojure.set :as set]
            [taoensso.timbre :refer [info debug trace error]]
            [com.walmartlabs.lacinia.executor :as executor]
            [dais.postgres.query-helpers :as h]
            [com.walmartlabs.lacinia.resolve :as resolve])
  (:import (com.walmartlabs.lacinia.resolve ResolverResult)
           (clojure.lang ExceptionInfo)
           (java.util.concurrent TimeUnit TimeoutException Future ExecutionException)))

(defonce active-dataloaders (atom {}))

(defonce worker-timeout 5000)

(defn- worker-with-timeout
  [f ctx params]
  (let [res (future
              (if-let [db-pool (:db-pool ctx)]
                (h/with-conn [c db-pool]
                  (f (assoc ctx :db c) params))
                (f ctx params)))
        ret (a/promise-chan)]
    (try
      (a/>!! ret (.get ^Future res worker-timeout TimeUnit/MILLISECONDS))
      (catch ExecutionException ex
        (let [nx-ex (.getCause ex)]
          (a/>!! ret {::worker-exception nx-ex
                      ::cause            "worker has thrown an exception: "})))
      (catch TimeoutException _
        (error "dataloader worker timeout" f params ctx)
        (a/>!! ret {::worker-exception params
                    ::cause            "worker has reached a timeout: "}))
      (catch Throwable ex
        (error "dataloader worker unknown exception" f params ctx)
        (a/>!! ret {::worker-exception ex
                    ::cause            "worker has unexpected behaviour: "}))
      (finally
        (a/close! ret)
        (future-cancel res)))
    ret))

(defn make-dataloader
  [ctx {:keys [key-processor identification]
        :or   {key-processor identity}}]
  (let [dataloader-chan (a/chan 1024)
        cache (atom {})]
    (trace "init dataloader" dataloader-chan identification)
    (swap! active-dataloaders assoc dataloader-chan identification)
    (a/go-loop [pending {}
                sentinel? false]
      ;(trace "dataloader loop starting with" pending sentinel? dataloader-chan)
      (let [data (a/<! dataloader-chan)]
        (trace "dataloader loop" data)
        (if data
          (cond
            ;; first case: force sentinel or two sentinels in a row
            ;; flush everything
            (or (= data ::force-sentinel)
                (and sentinel? (= data ::sentinel)))
            (do
              (trace "dataloader pending requests:" (count pending))
              (doseq [[[batch-fn key-fn key-proc] vs] pending]
                (let [ks (set (map first vs))]
                  (let [result (a/<! (worker-with-timeout batch-fn ctx ks))]
                    (if-let [error-data (::worker-exception result)]
                      (doseq [[k c] vs]
                        (a/>! c {::error (str (::cause result) error-data)})
                        (a/close! c))
                      (let [result (into {}
                                         (for [row result]
                                           [((or key-proc key-processor) (key-fn row)) row]))
                            missing (into {}
                                          (for [k (set/difference ks (set (keys result)))]
                                            [k ::not-found]))]
                        (when (seq result)
                          (trace "dataloader result" result)
                          (swap! cache update [batch-fn key-fn key-proc] merge result))
                        (when (seq missing)
                          (trace "dataloader missing values" missing)
                          (swap! cache update [batch-fn key-fn key-proc] merge missing))
                        (trace "dataloader cache" @cache)
                        (doseq [[k c] vs]
                          (let [v (get-in @cache [[batch-fn key-fn key-proc] k])]
                            (when-not (= v ::not-found)
                              (trace "dataloader putting new value" k v)
                              (a/>! c v)))
                          (a/close! c)))))))
              (recur {} false))

            ;; second case: one sentinel
            ;; wait for the next data or sentinel
            (= data ::sentinel)
            (do
              (trace "dataloader deque sentinel")
              (a/>! dataloader-chan ::sentinel)
              (recur pending true))

            :else
            (let [{:keys [batch-fn key-fn key-proc ret-chan key]} data]
              (if-let [cache-value (get-in @cache [[batch-fn key-fn key-proc] ((or key-proc key-processor) key)])]
                ;; third case: incoming request has applicable cache
                (do
                  (trace "dataloader applicable cache found")
                  (when-not (= cache-value ::not-found)
                    (trace "dataloader putting cached value" key cache-value)
                    (a/>! ret-chan cache-value))
                  (a/close! ret-chan)
                  (a/>! dataloader-chan ::sentinel)
                  (recur pending false))
                ;; fourth case: incoming request does not have applicable cache
                (do
                  (if (> (count (mapcat second pending)) 512)
                    (do
                      (trace "dataloader queue force sentinel")
                      (a/>! dataloader-chan ::force-sentinel))
                    (do
                      (trace "dataloader queue sentinel")
                      (a/>! dataloader-chan ::sentinel)))
                  (trace "dataloader queuing complete")
                  (recur
                    (update pending [batch-fn key-fn key-proc] conj [((or key-proc key-processor) key) ret-chan])
                    false)))))
          ;; dataloader is closed
          (do
            (trace "dataloader cache at closing" @cache)
            (doseq [[_ vs] pending
                    [k c] vs]
              (trace "dataloader cancelling" k)
              (a/>! c {::cancelled k})
              (a/close! c))
            (swap! active-dataloaders dissoc dataloader-chan)))))
    dataloader-chan))

(when-not *compile-files*
  (a/go-loop [c 1]
    (a/<! (a/timeout 5000))
    (if (= 0 (rem c 12))
      (info "active dataloaders:" (count @active-dataloaders) @active-dataloaders)
      (debug "active dataloaders:" (count @active-dataloaders) @active-dataloaders))
    (recur (inc c))))

(defn batch-loader
  [{:keys [batch value-> arg-> extract-fn key-fn key-proc] :or {key-fn :id}}]
  (trace "Making dataloader with" batch value-> arg->)
  ^ResolverResult ^:graphql/no-wrap
  (fn [{:keys [dataloader] :as ctx} args value]
    (try
      (if-let [key (cond
                     value-> (value-> value)
                     arg-> (arg-> args)
                     extract-fn (extract-fn ctx args value))]
        (let [selections (executor/selections-seq ctx)]
          (trace "dataloader Batch" key key-fn (vec selections))
          (if (and (keyword? key-fn)
                   (= 1 (count selections))
                   (= (name key-fn) (name (first selections)))
                   (nil? value))
            (resolve/resolve-as {key-fn key})
            (let [resolve-promise (resolve/resolve-promise)
                  ret-chan (a/promise-chan)
                  payload {:batch-fn batch
                           :ret-chan ret-chan
                           :key      key
                           :key-fn   key-fn
                           :key-proc key-proc}]
              (trace "dataloader is called with payload" payload dataloader)
              (a/>!! dataloader payload)
              (trace "dataloader put complete" payload dataloader)
              (a/take! ret-chan
                       (fn [value]
                         (trace "dataloader resolve result" value)
                         (cond
                           (::error value) (resolve/deliver! resolve-promise nil {:message (::error value)})
                           (::cancelled value) (resolve/deliver! resolve-promise nil {:message (str "batchloader cancelled due to timeout" (::cancelled value))})
                           :else (resolve/deliver! resolve-promise value nil))))
              resolve-promise)))
        (resolve/resolve-as nil))
      (catch ExceptionInfo ex
        (resolve/resolve-as nil {:message (.getMessage ex)}))
      (catch Throwable ex
        (error ex)
        (resolve/resolve-as nil {:message (str ex)})))))