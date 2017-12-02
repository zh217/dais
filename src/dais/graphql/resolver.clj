(ns dais.graphql.resolver
  (:require [clojure.core.async :as a]
            [taoensso.timbre :refer [debug info error]]
            [com.walmartlabs.lacinia.executor :as executor]
            [qbits.alia.async :as alia-async]
            [qbits.alia :as alia]
            [cheshire.core :as json]
            [io.aviso.ansi :refer [reset-font red bold-blue bold-red blue bold-white cyan bold-cyan]])
  (:import (clojure.lang ExceptionInfo)
           (com.datastax.driver.core.exceptions InvalidQueryException)))

(set! *warn-on-reflection* true)

(defmacro go-resolve
  [& body]
  `(a/go
     (try
       (let [result# (do ~@body)]
         {:data result#})
       (catch ExceptionInfo ex#
         (debug ex#)
         {:error {:message (str (.getMessage ex#))
                  :details (into {} (for [[k# v#] (ex-data ex#)] [k# (if (keyword? v#)
                                                                       (name v#)
                                                                       (str v#))]))}})
       (catch Throwable ex#
         (error ex#)
         {:error {:message (.getMessage ex#)}}))))

(defmacro thread-resolve
  [& body]
  `(a/thread
     (try
       (let [result# (do ~@body)]
         {:data result#})
       (catch ExceptionInfo ex#
         (debug ex#)
         {:error {:message (str (.getMessage ex#))
                  :details (into {} (for [[k# v#] (ex-data ex#)] [k# (if (keyword? v#)
                                                                       (name v#)
                                                                       (str v#))]))}})
       (catch Throwable ex#
         (error ex#)
         {:error {:message (.getMessage ex#)}}))))

(defn throw-if-error
  [v]
  (if (instance? Throwable v)
    (throw v)
    v))

(defn selection-set
  [ctx aliases]
  (let [selection (map (comp keyword name) (executor/selections-seq ctx))
        alias-selection (when aliases
                          (mapcat
                            (fn [v]
                              (cond
                                (keyword? v) [v]
                                (coll? v) v
                                :else []))
                            (map aliases selection)))]
    (set (concat selection alias-selection))))

(defn prune-columns
  ([ctx col-defs]
   (prune-columns ctx col-defs nil))
  ([ctx col-defs aliases]
   (->> (keys col-defs)
        (filter (selection-set ctx aliases)))))

(defn prune-values
  [m]
  (for [[k v] m
        :when (not (nil? v))]
    [k v]))

(defn ex!
  ([ctx query]
   (ex! ctx query nil {}))
  ([ctx query values]
   (ex! ctx query values {}))
  ([{:keys [cs-sess get-prepared]} query values opts]
   (debug (blue "QUERY") query values)
   (alia-async/execute-chan cs-sess
                            (if (map? query)
                              (get-prepared query)
                              query)
                            (assoc opts :values values))))

(defn ex-sync
  ([ctx query]
   (ex-sync ctx query nil {}))
  ([ctx query values]
   (ex-sync ctx query values {}))
  ([{:keys [cs-sess get-prepared]} query values opts]
   (debug (blue "QUERY") query values)
   (alia/execute cs-sess
                 (if (map? query)
                   (get-prepared query)
                   query)
                 (assoc opts :values values))))

(defn ex!-par
  ([ctx qvs]
   (ex!-par ctx qvs {}))
  ([{:keys [cs-sess]} qvs opts]
   (debug (blue "QUERY-PAR") (vec qvs))
   (for [qv qvs]
     (let [[query values] (if (vector? qv) qv [qv])]
       (alia-async/execute-chan cs-sess query (assoc opts :values values))))))

(defmacro <!ex-par
  [& args]
  `(loop [[c# & chs#] (ex!-par ~@args)
          res# []]
     (if c#
       (recur chs# (concat res# (a/<! c#)))
       res#)))

(defmacro <!!ex-par
  [& args]
  `(loop [[c# & chs#] (ex!-par ~@args)
          res# []]
     (if c#
       (recur chs# (concat res# (a/<!! c#)))
       res#)))

(defmacro <!ex
  [& args]
  `(-> (ex! ~@args)
       (a/<!)
       (throw-if-error)))

(defmacro <!!ex
  [& args]
  `(-> (ex-sync ~@args)
       (throw-if-error)))

(defn singleton
  ([res]
   (singleton res nil))
  ([res error-info]
   (if (= 1 (count res))
     (first res)
     (throw (or error-info (ex-info "wrong number of items for singleton" {:count (count res)}))))))

(defn singleton-or-nil
  ([res]
   (singleton-or-nil res nil))
  ([res error-info]
   (if (>= 1 (count res))
     (first res)
     (throw (or error-info (ex-info "wrong number of items for singleton-or-nil" {:count (count res)}))))))

(defmacro <!ex1
  [& args]
  `(-> (<!ex ~@args)
       (singleton)))

(defmacro <!!ex1
  [& args]
  `(-> (<!!ex ~@args)
       (singleton)))

(defmacro <!ex1?
  [& args]
  `(-> (<!ex ~@args)
       (singleton-or-nil)))

(defmacro <!!ex1?
  [& args]
  `(-> (<!!ex ~@args)
       (singleton-or-nil)))

(defmacro <!ex-ret?
  [& args]
  `(-> (<!ex ~@args)
       (first)
       (get ~(keyword "[applied]"))))

(defmacro <!!ex-ret?
  [& args]
  `(-> (<!!ex ~@args)
       (first)
       (get ~(keyword "[applied]"))))

(defmacro <!ex-ret!
  [& args]
  `(let [ret# (-> (<!ex ~@args)
                  (first))]
     (when-not (get ret# ~(keyword "[applied]"))
       (throw (ex-info "change is not applied" (or ret# {:data false}))))))

(defmacro <!!ex-ret!
  [& args]
  `(let [ret# (-> (<!!ex ~@args)
                  (first))]
     (when-not (get ret# ~(keyword "[applied]"))
       (throw (ex-info "change is not applied" (or ret# {:data false}))))))

(defn serialize
  [v]
  (when v
    (json/generate-string v)))

(defn deserialize
  [v]
  (when v
    (json/parse-string-strict v)))

(defn val-resolver
  ([f]
   (val-resolver f nil nil false))
  ([f val-key]
   (val-resolver f val-key nil false))
  ([f val-key arg-key]
   (val-resolver f val-key arg-key false))
  ([f val-key arg-key compound-key]
   (let [val-key (or val-key :id)
         arg-key (or arg-key :id)
         new-f (fn [ctx args vals]
                 (if (and (not compound-key) (map? (arg-key vals)))
                   (arg-key vals)
                   (when-let [key (val-key vals)]
                     (let [fields (selection-set ctx nil)]
                       (if (and (= 1 (count fields))
                                (= (first fields) arg-key))
                         {arg-key key}
                         (f ctx
                            (assoc (or args {})
                              arg-key key)
                            vals))))))]
     (with-meta
       new-f
       (merge (meta new-f)
              (select-keys (meta f) [:graphql/fn-tag :graphql/op-timeout]))))))

(defn prune-map
  [m]
  (into {}
        (for [[k v] m
              :when (not (nil? v))]
          [k v])))

(defn keymap
  [m]
  (into {}
        (for [[k _] m]
          [k k])))