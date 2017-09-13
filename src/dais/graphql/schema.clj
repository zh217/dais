(ns
  ^{:author "Ziyang Hu"}
  dais.graphql.schema
  (:require [clojure.walk :as walk]
            [clojure.core.async :as a]
            [taoensso.timbre :refer [error]]
            [com.walmartlabs.lacinia.resolve :as resolve])
  (:import (clojure.core.async.impl.protocols ReadPort)
           (com.walmartlabs.lacinia.resolve ResolverResult)))

(defn deep-merge
  [ms]
  (apply merge-with
         (fn [v1 v2]
           (cond
             (nil? v1) v2
             (nil? v2) v1
             (and (map? v1) (map? v2)) (deep-merge [v1 v2])
             :else (throw (ex-info "cannot merge branches that are not maps" {:v1 v1 :v2 v2}))))
         ms))

(defn- normalize-type
  [t]
  (if (keyword? t)
    (let [tt (name t)
          [_ base-type modifiers] (re-matches #"^(.*[^\!\*])([\!\*]*)$" tt)]
      (reduce
        (fn [acc nx]
          (case nx
            \* (list 'list acc)
            \! (list 'non-null acc)))
        (keyword base-type)
        modifiers))
    t))

(defn normalize-all-types
  [schema]
  (walk/postwalk
    (fn [f]
      (if (sequential? f)
        (case (first f)
          :type [:type (normalize-type (second f))]
          :resolve [:resolve (if-let [orig-fn (second f)]
                               (if (:graphql/no-wrap (meta orig-fn))
                                 orig-fn
                                 ^ResolverResult (fn [ctx args vals]
                                                   (try
                                                     (resolve/resolve-as (orig-fn ctx args vals) nil)
                                                     (catch Throwable ex
                                                       (error ex)
                                                       (resolve/resolve-as nil {:message (str ex)})))))
                               ^ResolverResult (fn [_ _ _]
                                                 (resolve/resolve-as nil {:message "unimplemented"})))]
          :stream [:stream (if-let [orig-fn (second f)]
                             (if (:graphql/no-wrap (meta orig-fn))
                               orig-fn
                               ^ResolverResult (fn [ctx args vals]
                                                 (try
                                                   (resolve/resolve-as (orig-fn ctx args vals) nil)
                                                   (catch Throwable ex
                                                     (error ex)
                                                     (resolve/resolve-as nil {:message (str ex)})))))
                             ^ResolverResult (fn [_ _ _]
                                               (resolve/resolve-as nil {:message "unimplemented"})))]
          f)
        f))
    schema))

(defn ^:private chan?
  [v]
  (instance? ReadPort v))

(defn core-async-decorator
  [object-name field-name f]
  (throw (ex-info "this is now useless" {:message "deprecated"}))
  #_(fn [context args value]
      (let [result (f context args value)]
        (if-not (chan? result)
          result
          (let [resolve-promise (resolve/resolve-promise)]
            (a/take! result
                     (fn [value]
                       (resolve/deliver! resolve-promise value nil)))
            resolve-promise)))))
