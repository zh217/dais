(ns
  ^{:author "Ziyang Hu"}
  dais.postgres.query-helpers
  (:refer-clojure :exclude [set update group-by array int-array cast])
  (:require [clojure.java.jdbc :as jdbc]
            [honeysql.core :as sql]
            [honeysql.format :as hf]
            [honeysql.helpers]
            [honeysql-postgres.helpers]
            [honeysql-postgres.format]                      ;; do not remove this, otherwise pg extensions will not work
            [taoensso.timbre :refer [debug report]]
            [io.aviso.ansi :refer [reset-font green red bold-blue bold-red blue bold-white cyan bold-cyan]]
            [cheshire.core :as json])
  (:import (org.joda.time DateTimeZone)
           (java.sql Array)
           (org.postgresql.util PGobject)))

(alter-var-root
  #'honeysql.format/*allow-dashed-names?*
  (constantly true))

(alter-var-root
  #'honeysql.format/infix-fns
  (fn [old]
    (conj old
          "&&" "||" "@>" "<@" "->" "->>" "#>" "#>>" "?" "?|" "?&" "#-")))

(defn ex!
  [db q-def & {:keys [params quoting parameterizer return-param-names]
               :or   {quoting :ansi parameterizer :jdbc}}]
  (debug "query-helper/ex!" q-def)
  (let [sql-vec (sql/format q-def
                            :quoting quoting
                            :parameterizer parameterizer
                            :params params
                            :return-param-names return-param-names)]
    (debug (green "SQL >>>") (first sql-vec) (green ">>>") (rest sql-vec))
    (jdbc/execute! db sql-vec)))

(defn q
  [db q-def & {:keys [params quoting parameterizer return-param-names]
               :or   {quoting :ansi parameterizer :jdbc}}]
  (debug "query-helper/q" q-def)
  (let [sql-vec (sql/format q-def
                            :quoting quoting
                            :parameterizer parameterizer
                            :params params
                            :return-param-names return-param-names)]
    (debug (green "SQL >>>") (first sql-vec) (green ">>>") (rest sql-vec))
    (jdbc/query db sql-vec)))

(defn q1
  "Call `q`, and take the first row. If the number of returned rows is not one, an error is raised."
  [& args]
  (let [result (apply q args)]
    (if (= 1 (count result))
      (first result)
      (throw (ex-info "wrong number of returned rows" {:error-type :bad-row-number-in-return
                                                       :ret        result})))))

(defn q1?
  "Call `q`, and take the first row. If the number of returned rows is more than one, an error is raised."
  [& args]
  (let [result (apply q args)]
    (if (>= 1 (count result))
      (first result)
      (throw (ex-info "wrong number of returned rows" {:error-type :bad-row-number-in-return
                                                       :ret        result})))))

(def ^{:doc "imported from `honeysql.helpers`"} select honeysql.helpers/select)
(def ^{:doc "imported from `honeysql.helpers`"} merge-select honeysql.helpers/merge-select)
(def ^{:doc "imported from `honeysql.helpers`"} from honeysql.helpers/from)
(def ^{:doc "imported from `honeysql.helpers`"} merge-from honeysql.helpers/merge-from)
(def ^{:doc "imported from `honeysql.helpers`"} where honeysql.helpers/where)
(def ^{:doc "imported from `honeysql.helpers`"} merge-where honeysql.helpers/merge-where)
(def ^{:doc "imported from `honeysql.helpers`"} join honeysql.helpers/join)
(def ^{:doc "imported from `honeysql.helpers`"} merge-join honeysql.helpers/merge-join)
(def ^{:doc "imported from `honeysql.helpers`"} left-join honeysql.helpers/left-join)
(def ^{:doc "imported from `honeysql.helpers`"} merge-left-join honeysql.helpers/merge-left-join)
(def ^{:doc "imported from `honeysql.helpers`"} right-join honeysql.helpers/right-join)
(def ^{:doc "imported from `honeysql.helpers`"} merge-right-join honeysql.helpers/merge-right-join)
(def ^{:doc "imported from `honeysql.helpers`"} full-join honeysql.helpers/full-join)
(def ^{:doc "imported from `honeysql.helpers`"} merge-full-join honeysql.helpers/merge-full-join)
(def ^{:doc "imported from `honeysql.helpers`"} group-by honeysql.helpers/group)
(def ^{:doc "imported from `honeysql.helpers`"} merge-group-by honeysql.helpers/merge-group-by)
(def ^{:doc "imported from `honeysql.helpers`"} having honeysql.helpers/having)
(def ^{:doc "imported from `honeysql.helpers`"} merge-having honeysql.helpers/merge-having)
(def ^{:doc "imported from `honeysql.helpers`"} order-by honeysql.helpers/order-by)
(def ^{:doc "imported from `honeysql.helpers`"} merge-order-by honeysql.helpers/merge-order-by)
(def ^{:doc "imported from `honeysql.helpers`"} limit honeysql.helpers/limit)
(def ^{:doc "imported from `honeysql.helpers`"} offset honeysql.helpers/offset)
(def ^{:doc "imported from `honeysql.helpers`"} insert-into honeysql.helpers/insert-into)
(def ^{:doc "imported from `honeysql.helpers`"} columns honeysql.helpers/columns)
(def ^{:doc "imported from `honeysql.helpers`"} merge-values honeysql.helpers/merge-values)
(def ^{:doc "imported from `honeysql.helpers`"} modifiers honeysql.helpers/modifiers)
(def ^{:doc "imported from `honeysql.helpers`"} values honeysql.helpers/values)
(def ^{:doc "imported from `honeysql.helpers`"} set honeysql.helpers/sset)
(def ^{:doc "imported from `honeysql.helpers`"} delete-from honeysql.helpers/delete-from)
(def ^{:doc "imported from `honeysql.core`"} raw honeysql.core/raw)
(def ^{:doc "imported from `honeysql.helpers`"} array honeysql.types/array)
(def ^{:doc "imported from `honeysql.helpers`"} update honeysql.helpers/update)
(def ^{:doc "imported from `honeysql-postgres.helpers`"} returning honeysql-postgres.helpers/returning)
(def ^{:doc "imported from `honeysql-postgres.helpers`"} upsert honeysql-postgres.helpers/upsert)
(def ^{:doc "imported from `honeysql-postgres.helpers`"} on-conflict honeysql-postgres.helpers/on-conflict)
(def ^{:doc "imported from `honeysql-postgres.helpers`"} do-update-set honeysql-postgres.helpers/do-update-set)
(def ^{:doc "imported from `honeysql-postgres.helpers`"} do-nothing honeysql-postgres.helpers/do-nothing)
(def ^{:doc "imported from `honeysql-postgres.helpers`"} do-update-set! honeysql-postgres.helpers/do-update-set!)
(def ^{:doc "imported from `honeysql.core`"} call honeysql.core/call)


(defn with-cte
  [& args]
  {:with (vec args)})

(def current-date (raw "current_date"))

(def ^:dynamic *default-time-zone* (DateTimeZone/forID "PRC"))

(defn int-array
  "Integer array for SQL"
  [elements]
  (array (map int elements)))

(defn array-agg
  [field]
  (call :array_agg field))

(defn agg-json-object
  "Produce a JSON array of JSON objects. Takes as argument a clojure map."
  [kvs]
  (call :jsonb_agg
        (apply call :jsonb_build_object
               (apply concat (for [[k v] kvs]
                               [(if (keyword? k) (name k) k) v])))))

(defn json-object
  "Produce a JSON object. Takes as argument a clojure map."
  [kvs]
  (apply call :jsonb_build_object
         (apply concat (for [[k v] kvs]
                         [(if (keyword? k) (name k) k) v]))))

(defn as-json
  [clj-val]
  (call :cast
        (json/generate-string clj-val)
        :jsonb))

(defn cast
  [var enum-type]
  (call :cast var enum-type))

(defn cast-enum
  [k enum-type]
  (when k
    (call :cast (name k) enum-type)))

(defn cast-enum-array
  [arr enum-type]
  (when arr
    (call :cast (array (for [e arr] (name e)))
          (raw (str (name enum-type) "[]")))))

(defn prefix-like
  [s]
  (str s "%"))

(defn infix-like
  [s]
  (str "%" s "%"))

(defmacro with-conn
  "Same as `clojure.java.jdbc/with-db-connection"
  [binding & body]
  `(clojure.java.jdbc/with-db-connection ~binding ~@body))

(defmacro with-tx
  "Same as `clojure.java.jdbc/with-db-transaction`"
  [binding & body]
  `(clojure.java.jdbc/with-db-transaction ~binding ~@body))

;; Make JDBC play well with array and json
(extend-protocol jdbc/IResultSetReadColumn
  PGobject
  (result-set-read-column [pgobj metadata idx]
    (let [type (.getType pgobj)
          value (.getValue pgobj)]
      (case type
        "json" (json/parse-string value true)
        "jsonb" (json/parse-string value true)
        value)))
  Array
  (result-set-read-column [arr metadata idx]
    (into [] (.getArray ^Array arr))))

;; pg @> operator
(defmethod hf/fn-handler "contains" [_ a b]
  (str (hf/to-sql a) " @> " (hf/to-sql b)))

(defmethod hf/fn-handler "extract" [_ field col]
  (str "EXTRACT" (hf/paren-wrap (str
                                  "'"
                                  (name field)
                                  "' FROM "
                                  (hf/to-sql col)))))