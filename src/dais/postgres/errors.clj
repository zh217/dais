(ns dais.postgres.errors
  {:author "Ziyang Hu"}
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io])
  (:import (java.sql SQLException)))

(def error-codes
  (-> (io/resource "dais/postgres/errors.edn")
      (slurp)
      (edn/read-string)))

(defn ->error-key
  [^SQLException ex]
  (:keyword (error-codes (.getSQLState ex))))
