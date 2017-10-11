(ns dais.elasticsearch.request-helpers
  {:author "Ziyang Hu"}
  (:refer-clojure :exclude [get])
  (:require [taoensso.timbre :refer [error]]
            [qbits.spandex :as s]
            [qbits.spandex.url :as url])
  (:import (java.util UUID)))

(defn request
  [client method path body]
  (try
    (s/request client
               {:url    (url/encode path)
                :method method
                :body   body})
    (catch Exception ex
      (if-let [data (ex-data ex)]
        data
        (throw ex)))))