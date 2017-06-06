(ns
  ^{:author "Ziyang Hu"}
  dais.elasticsearch.request-helpers
  (:refer-clojure :exclude [get])
  (:require [qbits.spandex :as s]
            [qbits.spandex.utils :as s-utils])
  (:import (java.util UUID)))

(defn request
  [client method path body]
  (try
    (s/request client
               {:url     (s-utils/url path)
                :method  method
                :headers {:Content-Type "application/json; charset=UTF8"}
                :body    body})
    (catch Exception ex
      (if-let [data (ex-data ex)]
        data
        (throw ex)))))