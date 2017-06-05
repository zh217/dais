(ns
  ^{:author "Ziyang Hu"}
  dais.elasticsearch.indexer
  (:require [clojure.core.async :as a]
            [taoensso.timbre :refer [info debug error trace]]
            [qbits.spandex :as s]
            [qbits.spandex.utils :as s-utils]))

(defn start-indexer
  [conn {:keys [bulk-request-conf on-start on-error streaming-pub processors]}]
  (let [{:keys [input-ch output-ch]}
        (s/bulk-chan conn (merge
                            bulk-request-conf
                            {:headers {:Content-Type "application/x-ndjson"}}))]
    (a/go-loop []
      (when-let [[reqs resp] (a/<! output-ch)]
        (if (instance? Exception resp)
          (do
            (error resp)
            (on-error (merge
                        {:requests      reqs
                         :error_message (str resp)}
                        (:body (ex-data resp)))))
          (trace resp))
        (recur)))
    (on-start conn)
    (into {}
          (for [[model processor] processors]
            (let [sub-chan (a/chan 128)]
              (a/sub streaming-pub model sub-chan)
              (a/go-loop []
                (if-let [val (a/<! sub-chan)]
                  (do
                    (when-let [v (processor val)]
                      (a/>! input-ch v))
                    (recur))
                  (do
                    (a/close! input-ch)
                    (a/close! output-ch)
                    (info "closed indexer for" model))))
              (info "started indexer for" model)
              {model sub-chan})))))

(defn stop-indexer
  [indexer]
  (doseq [[_ c] indexer]
    (a/close! c)))