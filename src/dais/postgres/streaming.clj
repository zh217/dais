(ns dais.postgres.streaming
  {:author "Ziyang Hu"}
  (:require [clojure.core.async :as a :refer [<! >! <!! >!!]]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.reader.edn :as edn]
            [instaparse.core]
            [me.raynes.conch.low-level :as sh]
            [taoensso.timbre :refer [info error debug trace report fatal]]
            [io.aviso.ansi :refer [reset-font green red bold-blue bold-red blue bold-white cyan bold-cyan]]
            [cheshire.core :as json]))

(defn- make-streaming-proc [config]
  (let [env {"PGHOST"     (:host config)
             "PGPORT"     (str (:port config))
             "PGDATABASE" (:database config)
             "PGUSER"     (:username config)
             "PGPASSWORD" (:password config)}]
    (debug "streaming" (:recvlogical-exec config)
           "--no-loop"
           "--dbname" (:database config)
           "--slot" (:replication-slot config)
           "--start" "--file" "-")
    (sh/proc (:recvlogical-exec config)
             "--no-loop"
             "--dbname" (:database config)
             "--slot" (:replication-slot config)
             "--start" "--file" "-"
             :env env)))

(defn start-streaming-proc
  [{:keys [host port database username password
           recvlogical-exec replication-slot] :as conf}]
  (let [proc (make-streaming-proc conf)]
    (when (or (not (:err proc))
              (not (:out proc)))
      (throw (ex-info "error starting process" {:proc proc})))
    (debug "mounting streaming proc")
    (a/thread
      (try
        (with-open [rdr (io/reader (:err proc))]
          (doseq [line (line-seq rdr)]
            (error "streaming-proc:" line)))
        (catch Throwable e
          (error e)
          (throw e))))
    proc))

(defn stop-streaming-proc
  [streaming-proc]
  (try
    (sh/done streaming-proc)
    (Thread/sleep 100)
    (sh/destroy streaming-proc)
    (catch Throwable e
      (error e))))

(def ^:private array-parser
  (instaparse.core/parser
    "arr = <sp '{' sp> ((unquoted | quoted | squoted) (<sp sep sp> (unquoted | quoted | squoted))* )? <sp '}' sp>
     sp = #'\\s*'
     sep = ','
     unquoted = #'[^\\'\\\"\\s}\",]+'
     squoted = #'\\'([^\\']|\\'\\')*\\''
     quoted = #'\\\"(\\\\\\\"|[^\"])*\\\"'"))

(defn- parse-array
  [s]
  (when s
    (mapv
      (fn [[t v]]
        (case t
          :squoted (-> v
                       (str/replace #"^'" "")
                       (str/replace #"'$" "")
                       (str/replace "''" "'"))
          :quoted (edn/read-string v)
          :unquoted (if (= v "NULL") nil v)))
      (rest (array-parser s)))))

(defn make-kv-pair
  [k v t]
  [(keyword k)
   (if (str/starts-with? t "_")
     (parse-array v)
     v)])

(def logical-decoding->value-streams
  (comp
    ;; The stream comes as items corresponding to transactions. We want items based on row.
    (mapcat (fn [{:keys [change] :as item}]
              (let [top-level-data (dissoc item :change)]
                (map #(merge top-level-data %) change))))
    (map (fn [{:keys [columnnames columnvalues columntypes schema table kind oldkeys]}]
           (let [tx-type (keyword kind)
                 model (keyword table)
                 {:keys [keynames keyvalues keytypes]} oldkeys]
             {:model model
              :type  tx-type
              :value (if (= tx-type :delete)
                       (into {} (map make-kv-pair keynames keyvalues keytypes))
                       (into {} (map make-kv-pair columnnames columnvalues columntypes)))})))
    (filter (fn [v]
              (debug (green "streaming") v)
              v))))

(defonce ^:private started? (atom false))

(defn start-data-stream
  [streaming-proc {:keys [streaming-buffer] :as conf}]
  (try
    (reset! started? true)
    (debug "mounting streaming")
    (let [buffer-size streaming-buffer
          stream-source (a/chan buffer-size
                                logical-decoding->value-streams
                                #(error % "handling stream failed"))
          counter (atom 0)]
      (a/thread
        (try
          (debug "start database streaming")
          (with-open [rdr (io/reader (:out streaming-proc))]
            (doseq [item (json/parsed-seq rdr true)]
              (swap! counter inc)
              (trace "json data stream" @counter item)
              (when (= 0 (rem @counter 500))
                (debug "json data stream" @counter))
              (>!! stream-source item))
            (if @started?
              (do
                (fatal "data-stream terminated unexpectedly.")
                (System/exit 1))
              (debug "wal2json stream terminated")))
          (catch Throwable e
            (error e))))
      stream-source)
    (catch Throwable e
      (error e "mounting streaming failed")
      (throw e))))

(defn stop-data-stream
  [data-stream]
  (try
    (reset! started? false)
    (debug "unmounting streaming")
    (a/close! data-stream)
    (catch Throwable e
      (error e))))