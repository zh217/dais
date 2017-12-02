(ns dais.kafka.client
  (:require [taoensso.nippy :as nippy]
            [clojure.core.async :as a]
            [clojure.string :as str])
  (:import (java.util Map Collection)
           (org.apache.kafka.common.serialization Serializer Deserializer)
           (org.apache.kafka.clients.producer KafkaProducer ProducerRecord Callback RecordMetadata)
           (org.apache.kafka.clients.consumer KafkaConsumer ConsumerRecord)
           (org.apache.kafka.common.header Header)
           (org.apache.kafka.common.errors WakeupException)))

(set! *warn-on-reflection* true)

(defn- opts->props
  ^Map [opts]
  (into {}
        (for [[k v] opts]
          [(str/replace (name k) "-" ".")
           (if (vector? v)
             (str/join "," (map str v))
             (str v))])))

(defn- nippy-serializer
  ^Serializer []
  (reify Serializer
    (close [this])
    (configure [this configs key?])
    (serialize [this topic data]
      (nippy/freeze data))))

(defn- nippy-deserializer
  ^Deserializer []
  (reify Deserializer
    (close [this])
    (configure [this configs key?])
    (deserialize [this topic payload]
      (nippy/thaw payload))))

(defn producer
  [opts]
  (let [serializer (nippy-serializer)]
    (KafkaProducer. (opts->props opts) serializer serializer)))

(defn close-producer
  [^KafkaProducer p]
  (.close p))

(defn send-raw!
  ([^KafkaProducer p record]
   (send-raw! p record nil))
  ([^KafkaProducer p {:keys [topic partition timestamp key value headers]} callback]
   (let [headers (when headers
                   (for [[k v] headers]
                     (reify Header
                       (key [this] k)
                       (value [this] v))))
         record (ProducerRecord. topic partition timestamp key value headers)]
     (.send p record callback))))

(defn send!
  [^KafkaProducer p {:keys [topic partition timestamp key value headers]}]
  (let [c (a/promise-chan)
        record (ProducerRecord. topic partition timestamp key value headers)
        callback-obj (reify Callback
                       (onCompletion [this metadata exception]
                         (a/put! c {:result    (when ^RecordMetadata metadata
                                                 {:offset     (.offset metadata)
                                                  :partition  (.partition metadata)
                                                  :key-size   (.serializedKeySize metadata)
                                                  :value-size (.serializedValueSize metadata)
                                                  :timestamp  (.timestamp metadata)
                                                  :topic      (.topic metadata)})
                                    :exception exception})))]
    (.send p record callback-obj)
    c))

(defmacro <!send
  [& args]
  `(a/<! (send! ~@args)))

(defmacro <!!send
  [& args]
  `(a/<!! (send! ~@args)))

(defn consumer
  [opts]
  (let [deserializer (nippy-deserializer)]
    (KafkaConsumer. (opts->props opts) deserializer deserializer)))

(defn async-consumer
  [opts topics output-chan]
  (let [c ^KafkaConsumer (consumer opts)
        put-fn (fn [^ConsumerRecord record]
                 (a/put! output-chan {:topic     (.topic record)
                                      :partition (.partition record)
                                      :offset    (.offset record)
                                      :key       (.key record)
                                      :value     (.value record)}))]
    (.subscribe c ^Collection topics)
    (future
      (try
        (loop []
          (let [records (.poll c Long/MAX_VALUE)]
            (if (every? put-fn records)
              (recur)
              (.close c))))
        (catch WakeupException ex
          (.close c)
          (a/close! output-chan))
        (catch Throwable ex
          (.printStackTrace ex))))
    c))

(defn stop-consumer!
  [^KafkaConsumer c]
  (.wakeup c))


(comment
  (def sample-producer-config
    {:bootstrap-servers ["localhost:9092"]
     :acks              "all"
     :retries           0
     :batch-size        16384
     :linger-ms         1
     :buffer-memory     33554432})

  (def sample-consumer-config
    {:bootstrap-servers       ["localhost:9092"]
     :group-id                (System/currentTimeMillis)
     :enable-auto-commit      true
     :auto-commit-interval-ms 1000}))