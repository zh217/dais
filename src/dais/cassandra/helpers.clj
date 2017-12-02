(ns dais.cassandra.helpers)

(defn invert-map
  ([m]
   (invert-map m byte))
  ([m conv-fn]
   (let [reverted (into {} (for [[k v] m] [v (conv-fn k)]))]
     (when-not (= (count m) (count reverted))
       (throw (ex-info "duplicate keys" {:original m
                                         :reverted reverted})))
     (fn [v]
       (when v
         (or (get reverted (if (string? v) (keyword v) v))
             (throw (ex-info "bad enum value" {:value v}))))))))

(defn make-columns
  [cls pk]
  (merge cls
         {:primary-key pk}))