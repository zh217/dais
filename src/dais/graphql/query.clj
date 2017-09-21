(ns
  ^{:author "Ziyang Hu"}
  dais.graphql.query
  (:require [clojure.string :as str]
            [clojure.java.io :as io]))

(defn extract-fragment-names
  [q]
  (->> q
       (re-seq #"\.\.\.\s*(\w+)\s*")
       (map (comp keyword second))))

(defn load-query
  [& file-names]
  (let [file-lines (mapcat #(->> %
                                 (io/resource)
                                 (slurp)
                                 (str/split-lines))
                           file-names)
        raw-results (loop [remaining file-lines
                           acc {}
                           last nil]
                      (let [[curr & nxt-lines] remaining]
                        (cond
                          (nil? curr) acc

                          (str/blank? curr) (recur nxt-lines acc last)

                          (re-matches #"^\s*(query|mutation|subscription|fragment)\s+.*$" curr)
                          (let [[_ _ q-name] (re-matches #"^\s*(query|mutation|subscription|fragment)\s+(\w+).*$" curr)
                                q-kw (keyword q-name)]
                            (recur nxt-lines
                                   (assoc acc q-kw curr)
                                   q-kw))

                          (nil? last) (recur nxt-lines acc last)

                          :else (recur nxt-lines
                                       (update acc last str "\n" curr)
                                       last))))
        dependencies (into {} (for [[k t] raw-results]
                                [k (extract-fragment-names t)]))
        all-deps (into {} (for [[k ds] dependencies]
                            [k (loop [a-ds (set ds)]
                                 (let [new-deps (mapcat dependencies a-ds)
                                       all-deps (set (concat a-ds new-deps))]
                                   (if (= all-deps a-ds)
                                     all-deps
                                     (recur all-deps))))]))]
    (into {}
          (for [[k t] raw-results]
            [k (str/join "\n\n" (list* t (map raw-results (all-deps k))))]))))