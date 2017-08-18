(ns
  ^{:author "Ziyang Hu"}
  dais.graphql.query
  (:require [clojure.string :as str]
            [clojure.java.io :as io]))

(defn load-query
  [file-name]
  (let [file-lines (->> file-name
                        (io/resource)
                        (slurp)
                        (str/split-lines))]
    (loop [remaining file-lines
           acc {}
           last nil]
      (let [[curr & nxt-lines] remaining]
        (cond
          (nil? curr) acc

          (str/blank? curr) (recur nxt-lines acc last)

          (re-matches #"^\s*(query|mutation)\s+.*$" curr)
          (let [[_ _ q-name] (re-matches #"^\s*(query|mutation)\s+(\w+).*$" curr)
                q-kw (keyword q-name)]
            (recur nxt-lines
                   (assoc acc q-kw curr)
                   q-kw))

          (nil? last) (recur nxt-lines acc last)

          :else (recur nxt-lines
                       (update acc last str "\n" curr)
                       last))))))