(ns
  ^{:author "Ziyang Hu"}
  dais.postgres.migration
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.java.jdbc :as jdbc]
            [taoensso.timbre :refer [info debug]]
            [loom.graph :as g]
            [loom.alg :as ga]
            [cpath-clj.core :as cp]))

(defn- tarjan
  "Christophe Grand, from https://gist.github.com/cgrand/5188919

   Returns the strongly connected components of a graph specified by its nodes
   and a successor function succs from node to nodes.
   The used algorithm is Tarjan's one."
  [nodes succs]
  (letfn [(sc [env node]
            ; env is a map from nodes to stack length or nil, nil means the node is known to belong to another SCC
            ; there are two special keys: ::stack for the current stack and ::sccs for the current set of SCCs
            #_{:post [(contains? % node)]}
            (if (contains? env node)
              env
              (let [stack (::stack env)
                    n (count stack)
                    env (assoc env node n ::stack (conj stack node))
                    env (reduce (fn [env succ]
                                  (let [env (sc env succ)]
                                    (assoc env node (min (or (env succ) n) (env node)))))
                                env (succs node))]
                (if (= n (env node))                        ; no link below us in the stack, call it a SCC
                  (let [nodes (::stack env)
                        scc (set (take (- (count nodes) n) nodes))
                        env (reduce #(assoc %1 %2 nil) env scc)] ; clear all stack lengths for these nodes since this SCC is done
                    (assoc env ::stack stack ::sccs (conj (::sccs env) scc)))
                  env))))]
    (::sccs (reduce sc {::stack () ::sccs #{}} nodes))))

;=> (def g {:c #{:d} :a #{:b :c} :b #{:a :c}})
;=> (tarjan (keys g) g)
;#{#{:c} #{:d} #{:a :b}}

(defn load-db-scripts
  [{:keys [init-script schema-path init-transaction? schema-transaction?]
    :or   {init-transaction?   false
           schema-transaction? true}}]
  (let [schema-map (->> (cp/resources schema-path)
                        (filter #(str/ends-with? (first %) ".sql"))
                        (map (fn [[k v]]
                               [(-> k
                                    (subs 1)
                                    (str/replace #"\.sql$" ""))
                                (let [o (first v)
                                      content (with-open [rdr (io/reader o)]
                                                (first (line-seq rdr)))
                                      depends (when (str/starts-with? content "--! depends:")
                                                (-> content
                                                    (str/replace #"--! depends:" "")
                                                    (str/trim)
                                                    (str/split #"[\s,]+")))]
                                  {:object  o
                                   :depends (set depends)})]))
                        (into {}))
        dep-tree (into {} (for [[k {:keys [depends]}] schema-map]
                            [k (when (seq depends)
                                 (vec (filter #(not= k %) depends)))]))
        schema-order (-> dep-tree
                         (g/digraph)
                         (ga/topsort)
                         (reverse)
                         (vec))]
    (when-not (seq schema-order)
      (throw (ex-info "cyclic dependencies found" {:components (tarjan
                                                                 (keys schema-map)
                                                                 (into {} dep-tree))})))
    {:init-transaction?   init-transaction?
     :schema-transaction? schema-transaction?
     :init-script         (when init-script {:object (io/resource init-script)})
     :schemata            schema-map
     :load-order          schema-order}))

(defn execute-db-scripts
  [conn {:keys [init-script schemata load-order init-transaction? schema-transaction?]}]
  (jdbc/with-db-connection [c conn]
    (jdbc/with-db-transaction [tx c]
      (when (seq init-script)
        (let [content (slurp (:object init-script))]
          (info "executing init script")
          (debug content)
          (jdbc/execute! (if init-transaction? tx c)
                         [content])))
      (doseq [schema load-order]
        (let [content (slurp (:object (get schemata schema)))]
          (info "executing schema script" schema)
          (jdbc/execute! (if init-transaction? tx c)
                         [content]))))))