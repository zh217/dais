(defproject infihis/dais "0.1.7-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :scm {:name "git"
        :url  "https://github.com/zh217/dais"}
  :pedantic? :abort
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [honeysql "0.9.1"]
                 [nilenso/honeysql-postgres "0.2.3"]
                 [org.postgresql/postgresql "42.1.4"]
                 [com.taoensso/timbre "4.10.0"]
                 [clj-time "0.14.0"]
                 [cheshire "5.8.0"]
                 [cc.qbits/spandex "0.5.2" :exclusions [org.clojure/clojure]]
                 [org.clojure/java.jdbc "0.7.1"]
                 [org.clojure/tools.reader "1.1.0"]
                 [me.raynes/conch "0.8.0"]
                 [aysylu/loom "1.0.0" :exclusions [tailrecursion/cljs-priority-map]]
                 [instaparse "1.4.8"]
                 [cpath-clj "0.1.2"]
                 [com.walmartlabs/lacinia "0.21.0"]
                 [org.clojure/data.priority-map "0.0.7"]])
