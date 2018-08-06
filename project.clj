(defproject infihis/dais "0.3.1"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :scm {:name "git"
        :url  "https://github.com/zh217/dais"}
  :pedantic? :abort
  :dependencies [[com.walmartlabs/lacinia "0.23.0"]]
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.8.0"]
                                  [honeysql "0.9.1"]
                                  [nilenso/honeysql-postgres "0.2.3"]
                                  [org.postgresql/postgresql "42.1.4"]
                                  [com.taoensso/timbre "4.10.0"]
                                  [clj-time "0.14.2"]
                                  [cheshire "5.8.0"]
                                  [cc.qbits/spandex "0.5.5" :exclusions [org.clojure/clojure]]
                                  [org.clojure/java.jdbc "0.7.3"]
                                  [org.clojure/tools.reader "1.1.1"]
                                  [me.raynes/conch "0.8.0"]
                                  [aysylu/loom "1.0.0" :exclusions [tailrecursion/cljs-priority-map]]
                                  [instaparse "1.4.8"]
                                  [cpath-clj "0.1.2"]
                                  [org.clojure/data.priority-map "0.0.7"]
                                  [org.apache.kafka/kafka-clients "1.0.0"]
                                  [com.taoensso/nippy "2.13.0"]
                                  [org.clojars.zh217/ring-jetty9-adapter "0.10.4-20171014.102324-1"]
                                  [cc.qbits/hayt "4.0.0" :exclusions [org.clojure/clojure]]
                                  [cc.qbits/alia "4.0.3" :exclusions [org.clojure/clojure]]
                                  [cc.qbits/alia-async "4.0.3" :exclusions [org.clojure/clojure]]
                                  [cc.qbits/alia-joda-time "4.0.3" :exclusions [org.clojure/clojure]]]}})
