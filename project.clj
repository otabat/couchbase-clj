(defproject couchbase-clj "0.2.0-SNAPSHOT"
  :description "A Clojure client for Couchbase Server 2.x."
  :url "https://github.com/otabat/couchbase-clj"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojure/data.json "0.2.4"]
                 [com.couchbase.client/couchbase-client "1.3.2"]]
  :profiles {:1.4 {:dependencies [[org.clojure/clojure "1.4.0"]]}
             :1.5 {:dependencies [[org.clojure/clojure "1.5.1"]]}
             :1.6 {:dependencies [[org.clojure/clojure "1.6.0-beta1"]]}}
  :aliases {"test-all" ["with-profile" "dev,1.4:dev,1.5:dev,1.6" "test"]
            "check-all" ["with-profile" "1.4:1.5:1.6" "check"]}
  :plugins [[codox "0.6.6"]])
