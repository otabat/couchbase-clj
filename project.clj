(defproject couchbase-clj "0.1.0"
  :description "A Clojure client for Couchbase Server 2.0."
  :url "https://github.com/otabat/couchbase-clj"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [org.clojure/data.json "0.1.3"]
                 [couchbase/couchbase-client "1.1-dp3"]
                 [spy/spymemcached "2.8.7"]]
  :plugins [[lein-swank "1.4.4"]
            [codox "0.6.1"]]
  :repositories {"couchbase" {:url "http://files.couchbase.com/maven2"}}
  :codox {:exclude [my.private.ns another.private.ns]})
