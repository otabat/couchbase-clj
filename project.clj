(defproject couchbase-clj "0.1.1"
  :description "A Clojure client for Couchbase Server 2.0."
  :url "https://github.com/otabat/couchbase-clj"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojure/data.json "0.2.1"]
                 [couchbase/couchbase-client "1.1.5"]
                 [spy/spymemcached "2.8.12"]]
  :plugins [[codox "0.6.4"]]
  :repositories {"couchbase" {:url "http://files.couchbase.com/maven2"}})
