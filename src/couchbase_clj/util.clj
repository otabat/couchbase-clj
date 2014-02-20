(ns couchbase-clj.util
  (:require [clojure.data.json :as json]))

(defn read-json
  "Reads a JSON value from input String.
  If data is nil, then nil is returned.
  If data is a empty string, then empty string is returned."
  [data]
  (when-not (nil? data) (json/read-json data true false "")))

(def ^{:doc "Wrapper of clojure.data.json/json-str. Just for convenience."}
  write-json json/json-str)
