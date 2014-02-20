(ns couchbase-clj.test.util
  (:use [clojure.test])
  (:require [clojure.data.json :as json]
            [couchbase-clj.util :as cb-util]))

(deftest read-json-test
  (testing "convert json data to clojure data"
    (is (= {:a 1} (cb-util/read-json (json/json-str {:a 1}))))
    (is (= [1 2 3] (cb-util/read-json (json/json-str [1 2 3]))))
    (is (= nil (cb-util/read-json nil)))))

(deftest write-json-test
  (testing "convert clojure data to json data"
    (is (= cb-util/write-json json/json-str))))

