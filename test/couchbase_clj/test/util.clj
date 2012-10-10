(ns couchbase-clj.test.util
  (:use [clojure.test])
  (:require [clojure.data.json :as json]
            [couchbase-clj.util :as util]
            [couchbase-clj.client :as client]
            [couchbase-clj.test.fixture :as test-fixture]))

(deftest read-json-test
  (testing "convert json data to clojure data"
    (is (= {:a 1} (util/read-json (json/json-str {:a 1}))))
    (is (= [1 2 3] (util/read-json (json/json-str [1 2 3]))))
    (is (= nil (util/read-json nil)))))

(deftest json-str-test
  (testing "convert clojure data to json data"
    (is (= util/json-str json/json-str))))

