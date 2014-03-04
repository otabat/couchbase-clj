(ns couchbase-clj.test.query
  (:import [com.couchbase.client.protocol.views Query])
  (:use [clojure.test])
  (:require [clojure.string :as clojure-string]
            [couchbase-clj.query :as cb-query]))

(deftest get-query-test
  (testing "Get the Query object"
    (let [q1 (cb-query/create-query {})]
      (is (instance? Query (cb-query/get-query q1))))))

(deftest reduce?-test
  (testing "If a query will include documents."
    (let [q1 (cb-query/create-query {:reduce true})
          q2 (cb-query/create-query {:reduce false})]
      (is (true? (cb-query/reduce? q1)))
      (is (false? (cb-query/reduce? q2))))))

(deftest include-docs?-test
  (testing "If a query will include documents."
    (let [q1 (cb-query/create-query {:include-docs true})
          q2 (cb-query/create-query {:include-docs false})]
      (is (true? (cb-query/include-docs? q1)))
      (is (false? (cb-query/include-docs? q2))))))

(deftest set-include-docs!-test
  (testing "Set include-docs option to a query."
    (let [q1 (cb-query/create-query {})
          q2 (cb-query/create-query {})
          q3 (cb-query/create-query {:include-docs true})]
      (cb-query/set-include-docs! q1 true)
      (cb-query/set-include-docs! q2 false)
      (is (true? (cb-query/include-docs? q1)))
      (is (false? (cb-query/include-docs? q2)))
      (is (true? (cb-query/include-docs? q3))))))

(deftest set-desc!-test
  (testing "Set desc option to a query."
    (let [q1 (cb-query/create-query {})
          q2 (cb-query/create-query {})
          q3 (cb-query/create-query {:desc true})]
      (cb-query/set-desc! q1 true)
      (cb-query/set-desc! q2 false)
      (is (= (cb-query/str q1) "?descending=true"))
      (is (= (cb-query/str q2) "?descending=false"))
      (is (= (cb-query/str q3) "?descending=true")))))

(deftest set-startkey-doc-id!-test
  (testing "Set startkey-doc-id option to a query."
    (let [q1 (cb-query/create-query {})
          q2 (cb-query/create-query {})
          q3 (cb-query/create-query {:startkey-doc-id :doc3})]
      (cb-query/set-startkey-doc-id! q1 :doc1)
      (cb-query/set-startkey-doc-id! q2 "doc2")
      (is (= (cb-query/str q1) "?startkey_docid=doc1"))
      (is (= (cb-query/str q2) "?startkey_docid=doc2"))
      (is (= (cb-query/str q3) "?startkey_docid=doc3")))))

(deftest set-endkey-doc-id!-test
  (testing "Set endkey-doc-id option to a query."
    (let [q1 (cb-query/create-query {})
          q2 (cb-query/create-query {})
          q3 (cb-query/create-query {:endkey-doc-id :doc3})]
      (cb-query/set-endkey-doc-id! q1 :doc1)
      (cb-query/set-endkey-doc-id! q2 "doc2")
      (is (= (cb-query/str q1) "?endkey_docid=%22doc1%22"))
      (is (= (cb-query/str q2) "?endkey_docid=%22doc2%22"))
      (is (= (cb-query/str q3) "?endkey_docid=%22doc3%22")))))

(deftest set-group!-test
  (testing "Set group option to a query."
    (let [q1 (cb-query/create-query {})
          q2 (cb-query/create-query {})
          q3 (cb-query/create-query {:group true})]
      (cb-query/set-group! q1 true)
      (cb-query/set-group! q2 false)
      (is (= (cb-query/str q1) "?group=true"))
      (is (= (cb-query/str q2) "?group=false"))
      (is (= (cb-query/str q3) "?group=true")))))

(deftest set-group-level!-test
  (testing "Set group-level option to a query."
    (let [q1 (cb-query/create-query {})
          q2 (cb-query/create-query {:group-level 2})]
      (cb-query/set-group-level! q1 1)
      (is (= (cb-query/str q1) "?group_level=1"))
      (is (= (cb-query/str q2) "?group_level=2")))))

(deftest set-inclusive-end!-test
  (testing "Set inclusive-end option to a query"
    (let [q1 (cb-query/create-query {})
          q2 (cb-query/create-query {})
          q3 (cb-query/create-query {:inclusive-end true})]
      (cb-query/set-inclusive-end! q1 true)
      (cb-query/set-inclusive-end! q2 false)
      (is (= (cb-query/str q1) "?inclusive_end=true"))
      (is (= (cb-query/str q2) "?inclusive_end=false"))
      (is (= (cb-query/str q3) "?inclusive_end=true")))))

(deftest set-key!-test
  (testing "Set key option to a query."
    (let [q1 (cb-query/create-query {})
          q2 (cb-query/create-query {})
          q3 (cb-query/create-query {})
          q4 (cb-query/create-query {})
          q5 (cb-query/create-query {:key :key5})]
      (cb-query/set-key! q1 :key1)
      (cb-query/set-key! q2 "key2")
      (cb-query/set-key! q3 1)
      (cb-query/set-key! q4 [1 2])
      (is (= (cb-query/str q1) "?key=%22key1%22"))
      (is (= (cb-query/str q2) "?key=%22key2%22"))
      (is (= (cb-query/str q3) "?key=1"))
      (is (= (cb-query/str q4) "?key=%5B1%2C2%5D"))
      (is (= (cb-query/str q5) "?key=%22key5%22")))))

(deftest set-limit!-test
  (testing "Set limit option to a query."
    (let [q1 (cb-query/create-query {})
          q2 (cb-query/create-query {:limit 5})]
      (cb-query/set-limit! q1 100)
      (is (= (cb-query/str q1) "?limit=100"))
      (is (= (cb-query/str q2) "?limit=5")))))

(deftest set-range!-test
  (testing "Set range option to a query."
    (let [q1 (cb-query/create-query {})
          q2 (cb-query/create-query {})
          q3 (cb-query/create-query {})
          q4 (cb-query/create-query {})
          q5 (cb-query/create-query {:range [:doc5 :doc6]})]
      (cb-query/set-range! q1 [:doc1 :doc2])
      (cb-query/set-range! q2 ["doc3" "doc4"])
      (cb-query/set-range! q3 [1 2])
      (cb-query/set-range! q4 [[1 2] [3 4]])
      (is (= (cb-query/str q1) "?startkey=%22doc1%22&endkey=%22doc2%22"))
      (is (= (cb-query/str q2) "?startkey=%22doc3%22&endkey=%22doc4%22"))
      (is (= (cb-query/str q3) "?startkey=1&endkey=2"))
      (is (= (cb-query/str q4) "?startkey=%5B1%2C2%5D&endkey=%5B3%2C4%5D"))
      (is (= (cb-query/str q5) "?startkey=%22doc5%22&endkey=%22doc6%22")))))

(deftest set-range-start!-test
  (testing "Set range-start option to a query."
    (let [q1 (cb-query/create-query {})
          q2 (cb-query/create-query {})
          q3 (cb-query/create-query {})
          q4 (cb-query/create-query {})
          q5 (cb-query/create-query {:range-start :doc5})]
      (cb-query/set-range-start! q1 :doc1)
      (cb-query/set-range-start! q2 "doc2")
      (cb-query/set-range-start! q3 1)
      (cb-query/set-range-start! q4 [1 2])
      (is (= (cb-query/str q1) "?startkey=%22doc1%22"))
      (is (= (cb-query/str q2) "?startkey=%22doc2%22"))
      (is (= (cb-query/str q3) "?startkey=1"))
      (is (= (cb-query/str q4) "?startkey=%5B1%2C2%5D"))
      (is (= (cb-query/str q5) "?startkey=%22doc5%22")))))

(deftest set-range-end!-test
  (testing "Set range-end to a query."
    (let [q1 (cb-query/create-query {})
          q2 (cb-query/create-query {})
          q3 (cb-query/create-query {})
          q4 (cb-query/create-query {})
          q5 (cb-query/create-query {:range-end :doc5})]
      (cb-query/set-range-end! q1 :doc1)
      (cb-query/set-range-end! q2 "doc2")
      (cb-query/set-range-end! q3 2)
      (cb-query/set-range-end! q4 [3 4])
      (is (= (cb-query/str q1) "?endkey=%22doc1%22"))
      (is (= (cb-query/str q2) "?endkey=%22doc2%22"))
      (is (= (cb-query/str q3) "?endkey=2"))
      (is (= (cb-query/str q4) "?endkey=%5B3%2C4%5D"))
      (is (= (cb-query/str q5) "?endkey=%22doc5%22")))))

(deftest set-reduce!-test
  (testing "Set reduce option to a query."
    (let [q1 (cb-query/create-query {})
          q2 (cb-query/create-query {})
          q3 (cb-query/create-query {:reduce true})]
      (cb-query/set-reduce! q1 true)
      (cb-query/set-reduce! q2 false)
      (is (= (cb-query/str q1) "?reduce=true"))
      (is (= (cb-query/str q2) "?reduce=false"))
      (is (= (cb-query/str q3) "?reduce=true")))))

(deftest set-skip!-test
  (testing "Set skip option to a query."
    (let [q1 (cb-query/create-query {})
          q2 (cb-query/create-query {})
          q3 (cb-query/create-query {:skip 5})]
      (cb-query/set-skip! q1 1)
      (cb-query/set-skip! q2 100)
      (is (= (cb-query/str q1) "?skip=1"))
      (is (= (cb-query/str q2) "?skip=100"))
      (is (= (cb-query/str q3) "?skip=5")))))

(deftest set-stale!-test
  (testing "Set stale option to a query."
    (let [q1 (cb-query/create-query {})
          q2 (cb-query/create-query {})
          q3 (cb-query/create-query {})
          q4 (cb-query/create-query {})
          q5 (cb-query/create-query {})
          q6 (cb-query/create-query {})
          q7 (cb-query/create-query {})
          q8 (cb-query/create-query {:stale :ok})]
      (cb-query/set-stale! q1 :ok)
      (cb-query/set-stale! q2 :false)
      (cb-query/set-stale! q3 :update-after)
      (cb-query/set-stale! q4 :true)
      (cb-query/set-stale! q5 true)
      (cb-query/set-stale! q6 false)
      (cb-query/set-stale! q7 :else)
      (is (= (cb-query/str q1) "?stale=ok"))
      (is (= (cb-query/str q2) "?stale=false"))
      (is (= (cb-query/str q3) "?stale=update_after"))
      (is (= (cb-query/str q4) "?stale=ok"))
      (is (= (cb-query/str q5) "?stale=ok"))
      (is (= (cb-query/str q6) "?stale=false"))
      (is (= (cb-query/str q7) "?stale=update_after"))
      (is (= (cb-query/str q8) "?stale=ok")))))

(deftest set-on-error!-test
  (testing "Set on-error option to a query."
    (let [q1 (cb-query/create-query {})
          q2 (cb-query/create-query {})
          q3 (cb-query/create-query {})
          q4 (cb-query/create-query {:on-error :stop})]
      (cb-query/set-on-error! q1 :stop)
      (cb-query/set-on-error! q2 :continue)
      (cb-query/set-on-error! q3 :else)
      (is (= (cb-query/str q1) "?on_error=stop"))
      (is (= (cb-query/str q2) "?on_error=continue"))
      (is (= (cb-query/str q3) "?on_error=continue"))
      (is (= (cb-query/str q4) "?on_error=stop")))))

(deftest assoc!-test
  (testing "Getting an updated query."
    (let [q1 (cb-query/create-query {:limit 1})]
      (cb-query/assoc! q1 {:limit 100
                           :group-level 1})
      (is (= (cb-query/str q1) "?limit=100&group_level=1")))))

(deftest str-tes
  (testing "String conversion of a query."
    (let [q1 (cb-query/create-query {:limit 1})]
      (is (= (cb-query/str q1) "?limit=1")))))

(defn- parse-query-string
  [query-string]
  (let [s (clojure-string/replace query-string #"^\?" "")]
    (when-not (empty? s)
      (->> (clojure-string/split s #"&")
           (map #(clojure-string/split % #"="))
           (reduce (fn [acc [k v]] (assoc acc (keyword k) v)) {})))))

(deftest create-query-test
  (testing "Creation of a query."
    (let [q1 (cb-query/create-query {:limit 1})
          q2 (cb-query/create-query {:include-docs true
                                     :desc false
                                     :startkey-doc-id :doc1
                                     :endkey-doc-id :doc2
                                     :group false
                                     :group-level 1
                                     :inclusive-end false
                                     :key :key1
                                     :limit 100
                                     :range [:start-key :end-key]
                                     :reduce false
                                     :skip 1
                                     :stale false
                                     :on-error :continue})]
      (is (= (.toString (cb-query/get-query q1)) "?limit=1"))
      (is (true? (cb-query/include-docs? q2)))
      (is (= (-> (cb-query/get-query q2)
                 .toString
                 parse-query-string)
             {:descending "false"
              :startkey_docid "doc1"
              :endkey_docid "%22doc2%22"
              :group "false"
              :group_level "1"
              :inclusive_end "false"
              :key "%22key1%22"
              :limit "100"
              :startkey "%22start-key%22"
              :endkey "%22end-key%22"
              :reduce "false"
              :skip "1"
              :stale "false"
              :on_error "continue"})))))

(deftest defquery-test
  (testing "Creation of a query and binding to a Var."
    (cb-query/defquery q1 {:limit 1})
    (cb-query/defquery q2 {:include-docs false
                           :desc false
                           :startkey-doc-id :doc1
                           :endkey-doc-id :doc2
                           :group false
                           :group-level 1
                           :inclusive-end false
                           :key :key1
                           :limit 100
                           :range-start :start-key
                           :range-end :end-key
                           :reduce false
                           :skip 1
                           :stale false
                           :on-error :continue})
    (is (= (.toString (cb-query/get-query q1)) "?limit=1"))
    (is (false? (cb-query/include-docs? q2)))
    (is (= (-> (cb-query/get-query q2)
               .toString
               parse-query-string)
           {:descending "false"
            :startkey_docid "doc1"
            :endkey_docid "%22doc2%22"
            :group "false"
            :group_level "1"
            :inclusive_end "false"
            :key "%22key1%22"
            :limit "100"
            :startkey "%22start-key%22"
            :endkey "%22end-key%22"
            :reduce "false"
            :skip "1"
            :stale "false"
            :on_error "continue"}))))
