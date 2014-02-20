(ns couchbase-clj.test.query
  (:use [clojure.test])
  (:require [couchbase-clj.query :as cb-query]))

;(deftest get-query-test)
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

(deftest set-include-docs-test
  (testing "Set include-docs option to a query."
    (let [q1 (cb-query/create-query {:include-docs true})
          q2 (cb-query/create-query {:include-docs false})]
      (is (true? (cb-query/include-docs? q1)))
      (is (false? (cb-query/include-docs? q2))))))

(deftest set-desc-test
  (testing "Set desc option to a query."
    (let [q1 (cb-query/create-query {:desc true})
          q2 (cb-query/create-query {:desc false})]
      (is (= (cb-query/str q1) "?descending=true"))
      (is (= (cb-query/str q2) "?descending=false")))))

(deftest set-startkey-doc-id-test
  (testing "Set startkey-doc-id option to a query."
    (let [q1 (cb-query/create-query {:startkey-doc-id :doc1})
          q2 (cb-query/create-query {:startkey-doc-id "doc2"})]
      (is (= (cb-query/str q1) "?startkey_docid=doc1"))
      (is (= (cb-query/str q2) "?startkey_docid=doc2")))))

(deftest set-endkey-doc-id-test
  (testing "Set endkey-doc-id option to a query."
    (let [q1 (cb-query/create-query {:endkey-doc-id :doc1})
          q2 (cb-query/create-query {:endkey-doc-id "doc2"})]
      (is (= (cb-query/str q1) "?endkey_docid=%22doc1%22"))
      (is (= (cb-query/str q2) "?endkey_docid=%22doc2%22")))))

(deftest set-group-test
  (testing "Set group option to a query."
    (let [q1 (cb-query/create-query {:group true})
          q2 (cb-query/create-query {:group false})]
      (is (= (cb-query/str q1) "?group=true"))
      (is (= (cb-query/str q2) "?group=false")))))

(deftest set-group-level-test
  (testing "Set group-level option to a query."
    (let [q1 (cb-query/create-query {:group-level 1})]
      (is (= (cb-query/str q1) "?group_level=1")))))

(deftest set-inclusive-end-test
  (testing "Set inclusive-end option to a query"
    (let [q1 (cb-query/create-query {:inclusive-end true})
          q2 (cb-query/create-query {:inclusive-end false})]
      (is (= (cb-query/str q1) "?inclusive_end=true"))
      (is (= (cb-query/str q2) "?inclusive_end=false")))))

(deftest set-key-test
  (testing "Set key option to a query."
    (let [q1 (cb-query/create-query {:key :key1})
          q2 (cb-query/create-query {:key "key2"})
          q3 (cb-query/create-query {:key 1})
          q4 (cb-query/create-query {:key [1 2]})]
      (is (= (cb-query/str q1) "?key=%22key1%22"))
      (is (= (cb-query/str q2) "?key=%22key2%22"))
      (is (= (cb-query/str q3) "?key=1"))
      (is (= (cb-query/str q4) "?key=%5B1%2C2%5D")))))

(deftest set-limit-test
  (testing "Set limit option to a query."
    (let [q1 (cb-query/create-query {:limit 100})]
      (is (= (cb-query/str q1) "?limit=100")))))

(deftest set-range-test
  (testing "Set range option to a query."
    (let [q1 (cb-query/create-query {:range [:doc1 :doc2]})
          q2 (cb-query/create-query {:range ["doc3" "doc4"]})
          q3 (cb-query/create-query {:range [1 2]})
          q4 (cb-query/create-query {:range [[1 2] [3 4]]})]
      (is (= (cb-query/str q1) "?startkey=%22doc1%22&endkey=%22doc2%22"))
      (is (= (cb-query/str q2) "?startkey=%22doc3%22&endkey=%22doc4%22"))
      (is (= (cb-query/str q3) "?startkey=1&endkey=2"))
      (is (= (cb-query/str q4) "?startkey=%5B1%2C2%5D&endkey=%5B3%2C4%5D")))))

(deftest set-range-start-test
  (testing "Set range-start option to a query."
    (let [q1 (cb-query/create-query {:range-start :doc1})
          q2 (cb-query/create-query {:range-start "doc2"})
          q3 (cb-query/create-query {:range-start 1})
          q4 (cb-query/create-query {:range-start [1 2]})]
      (is (= (cb-query/str q1) "?startkey=%22doc1%22"))
      (is (= (cb-query/str q2) "?startkey=%22doc2%22"))
      (is (= (cb-query/str q3) "?startkey=1"))
      (is (= (cb-query/str q4) "?startkey=%5B1%2C2%5D")))))

(deftest set-range-end-test
  (testing "Set range-end to a query."
    (let [q1 (cb-query/create-query {:range-end :doc1})
          q2 (cb-query/create-query {:range-end "doc2"})
          q3 (cb-query/create-query {:range-end 2})
          q4 (cb-query/create-query {:range-end [3 4]})]
      (is (= (cb-query/str q1) "?endkey=%22doc1%22"))
      (is (= (cb-query/str q2) "?endkey=%22doc2%22"))
      (is (= (cb-query/str q3) "?endkey=2"))
      (is (= (cb-query/str q4) "?endkey=%5B3%2C4%5D")))))

(deftest set-reduce-test
  (testing "Set reduce option to a query."
    (let [q1 (cb-query/create-query {:reduce true})
          q2 (cb-query/create-query {:reduce false})]
      (is (= (cb-query/str q1) "?reduce=true"))
      (is (= (cb-query/str q2) "?reduce=false")))))

(deftest set-skip-test
  (testing "Set skip option to a query."
    (let [q1 (cb-query/create-query {:skip 1})
          q2 (cb-query/create-query {:skip 100})]
      (is (= (cb-query/str q1) "?skip=1"))
      (is (= (cb-query/str q2) "?skip=100")))))

(deftest set-stale-test
  (testing "Set stale option to a query."
    (let [q1 (cb-query/create-query {:stale :ok})
          q2 (cb-query/create-query {:stale :false})
          q3 (cb-query/create-query {:stale :update-after})
          q4 (cb-query/create-query {:stale :true})
          q5 (cb-query/create-query {:stale true})
          q6 (cb-query/create-query {:stale false})
          q7 (cb-query/create-query {:stale :else})]
      (is (= (cb-query/str q1) "?stale=ok"))
      (is (= (cb-query/str q2) "?stale=false"))
      (is (= (cb-query/str q3) "?stale=update_after"))
      (is (= (cb-query/str q4) "?stale=ok"))
      (is (= (cb-query/str q5) "?stale=ok"))
      (is (= (cb-query/str q6) "?stale=false"))
      (is (= (cb-query/str q7) "?stale=update_after")))))

(deftest set-on-error-test
  (testing "Set on-error option to a query."
    (let [q1 (cb-query/create-query {:on-error :stop})
          q2 (cb-query/create-query {:on-error :continue})
          q3 (cb-query/create-query {:on-error :else})]
      (is (= (cb-query/str q1) "?on_error=stop"))
      (is (= (cb-query/str q2) "?on_error=continue"))
      (is (= (cb-query/str q3) "?on_error=continue")))))

(deftest assoc-test
  (testing "Gettina an updated copy of the query."
    (let [q1 (cb-query/create-query {:limit 1})
          q2 (cb-query/assoc q1 {:limit 100
                                 :skip 10})]
      (is (= (cb-query/str q2) "?limit=100&skip=10")))))

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
                                     :range-start :start-key2
                                     :range-end :end-key2
                                     :reduce false
                                     :skip 1
                                     :stale false
                                     :on-error :continue})]
      (is (= (.toString (cb-query/get-query q1)) "?limit=1"))
      (is (= (.toString (cb-query/get-query q2))
             (str "?limit=100&endkey_docid=%22doc2%22&group_level=1"
                  "&startkey=%22start-key2%22&skip=1&descending=false"
                  "&reduce=false&inclusive_end=false&on_error=continue"
                  "&endkey=%22end-key2%22&group=false&stale=false"
                  "&key=%22key1%22&startkey_docid=doc1"))))))

(deftest defquery
  (testing "Creation of a query and binding to a Var."
    (cb-query/defquery q1 {:limit 1})
    (cb-query/defquery q2 {:include-docs true
                           :desc false
                           :startkey-doc-id :doc1
                           :endkey-doc-id :doc2
                           :group false
                           :group-level 1
                           :inclusive-end false
                           :key :key1
                           :limit 100
                           :range [:start-key :end-key]
                           :range-start :start-key2
                           :range-end :end-key2
                           :reduce false
                           :skip 1
                           :stale false
                           :on-error :continue})
    (is (= (.toString (cb-query/get-query q1)) "?limit=1"))
    (is (= (.toString (cb-query/get-query q2))
           (str "?limit=100&endkey_docid=%22doc2%22&group_level=1"
                "&startkey=%22start-key2%22&skip=1&descending=false"
                "&reduce=false&inclusive_end=false&on_error=continue"
                "&endkey=%22end-key2%22&group=false&"
                "stale=false&key=%22key1%22&startkey_docid=doc1")))))
