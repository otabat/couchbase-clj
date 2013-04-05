(ns couchbase-clj.test.query
  (:require [couchbase-clj.query :as cbq]
            [couchbase-clj.test.fixture :as tf])
  (:use [clojure.test]))

;(deftest get-query-test)
(deftest reduce?-test
  (testing "If a query will include documents."
    (let [q1 (cbq/create-query {:reduce true})
          q2 (cbq/create-query {:reduce false})]
      (is (true? (cbq/reduce? q1)))
      (is (false? (cbq/reduce? q2))))))

(deftest include-docs?-test
  (testing "If a query will include documents."
    (let [q1 (cbq/create-query {:include-docs true})
          q2 (cbq/create-query {:include-docs false})]
      (is (true? (cbq/include-docs? q1)))
      (is (false? (cbq/include-docs? q2))))))

(deftest set-include-docs-test
  (testing "Set include-docs option to a query."
    (let [q1 (cbq/create-query {:include-docs true})
          q2 (cbq/create-query {:include-docs false})]
      (is (true? (cbq/include-docs? q1)))
      (is (false? (cbq/include-docs? q2))))))

(deftest set-desc-test
  (testing "Set desc option to a query."
    (let [q1 (cbq/create-query {:desc true})
          q2 (cbq/create-query {:desc false})]
      (is (= (cbq/str q1) "?descending=true"))
      (is (= (cbq/str q2) "?descending=false")))))

(deftest set-startkey-doc-id-test
  (testing "Set startkey-doc-id option to a query."
    (let [q1 (cbq/create-query {:startkey-doc-id :doc1})
          q2 (cbq/create-query {:startkey-doc-id "doc2"})]
      (is (= (cbq/str q1) "?startkey_docid=doc1"))
      (is (= (cbq/str q2) "?startkey_docid=doc2")))))

(deftest set-endkey-doc-id-test
  (testing "Set endkey-doc-id option to a query."
    (let [q1 (cbq/create-query {:endkey-doc-id :doc1})
          q2 (cbq/create-query {:endkey-doc-id "doc2"})]
      (is (= (cbq/str q1) "?endkey_docid=%22doc1%22"))
      (is (= (cbq/str q2) "?endkey_docid=%22doc2%22")))))

(deftest set-group-test
  (testing "Set group option to a query."
    (let [q1 (cbq/create-query {:group true})
          q2 (cbq/create-query {:group false})]
      (is (= (cbq/str q1) "?group=true"))
      (is (= (cbq/str q2) "?group=false")))))

(deftest set-group-level-test
  (testing "Set group-level option to a query."
    (let [q1 (cbq/create-query {:group-level 1})]
      (is (= (cbq/str q1) "?group_level=1")))))

(deftest set-inclusive-end-test
  (testing "Set inclusive-end option to a query"
    (let [q1 (cbq/create-query {:inclusive-end true})
          q2 (cbq/create-query {:inclusive-end false})]
      (is (= (cbq/str q1) "?inclusive_end=true"))
      (is (= (cbq/str q2) "?inclusive_end=false")))))

(deftest set-key-test
  (testing "Set key option to a query."
    (let [q1 (cbq/create-query {:key :key1})
          q2 (cbq/create-query {:key "key2"})]
      (is (= (cbq/str q1) "?key=%22key1%22"))
      (is (= (cbq/str q2) "?key=%22key2%22")))))

(deftest set-limit-test
  (testing "Set limit option to a query."
    (let [q1 (cbq/create-query {:limit 100})]
      (is (= (cbq/str q1) "?limit=100")))))

(deftest set-range-test
  (testing "Set range option to a query."
    (let [q1 (cbq/create-query {:range [:doc1 :doc2]})
          q2 (cbq/create-query {:range ["doc3" "doc4"]})]
      (is (= (cbq/str q1) "?startkey=%22doc1%22&endkey=%22doc2%22"))
      (is (= (cbq/str q2) "?startkey=%22doc3%22&endkey=%22doc4%22")))))

(deftest set-range-start-test
  (testing "Set range-start option to a query."
    (let [q1 (cbq/create-query {:range-start :doc1})
          q2 (cbq/create-query {:range-start "doc2"})]
      (is (= (cbq/str q1) "?startkey=%22doc1%22"))
      (is (= (cbq/str q2) "?startkey=%22doc2%22")))))

(deftest set-range-end-test
  (testing "Set range-end to a query."
    (let [q1 (cbq/create-query {:range-end :doc1})
          q2 (cbq/create-query {:range-end "doc2"})]
      (is (= (cbq/str q1) "?endkey=%22doc1%22"))
      (is (= (cbq/str q2) "?endkey=%22doc2%22")))))

(deftest set-reduce-test
  (testing "Set reduce option to a query."
    (let [q1 (cbq/create-query {:reduce true})
          q2 (cbq/create-query {:reduce false})]
      (is (= (cbq/str q1) "?reduce=true"))
      (is (= (cbq/str q2) "?reduce=false")))))

(deftest set-skip-test
  (testing "Set skip option to a query."
    (let [q1 (cbq/create-query {:skip 1})
          q2 (cbq/create-query {:skip 100})]
      (is (= (cbq/str q1) "?skip=1"))
      (is (= (cbq/str q2) "?skip=100")))))

(deftest set-stale-test
  (testing "Set stale option to a query."
    (let [q1 (cbq/create-query {:stale :ok})
          q2 (cbq/create-query {:stale :false})
          q3 (cbq/create-query {:stale :update-after})
          q4 (cbq/create-query {:stale :true})
          q5 (cbq/create-query {:stale true})
          q6 (cbq/create-query {:stale false})
          q7 (cbq/create-query {:stale :else})]
      (is (= (cbq/str q1) "?stale=ok"))
      (is (= (cbq/str q2) "?stale=false"))
      (is (= (cbq/str q3) "?stale=update_after"))
      (is (= (cbq/str q4) "?stale=ok"))
      (is (= (cbq/str q5) "?stale=ok"))
      (is (= (cbq/str q6) "?stale=false"))
      (is (= (cbq/str q7) "?stale=update_after")))))

(deftest set-on-error-test
  (testing "Set on-error option to a query."
    (let [q1 (cbq/create-query {:on-error :stop})
          q2 (cbq/create-query {:on-error :continue})
          q3 (cbq/create-query {:on-error :else})]
      (is (= (cbq/str q1) "?on_error=stop"))
      (is (= (cbq/str q2) "?on_error=continue"))
      (is (= (cbq/str q3) "?on_error=continue")))))

(deftest assoc-test
  (testing "Gettina an updated copy of the query."
    (let [q1 (cbq/create-query {:limit 1})
          q2 (cbq/assoc q1 {:limit 100
                            :skip 10})]
      (is (= (cbq/str q2) "?limit=100&skip=10")))))

(deftest assoc!-test
  (testing "Getting an updated query."
    (let [q1 (cbq/create-query {:limit 1})]
      (cbq/assoc! q1 {:limit 100
                      :group-level 1})
      (is (= (cbq/str q1) "?limit=100&group_level=1")))))

(deftest str-tes
  (testing "String conversion of a query."
    (let [q1 (cbq/create-query {:limit 1})]
      (is (= (cbq/str q1) "?limit=1")))))

(deftest create-query-test
  (testing "Creation of a query."
    (let [q1 (cbq/create-query {:limit 1})
          q2 (cbq/create-query {:include-docs true
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
      (is (= (.toString (cbq/get-query q1)) "?limit=1"))
      (is (= (.toString (cbq/get-query q2))
             (str "?limit=100&endkey_docid=%22doc2%22&group_level=1"
                  "&startkey=%22start-key2%22&skip=1&descending=false"
                  "&reduce=false&inclusive_end=false&on_error=continue"
                  "&endkey=%22end-key2%22&group=false&stale=false"
                  "&key=%22key1%22&startkey_docid=doc1"))))))

(deftest defquery
  (testing "Creation of a query and binding to a Var."
    (cbq/defquery q1 {:limit 1})
    (cbq/defquery q2 {:include-docs true
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
    (is (= (.toString (cbq/get-query q1)) "?limit=1"))
    (is (= (.toString (cbq/get-query q2))
           (str "?limit=100&endkey_docid=%22doc2%22&group_level=1"
                "&startkey=%22start-key2%22&skip=1&descending=false"
                "&reduce=false&inclusive_end=false&on_error=continue"
                "&endkey=%22end-key2%22&group=false&"
                "stale=false&key=%22key1%22&startkey_docid=doc1")))))
