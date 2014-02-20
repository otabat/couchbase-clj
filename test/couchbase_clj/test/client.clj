(ns couchbase-clj.test.client
  (:import [net.spy.memcached
            PersistTo ReplicateTo CASValue CASResponse
            FailureMode DefaultHashAlgorithm]
           [net.spy.memcached.auth AuthDescriptor PlainCallbackHandler]
           [net.spy.memcached.transcoders
            Transcoder LongTranscoder SerializingTranscoder]
           [net.spy.memcached.internal GetFuture BulkGetFuture OperationFuture]
           [com.couchbase.client CouchbaseClient CouchbaseConnectionFactory]
           [com.couchbase.client.vbucket VBucketNodeLocator]
           [com.couchbase.client.internal HttpFuture]
           [com.couchbase.client.protocol.views View ViewRow])
  (:use [clojure.test :exclude [set-test]])
  (:require [couchbase-clj.client :as cb-client]
            [couchbase-clj.query :as cb-query]
            [couchbase-clj.test.fixture :as tf]))

;(use-fixtures :once tf/setup-client tf/flush-data)
(use-fixtures :once tf/setup-client)

(deftest persist-to-test
  (testing "Conversion of a keyword to PersistTo object."
    (is (= (cb-client/persist-to :master) PersistTo/MASTER))
    (is (= (cb-client/persist-to :one) PersistTo/ONE))
    (is (= (cb-client/persist-to :two) PersistTo/TWO))
    (is (= (cb-client/persist-to :three) PersistTo/THREE))
    (is (= (cb-client/persist-to :four) PersistTo/FOUR))
    (is (= (cb-client/persist-to :else) PersistTo/MASTER))))

(deftest replicate-to-test
  (testing "Conversion of a keyword to a ReplicateTo object."
    (is (= (cb-client/replicate-to :zero) ReplicateTo/ZERO))
    (is (= (cb-client/replicate-to :one) ReplicateTo/ONE))
    (is (= (cb-client/replicate-to :two) ReplicateTo/TWO))
    (is (= (cb-client/replicate-to :three) ReplicateTo/THREE))
    (is (= (cb-client/replicate-to :else) ReplicateTo/ZERO))))

(deftest cas-id-test
  (testing "Get the cas-id from a CASValue object."
    (cb-client/add (tf/get-client) :cas-id 1)
    (is (->> (cb-client/cas-id (cb-client/get-cas (tf/get-client) :cas-id))
             (instance? Long)))))

(deftest cas-val-test
  (testing "Get the value from a CASValue object."
    (cb-client/add (tf/get-client) :cas-val "cas-id")
    (is (= (cb-client/cas-val (cb-client/get-cas (tf/get-client) :cas-val))
           "cas-id"))))

(deftest cas-val-json-test
  (testing "Get the JSON string value converted to a Clojure data
  from a CASValue object."
    (cb-client/add-json (tf/get-client) :cas-val-json {:cas-val-json 1})
    (is (= (-> (cb-client/get-cas (tf/get-client) :cas-val-json)
               cb-client/cas-val-json)
           {:cas-val-json 1}))))

(deftest get-client-test
  (testing "Get the CouchbaseClient object."
    (is (->> (cb-client/get-client (tf/get-client))
             (instance? CouchbaseClient)))))

(deftest get-factory-test
  (testing "Get the CouchbaseConnectionFactory object."
    (is (->> (cb-client/get-factory (tf/get-client))
             (instance? CouchbaseConnectionFactory)))))
(deftest get-available-servers-test
  (testing "Get the addresses of available servers."
    (let [servers (cb-client/get-available-servers (tf/get-client))]
      (is (or (vector? servers) (nil? servers))))))

(deftest get-unavailable-servers-test
  (testing "Get the addresses of unavailable servers."
    (let [servers (cb-client/get-unavailable-servers (tf/get-client))]
      (is (or (vector? servers) (nil? servers))))))

(deftest get-node-locator-test
  (testing "Get the VBucketNodeLocator object."
    (is (->> (cb-client/get-node-locator (tf/get-client))
             (instance? VBucketNodeLocator)))))

(deftest get-versions-test
  (testing "Get versions of all servers."
    (is (map? (cb-client/get-versions (tf/get-client))))))

(deftest get-sasl-mechanisms-test
  (testing "Get the list of sasl mechanisms."
    (is (set? (cb-client/get-sasl-mechanisms (tf/get-client))))))

(deftest get-client-status-test
  (testing "Get all stats of the connections."
    (is (map? (cb-client/get-client-status (tf/get-client))))))

(deftest get-auth-descriptor-test
  (testing "Get the auth descriptor."
    (let [dsc (cb-client/get-auth-descriptor (tf/get-client))]
      (is (or (instance? AuthDescriptor dsc) (nil? dsc))))))

(deftest get-failure-mode-test
  (testing "Get the FailureMode object."
    (is (instance? FailureMode (cb-client/get-failure-mode (tf/get-client))))))

(deftest get-hash-alg-test
  (testing "Get the DefaultHashAlgorithm object."
    (is (->> (cb-client/get-hash-alg (tf/get-client))
             (instance? DefaultHashAlgorithm)))))

(deftest get-max-reconnect-delay-test
  (testing "Get the max reconnect delay."
    (is (= (cb-client/get-max-reconnect-delay (tf/get-client)) 30))))

;; Test failed, not working.
;(deftest get-min-reconnect-interval-test
;  (testing "Get the min reconnect interval."
;    (is (= (cb-client/get-min-reconnect-interval (tf/get-client)) 1100))))

(deftest get-op-queue-max-block-time-test
  (testing "Get the op queue max block time."
    (is (= (cb-client/get-op-queue-max-block-time (tf/get-client)) 10000))))

(deftest get-op-timeout-test
  (testing "Get the op timeout."
    (is (= (cb-client/get-op-timeout (tf/get-client)) 2500))))

(deftest get-read-buffer-size-test
  (testing "Get the read buffer size."
    (is (= (cb-client/get-read-buffer-size (tf/get-client)) 16384))))

(deftest get-timeout-exception-threshold-test
  (testing "Get the timeout exception threshold."
    (is (= (cb-client/get-timeout-exception-threshold (tf/get-client)) 998))))

(deftest get-transcoder-test
  (testing "Get the transcoder."
    (is (instance? Transcoder (cb-client/get-transcoder (tf/get-client))))))

(deftest daemon?-test
  (testing "Get the Boolean value of daemon option."
    (is (false? (cb-client/daemon? (tf/get-client))))))

(deftest should-optimize?-test
  (testing "Get the Boolean value of should optimize option."
    (is (false? (cb-client/should-optimize? (tf/get-client))))))

(deftest use-nagle-algorithm?-test
  (testing "Get the Boolean value of use nagle algorithm option."
    (is (false? (cb-client/use-nagle-algorithm? (tf/get-client))))))

(deftest async-add-test
  (testing "Asynchronously adding a value."
    (cb-client/delete (tf/get-client) :async-add1)
    (cb-client/delete (tf/get-client) :async-add2)
    (cb-client/delete (tf/get-client) :async-add3)
    (cb-client/delete (tf/get-client) :async-add4)
    (let [fut1 (cb-client/async-add (tf/get-client) :async-add1 1)
          fut2 (cb-client/async-add (tf/get-client) "async-add2" 2 {:expiry 30})
          fut3 (cb-client/async-add (tf/get-client)
                                    'async-add3
                                    "1"
                                    {:transcoder (cb-client/get-transcoder
                                                  (tf/get-client))})
          fut4 (cb-client/async-add (tf/get-client)
                                    :async-add4
                                    "a"
                                    {:observe true
                                     :persist :master
                                     :replicate :zero})]
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut1))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut2))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut3))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut4))
      (is (true? @fut1))
      (is (true? @fut2))
      (is (true? @fut3))
      (is (true? @fut4))
      (is (= (cb-client/get (tf/get-client) :async-add1) 1))
      (is (= (cb-client/get (tf/get-client) :async-add2) 2))
      (is (= (cb-client/get (tf/get-client) :async-add3) "1"))
      (is (= (cb-client/get (tf/get-client) :async-add4) "a")))))

(deftest add-test
  (testing "Synchronously adding a value."
    (cb-client/delete (tf/get-client) :add1)
    (cb-client/delete (tf/get-client) :add2)
    (cb-client/delete (tf/get-client) :add3)
    (cb-client/delete (tf/get-client) :add4)
    (cb-client/delete (tf/get-client) :add5)
    (cb-client/add (tf/get-client) :add1 1)
    (cb-client/add (tf/get-client) "add2" 2 {:expiry 30})
    (cb-client/add (tf/get-client)
                   :add3
                   "1"
                   {:transcoder (cb-client/get-transcoder (tf/get-client))})
    (cb-client/async-add (tf/get-client)
                         'add4
                         "a"
                         {:observe true
                          :persist :master
                          :replicate :zero})
    (cb-client/add (tf/get-client) :add5 {:a 1} {:timeout 1000})
    (is (= (cb-client/get (tf/get-client) :add1) 1))
    (is (= (cb-client/get (tf/get-client) :add2) 2))
    (is (= (cb-client/get (tf/get-client) :add3) "1"))
    (is (= (cb-client/get (tf/get-client) :add4) "a"))
    (is (= (cb-client/get (tf/get-client) :add5) {:a 1}))))

(deftest async-add-json-test
  (testing "Asynchronously adding a Clojure data converted
to a JSON string value."
    (cb-client/delete (tf/get-client) :async-add-json1)
    (cb-client/delete (tf/get-client) :async-add-json2)
    (cb-client/delete (tf/get-client) :async-add-json3)
    (cb-client/delete (tf/get-client) :async-add-json4)
    (let [fut1 (cb-client/async-add-json (tf/get-client) :async-add-json1 "1")
          fut2 (cb-client/async-add-json (tf/get-client)
                                         "async-add-json2"
                                         "2"
                                         {:expiry 30})
          fut3 (cb-client/async-add-json
                (tf/get-client)
                'async-add-json3
                {:a 1}
                {:transcoder (cb-client/get-transcoder (tf/get-client))})
          fut4 (cb-client/async-add-json (tf/get-client)
                                         :async-add-json4
                                         "a"
                                         {:observe true
                                          :persist :master
                                          :replicate :zero})]
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut1))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut2))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut3))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut4))
      (is (true? @fut1))
      (is (true? @fut2))
      (is (true? @fut3))
      (is (true? @fut4))
      (is (= (cb-client/get-json (tf/get-client) :async-add-json1) "1"))
      (is (= (cb-client/get-json (tf/get-client) :async-add-json2) "2"))
      (is (= (cb-client/get-json (tf/get-client) :async-add-json3) {:a 1}))
      (is (= (cb-client/get-json (tf/get-client) :async-add-json4) "a")))))

(deftest add-json-test
  (testing "Synchronously adding a Clojure data converted
to a JSON string value."
    (cb-client/delete (tf/get-client) :add-json1)
    (cb-client/delete (tf/get-client) :add-json2)
    (cb-client/delete (tf/get-client) :add-json3)
    (cb-client/delete (tf/get-client) :add-json4)
    (cb-client/delete (tf/get-client) :add-json5)
    (cb-client/add-json (tf/get-client) :add-json1 "1")
    (cb-client/add-json (tf/get-client) "add-json2" "2" {:expiry 30})
    (cb-client/add-json (tf/get-client)
                        'add-json3
                        {:a 1}
                        {:transcoder (cb-client/get-transcoder (tf/get-client))})
    (cb-client/add-json (tf/get-client)
                        :add-json4
                        "a"
                        {:observe true
                         :persist :master
                         :replicate :zero})
    (cb-client/add-json (tf/get-client) :add-json5 [1 2] {:timeout 1000})
    (is (= (cb-client/get-json (tf/get-client) :add-json1) "1"))
    (is (= (cb-client/get-json (tf/get-client) :add-json2) "2"))
    (is (= (cb-client/get-json (tf/get-client) :add-json3) {:a 1}))
    (is (= (cb-client/get-json (tf/get-client) :add-json4) "a"))
    (is (= (cb-client/get-json (tf/get-client) :add-json5) [1 2]))))

(deftest async-append-test
  (testing "Asynchronously append a value."
    (cb-client/delete (tf/get-client) :async-append1)
    (cb-client/delete (tf/get-client) :async-append2)
    (cb-client/add (tf/get-client) :async-append1 "1")
    (cb-client/add (tf/get-client) :async-append2 "3")
    (let [fut1 (cb-client/async-append (tf/get-client) 
                                       :async-append1
                                       ",2"
                                       (cb-client/get-cas-id (tf/get-client)
                                                             :async-append1))
          fut2 (cb-client/async-append (tf/get-client) 
                                       "async-append2"
                                       ",4"
                                       (cb-client/get-cas-id (tf/get-client)
                                                             :async-append2)
                                       {:transcoder (cb-client/get-transcoder
                                                     (tf/get-client))})]
      (is (true? @fut1))
      (is (true? @fut2))
      (is (= (cb-client/get (tf/get-client) :async-append1) "1,2"))
      (is (= (cb-client/get (tf/get-client) :async-append2) "3,4")))))

(deftest append-test
  (testing "Synchronously append a value."
    (cb-client/delete (tf/get-client) :append1)
    (cb-client/delete (tf/get-client) :append2)
    (cb-client/delete (tf/get-client) :append3)
    (cb-client/add (tf/get-client) :append1 "1")
    (cb-client/add (tf/get-client) :append2 "3")
    (cb-client/add (tf/get-client) :append3 "5")
    (cb-client/append (tf/get-client) 
                      :append1
                      ",2"
                      (cb-client/get-cas-id (tf/get-client) :append1))
    (cb-client/append (tf/get-client) 
                      "append2"
                      ",4"
                      (cb-client/get-cas-id (tf/get-client) :append2)
                      {:transcoder (cb-client/get-transcoder (tf/get-client))})
    (cb-client/append (tf/get-client) 
                      'append3
                      ",6"
                      (cb-client/get-cas-id (tf/get-client) :append3)
                      {:timeout 1000})
    (is (= (cb-client/get (tf/get-client) :append1) "1,2"))
    (is (= (cb-client/get (tf/get-client) :append2) "3,4"))
    (is (= (cb-client/get (tf/get-client) :append3) "5,6"))))

(deftest async-prepend-test
  (testing "Asynchronously prepend a value."
    (cb-client/delete (tf/get-client) :async-prepend1)
    (cb-client/delete (tf/get-client) :async-prepend2)
    (cb-client/add (tf/get-client) :async-prepend1 "1")
    (cb-client/add (tf/get-client) :async-prepend2 "3")
    (let [fut1 (cb-client/async-prepend (tf/get-client) 
                                        :async-prepend1
                                        "2,"
                                        (cb-client/get-cas-id (tf/get-client)
                                                              :async-prepend1))
          fut2 (cb-client/async-prepend (tf/get-client) 
                                        "async-prepend2"
                                        "4,"
                                        (cb-client/get-cas-id
                                         (tf/get-client)
                                         :async-prepend2)
                                        {:transcoder (cb-client/get-transcoder
                                                      (tf/get-client))})]
      (is (true? @fut1))
      (is (true? @fut2))
      (is (= (cb-client/get (tf/get-client) :async-prepend1) "2,1"))
      (is (= (cb-client/get (tf/get-client) :async-prepend2) "4,3")))))

(deftest prepend-test
  (testing "Synchronously prepend a value."
    (cb-client/delete (tf/get-client) :prepend1)
    (cb-client/delete (tf/get-client) :prepend2)
    (cb-client/delete (tf/get-client) :prepend3)
    (cb-client/add (tf/get-client) :prepend1 "1")
    (cb-client/add (tf/get-client) :prepend2 "3")
    (cb-client/add (tf/get-client) :prepend3 "5")
    (cb-client/prepend (tf/get-client) 
                       :prepend1
                       "2,"
                       (cb-client/get-cas-id (tf/get-client) :prepend1))
    (cb-client/prepend (tf/get-client) 
                       "prepend2"
                       "4,"
                       (cb-client/get-cas-id (tf/get-client) :prepend2)
                       {:transcoder (cb-client/get-transcoder (tf/get-client))})
    (cb-client/prepend (tf/get-client) 
                       'prepend3
                       "6,"
                       (cb-client/get-cas-id (tf/get-client) :prepend3)
                       {:timeout 1000})
    (is (= (cb-client/get (tf/get-client) :prepend1) "2,1"))
    (is (= (cb-client/get (tf/get-client) :prepend2) "4,3"))
    (is (= (cb-client/get (tf/get-client) :prepend3) "6,5"))))


(deftest async-delete-test
  (testing "Asynchronously delete a value."
    (cb-client/add (tf/get-client) :async-delete1 1)
    (cb-client/add (tf/get-client) :async-delete2 2)
    (let [fut1 (cb-client/async-delete (tf/get-client) :async-delete1)
          fut2 (cb-client/async-delete (tf/get-client) "async-delete2")]
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut1))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut2))
      (is (true? @fut1))
      (is (true? @fut2))
      (is (nil? (cb-client/get (tf/get-client) :async-delete1)))
      (is (nil? (cb-client/get (tf/get-client) :async-delete2))))))

(deftest delete-test
  (testing "Synchronously delete a value."
    (cb-client/add (tf/get-client) :delete1 1)
    (cb-client/add (tf/get-client) :delete2 2)
    (cb-client/delete (tf/get-client) :delete1)
    (cb-client/delete (tf/get-client) "delete2" {:timeout 1000})
    (is (nil? (cb-client/get (tf/get-client) :delete1)))
    (is (nil? (cb-client/get (tf/get-client) :delete2)))))

(deftest async-get-test
  (testing "Asynchronously get a value."
    (cb-client/delete (tf/get-client) :async-get1)
    (cb-client/delete (tf/get-client) :async-get2)
    (cb-client/add (tf/get-client) :async-get1 1)
    (cb-client/add (tf/get-client) :async-get2 2)
    (let [fut1 (cb-client/async-get (tf/get-client) :async-get1)
          fut2 (cb-client/async-get (tf/get-client)
                                    "async-get2"
                                    {:transcoder (cb-client/get-transcoder
                                                  (tf/get-client))})]
      (is (instance? couchbase_clj.future.CouchbaseCljGetFuture fut1))
      (is (instance? couchbase_clj.future.CouchbaseCljGetFuture fut2))
      (is (= @fut1) 1)
      (is (= @fut2) 2))))

(deftest get-test
  (testing "Synchronously get a value."
    (cb-client/delete (tf/get-client) :get1)
    (cb-client/delete (tf/get-client) :get2)
    (cb-client/add (tf/get-client) :get1 1)
    (cb-client/add (tf/get-client) :get2 2)
    (is (= (cb-client/get (tf/get-client) :get1) 1))
    (is (= (cb-client/get (tf/get-client)
                          "get2"
                          {:transcoder (cb-client/get-transcoder
                                        (tf/get-client))}) 2))))

(deftest get-json-test
  (testing "Synchronously get a JSON string value converted to a Clojure data."
    (cb-client/delete (tf/get-client) :get-json1)
    (cb-client/delete (tf/get-client) :get-json2)
    (cb-client/delete (tf/get-client) :get-json3)
    (cb-client/add-json (tf/get-client) :get-json1 "1")
    (cb-client/add-json (tf/get-client) :get-json2 {:a 1})
    (cb-client/add-json (tf/get-client) :get-json3 [1 2])
    (is (= (cb-client/get-json (tf/get-client) :get-json1) "1"))
    (is (= (cb-client/get-json (tf/get-client)
                               "get-json2"
                               {:transcoder (cb-client/get-transcoder
                                             (tf/get-client))})
           {:a 1}))
    (is (= (cb-client/get-json (tf/get-client) :get-json3) [1 2]))))

(deftest async-get-touch-test
  (testing "Asynchronously get the value and update the expiry."
    (cb-client/delete (tf/get-client) :async-get-touch1)
    (cb-client/delete (tf/get-client) :async-get-touch2)
    (cb-client/delete (tf/get-client) :async-get-touch3)
    (cb-client/add (tf/get-client) :async-get-touch1 1)
    (cb-client/add (tf/get-client) :async-get-touch2 2)
    (cb-client/add (tf/get-client) :async-get-touch3 3)
    (let [fut1 (cb-client/async-get-touch (tf/get-client) :async-get-touch1)
          fut2 (cb-client/async-get-touch (tf/get-client) "async-get-touch2" {:expiry 0})
          fut3 (cb-client/async-get-touch (tf/get-client)
                                          'async-get-touch3
                                          {:transcoder (cb-client/get-transcoder
                                                        (tf/get-client))})]
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut1))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut2))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut3))
      (is (instance? CASValue @fut1))
      (is (instance? CASValue @fut2))
      (is (instance? CASValue @fut3))
      (is (= (cb-client/get (tf/get-client) :async-get-touch1)
             (cb-client/cas-val @fut1)))
      (is (= (cb-client/get (tf/get-client) :async-get-touch2) 2))
      (is (= (cb-client/get (tf/get-client) :async-get-touch3)
             (cb-client/cas-val @fut3))))))

(deftest get-touch-test
  (testing "Synchronously get the value and update the expiry."
    (cb-client/delete (tf/get-client) :get-touch1)
    (cb-client/delete (tf/get-client) :get-touch2)
    (cb-client/delete (tf/get-client) :get-touch3)
    (cb-client/add (tf/get-client) :get-touch1 1)
    (cb-client/add (tf/get-client) :get-touch2 2)
    (cb-client/add (tf/get-client) :get-touch3 3)
    (let [rs1 (cb-client/get-touch (tf/get-client) :get-touch1)
          rs2 (cb-client/get-touch (tf/get-client) "get-touch2" {:expiry 0})
          rs3 (cb-client/get-touch (tf/get-client)
                                   'get-touch3
                                   {:transcoder (cb-client/get-transcoder
                                                 (tf/get-client))})]
      (is (instance? CASValue rs1))
      (is (instance? CASValue rs2))
      (is (instance? CASValue rs3))
      (is (= (cb-client/get (tf/get-client) :get-touch1)
             (cb-client/cas-val rs1)))
      (is (= (cb-client/get (tf/get-client) :get-touch2) 2))
      (is (= (cb-client/get (tf/get-client) :get-touch3)
             (cb-client/cas-val rs3))))))

(deftest async-get-multi-test
  (testing "Asynchronously get value values."
    (cb-client/delete (tf/get-client) :async-get-multi1)
    (cb-client/delete (tf/get-client) :async-get-multi2)
    (cb-client/add (tf/get-client) :async-get-multi1 1)
    (cb-client/add (tf/get-client) :async-get-multi2 2)
    (let [fut1 (cb-client/async-get-multi (tf/get-client) [:async-get-multi1])
          fut2 (cb-client/async-get-multi (tf/get-client)
                                          ["async-get-multi1" "async-get-multi2"]
                                          {:transcoder (cb-client/get-transcoder
                                                        (tf/get-client))})]
      (is (instance? couchbase_clj.future.CouchbaseCljBulkGetFuture fut1))
      (is (instance? couchbase_clj.future.CouchbaseCljBulkGetFuture fut2))
      (is (= @fut1) {"async-get-multi1" 1})
      (is (= @fut2) {"async-get-multi1" 1 "async-get-multi2" 2}))))

(deftest get-multi-test
  (testing "Synchronously get multi values."
    (cb-client/delete (tf/get-client) :get-multi1)
    (cb-client/delete (tf/get-client) :get-multi2)
    (cb-client/add (tf/get-client) :get-multi1 1)
    (cb-client/add (tf/get-client) :get-multi2 2)
    (is (= (cb-client/get-multi (tf/get-client) [:get-multi1]) {"get-multi1" 1}))
    (is (= (cb-client/get-multi (tf/get-client)
                                ["get-multi1" "get-multi2"]
                                {:transcoder (cb-client/get-transcoder
                                              (tf/get-client))})
           {"get-multi1" 1 "get-multi2" 2}))))

(deftest get-multi-json-test
  (testing (str "Synchronously get multi JSON string values that are converted
to a Clojure data.")
    (cb-client/delete (tf/get-client) :get-multi-json1)
    (cb-client/delete (tf/get-client) :get-multi-json2)
    (cb-client/delete (tf/get-client) :get-multi-json3)
    (cb-client/add-json (tf/get-client) :get-multi-json1 "1")
    (cb-client/add-json (tf/get-client) :get-multi-json2 {:a 1})
    (cb-client/add-json (tf/get-client) :get-multi-json3 [1 2])
    (is (= (cb-client/get-multi-json (tf/get-client) [:get-multi-json1])
           {"get-multi-json1" "1"}))
    (is (= (cb-client/get-multi-json (tf/get-client)
                                     ["get-multi-json1" "get-multi-json2"]
                                     {:transcoder (cb-client/get-transcoder
                                                   (tf/get-client))})
           {"get-multi-json1" "1" "get-multi-json2" {:a 1}}))
    (is (= (cb-client/get-multi-json (tf/get-client)
                                     ["get-multi-json3"]
                                     {:transcoder (cb-client/get-transcoder
                                                   (tf/get-client))})
           {"get-multi-json3" [1 2]}))))

(deftest async-get-lock-test
  (testing "Asynchronously get a value and lock."
    (cb-client/delete (tf/get-client) :async-get-lock1)
    (cb-client/delete (tf/get-client) :async-get-lock2)
    (cb-client/delete (tf/get-client) :async-get-lock3)
    (cb-client/add (tf/get-client) :async-get-lock1 1)
    (cb-client/add (tf/get-client) :async-get-lock2 2)
    (cb-client/add (tf/get-client) :async-get-lock3 3)
    (let [fut1 (cb-client/async-get-lock (tf/get-client) :async-get-lock1)
          fut2 (cb-client/async-get-lock (tf/get-client)
                                         "async-get-lock2"
                                         {:expiry 30})
          fut3 (cb-client/async-get-lock (tf/get-client)
                                         'async-get-lock3
                                         {:transcoder (cb-client/get-transcoder
                                                       (tf/get-client))})]
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut1))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut2))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut3))
      (is (instance? CASValue @fut1))
      (is (instance? CASValue @fut2))
      (is (instance? CASValue @fut3))
      (is (cb-client/locked? (tf/get-client) :async-get-lock1))
      (is (cb-client/locked? (tf/get-client) :async-get-lock2))
      (is (cb-client/locked? (tf/get-client) :async-get-lock3)))))

(deftest get-lock-test
  (testing "Synchronously get a value and lock."
    (cb-client/delete (tf/get-client) :get-lock1)
    (cb-client/delete (tf/get-client) :get-lock2)
    (cb-client/delete (tf/get-client) :get-lock3)
    (cb-client/add (tf/get-client) :get-lock1 1)
    (cb-client/add (tf/get-client) :get-lock2 2)
    (cb-client/add (tf/get-client) :get-lock3 3)
    (let [rs1 (cb-client/get-lock (tf/get-client) :get-lock1)
          rs2 (cb-client/get-lock (tf/get-client) "get-lock2" {:expiry 30})
          rs3 (cb-client/get-lock (tf/get-client)
                                  'get-lock3
                                  {:transcoder (cb-client/get-transcoder
                                                (tf/get-client))})]
      (is (instance? CASValue rs1))
      (is (instance? CASValue rs2))
      (is (instance? CASValue rs3))
      (is (cb-client/locked? (tf/get-client) :get-lock1))
      (is (cb-client/locked? (tf/get-client) :get-lock2))
      (is (cb-client/locked? (tf/get-client) :get-lock3)))))

(deftest locked?-test
  (testing "Whether key is locked or not."
    (cb-client/add (tf/get-client) :locked 1)
    (cb-client/get-lock (tf/get-client) :locked)
    (is (cb-client/locked? (tf/get-client) :locked))))

(deftest async-get-cas-test
  (testing "Asynchronously get a value and CASValue object."
    (cb-client/delete (tf/get-client) :async-get-cas1)
    (cb-client/delete (tf/get-client) :async-get-cas2)
    (cb-client/delete (tf/get-client) :async-get-cas3)
    (cb-client/add (tf/get-client) :async-get-cas1 1)
    (cb-client/add (tf/get-client) :async-get-cas2 2)
    (cb-client/add (tf/get-client) :async-get-cas3 3)
    (let [fut1 (cb-client/async-get-cas (tf/get-client) :async-get-cas1)
          fut2 (cb-client/async-get-cas (tf/get-client)
                                        "async-get-cas2"
                                        {:expiry 30})
          fut3 (cb-client/async-get-cas (tf/get-client)
                                        'async-get-cas3
                                        {:transcoder (cb-client/get-transcoder
                                                      (tf/get-client))})]
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut1))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut2))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut3))
      (is (instance? CASValue @fut1))
      (is (instance? CASValue @fut2))
      (is (instance? CASValue @fut3)))))

(deftest get-cas-test
  (testing "Synchronously get a value and CASValue object."
    (cb-client/delete (tf/get-client) :get-cas1)
    (cb-client/delete (tf/get-client) :get-cas2)
    (cb-client/delete (tf/get-client) :get-cas3)
    (cb-client/add (tf/get-client) :get-cas1 1)
    (cb-client/add (tf/get-client) :get-cas2 2)
    (cb-client/add (tf/get-client) :get-cas3 3)
    (let [rs1 (cb-client/get-cas (tf/get-client) :get-cas1)
          rs2 (cb-client/get-cas (tf/get-client) "get-cas2" {:expiry 30})
          rs3 (cb-client/get-cas
                (tf/get-client)
                'get-cas3 {:transcoder (cb-client/get-transcoder
                                        (tf/get-client))})]
      (is (instance? CASValue rs1))
      (is (instance? CASValue rs2))
      (is (instance? CASValue rs3)))))

(deftest get-cas-id-test
  (testing "Synchronously get a value and CASValue object."
    (cb-client/delete (tf/get-client) :get-cas-id1)
    (cb-client/delete (tf/get-client) :get-cas-id2)
    (cb-client/delete (tf/get-client) :get-cas-id3)
    (cb-client/add (tf/get-client) :get-cas-id1 1)
    (cb-client/add (tf/get-client) :get-cas-id2 2)
    (cb-client/add (tf/get-client) :get-cas-id3 3)
    (let [rs1 (cb-client/get-cas-id (tf/get-client) :get-cas-id1)
          rs2 (cb-client/get-cas-id (tf/get-client) "get-cas-id2" {:expiry 30})
          rs3 (cb-client/get-cas-id (tf/get-client)
                                    'get-cas-id3
                                    {:transcoder (cb-client/get-transcoder
                                                  (tf/get-client))})]
      (is (instance? Long rs1))
      (is (instance? Long rs2))
      (is (instance? Long rs3)))))

(deftest async-inc-test
  (testing "Asynchronously increment a value."
    (cb-client/delete (tf/get-client) :async-inc1)
    (cb-client/delete (tf/get-client) :async-inc2)
    (cb-client/add (tf/get-client) :async-inc1 "1")
    (cb-client/add (tf/get-client) :async-inc2 "2")
    (let [fut1 (cb-client/async-inc (tf/get-client) :async-inc1)
          fut2 (cb-client/async-inc (tf/get-client) "async-inc2" {:offset 2})]
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut1))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut2))
      (is (= @fut1 2))
      (is (= @fut2 4)))))
  
(deftest inc-test
  (testing "Synchronously increment a value."
    (cb-client/delete (tf/get-client) :inc1)
    (cb-client/delete (tf/get-client) :inc2)
    (cb-client/delete (tf/get-client) :inc3)
    (cb-client/delete (tf/get-client) :inc4)
    (cb-client/add (tf/get-client) :inc1 "1")
    (cb-client/add (tf/get-client) :inc2 "2")
    (cb-client/add (tf/get-client) :inc3 "3")
    (let [rs1 (cb-client/inc (tf/get-client) :inc1)
          rs2 (cb-client/inc (tf/get-client) "inc2" {:expiry 30})
          rs3 (cb-client/inc (tf/get-client) 'inc3 {:offset 3})
          rs4 (cb-client/inc (tf/get-client) :inc4 {:default 4})]
      (is (= rs1 2))
      (is (= rs2 3))
      (is (= rs3 6))
      (is (= rs4 4)))))

(deftest async-dec-test
  (testing "Asynchronously decrement a value."
    (cb-client/delete (tf/get-client) :async-dec1)
    (cb-client/delete (tf/get-client) :async-dec2)
    (cb-client/add (tf/get-client) :async-dec1 "1")
    (cb-client/add (tf/get-client) :async-dec2 "2")
    (let [fut1 (cb-client/async-dec (tf/get-client) :async-dec1)
          fut2 (cb-client/async-dec (tf/get-client) "async-dec2" {:offset 2})]
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut1))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut2))
      (is (= @fut1 0))
      (is (= @fut2 0)))))
  
(deftest dec-test
  (testing "Synchronously decrement a value."
    (cb-client/delete (tf/get-client) :dec1)
    (cb-client/delete (tf/get-client) :dec2)
    (cb-client/delete (tf/get-client) :dec3)
    (cb-client/delete (tf/get-client) :dec4)
    (cb-client/add (tf/get-client) :dec1 "1")
    (cb-client/add (tf/get-client) :dec2 "2")
    (cb-client/add (tf/get-client) :dec3 "3")
    (let [rs1 (cb-client/dec (tf/get-client) :dec1)
          rs2 (cb-client/dec (tf/get-client) "dec2" {:expiry 30})
          rs3 (cb-client/dec (tf/get-client) 'dec3 {:offset 3})
          rs4 (cb-client/dec (tf/get-client) :dec4 {:default 4})]
      (is (= rs1 0))
      (is (= rs2 1))
      (is (= rs3 0))
      (is (= rs4 4)))))

(deftest async-replace-test
  (testing "Asynchronously replacing a value."
    (cb-client/delete (tf/get-client) :async-replace1)
    (cb-client/delete (tf/get-client) :async-replace2)
    (cb-client/delete (tf/get-client) :async-replace3)
    (cb-client/delete (tf/get-client) :async-replace4)
    (cb-client/add (tf/get-client) :async-replace1 1)
    (cb-client/add (tf/get-client) :async-replace2 2)
    (cb-client/add (tf/get-client) :async-replace3 3)
    (cb-client/add (tf/get-client) :async-replace4 4)
    (let [fut1 (cb-client/async-replace (tf/get-client) :async-replace1 11)
          fut2 (cb-client/async-replace (tf/get-client)
                                        "async-replace2"
                                        22
                                        {:expiry 30})
          fut3 (cb-client/async-replace (tf/get-client)
                                        'async-replace3
                                        "11"
                                        {:transcoder (cb-client/get-transcoder
                                                      (tf/get-client))})
          fut4 (cb-client/async-replace (tf/get-client)
                                        :async-replace4
                                        "aa"
                                        {:observe true
                                         :persist :master
                                         :replicate :zero})]
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut1))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut2))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut3))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut4))
      (is (true? @fut1))
      (is (true? @fut2))
      (is (true? @fut3))
      (is (true? @fut4))
      (is (= (cb-client/get (tf/get-client) :async-replace1) 11))
      (is (= (cb-client/get (tf/get-client) :async-replace2) 22))
      (is (= (cb-client/get (tf/get-client) :async-replace3) "11"))
      (is (= (cb-client/get (tf/get-client) :async-replace4) "aa")))))

(deftest replace-test
  (testing "Synchronously replacing a value."
    (cb-client/delete (tf/get-client) :replace1)
    (cb-client/delete (tf/get-client) :replace2)
    (cb-client/delete (tf/get-client) :replace3)
    (cb-client/delete (tf/get-client) :replace4)
    (cb-client/delete (tf/get-client) :replace5)
    (cb-client/add (tf/get-client) :replace1 1)
    (cb-client/add (tf/get-client) :replace2 2)
    (cb-client/add (tf/get-client) :replace3 3)
    (cb-client/add (tf/get-client) :replace4 4)
    (cb-client/add (tf/get-client) :replace5 5)
    (let [rs1 (cb-client/replace (tf/get-client) :replace1 11)
          rs2 (cb-client/replace (tf/get-client) "replace2" 22 {:expiry 30})
          rs3 (cb-client/replace (tf/get-client)
                                 'replace3
                                 "11"
                                 {:transcoder (cb-client/get-transcoder
                                               (tf/get-client))})
          rs4 (cb-client/replace
                (tf/get-client) :replace4 "aa"
                {:observe true :persist :master :replicate :zero})
          rs5 (cb-client/replace (tf/get-client) :replace5 33 {:timeout 1000})]
      (is (true? rs1))
      (is (true? rs2))
      (is (true? rs3))
      (is (true? rs4))
      (is (true? rs5))
      (is (= (cb-client/get (tf/get-client) :replace1) 11))
      (is (= (cb-client/get (tf/get-client) :replace2) 22))
      (is (= (cb-client/get (tf/get-client) :replace3) "11"))
      (is (= (cb-client/get (tf/get-client) :replace4) "aa"))
      (is (= (cb-client/get (tf/get-client) :replace5) 33)))))

(deftest async-replace-json-test
  (testing "Asynchronously replacing a JSON string value converted
to a Clojure data."
    (cb-client/delete (tf/get-client) :async-replace-json1)
    (cb-client/delete (tf/get-client) :async-replace-json2)
    (cb-client/delete (tf/get-client) :async-replace-json3)
    (cb-client/delete (tf/get-client) :async-replace-json4)
    (cb-client/add (tf/get-client) :async-replace-json1 1)
    (cb-client/add (tf/get-client) :async-replace-json2 2)
    (cb-client/add (tf/get-client) :async-replace-json3 3)
    (cb-client/add (tf/get-client) :async-replace-json4 4)
    (let [fut1 (cb-client/async-replace-json (tf/get-client)
                                             :async-replace-json1
                                             "11")
          fut2 (cb-client/async-replace-json (tf/get-client)
                                             "async-replace-json2"
                                             "22"
                                             {:expiry 30})
          fut3 (cb-client/async-replace-json
                (tf/get-client)
                'async-replace-json3
                {:a 1}
                {:transcoder (cb-client/get-transcoder (tf/get-client))})
          fut4 (cb-client/async-replace-json (tf/get-client)
                                             :async-replace-json4
                                             [1 2]
                                             {:observe true
                                              :persist :master
                                              :replicate :zero})]
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut1))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut2))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut3))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut4))
      (is (true? @fut1))
      (is (true? @fut2))
      (is (true? @fut3))
      (is (true? @fut4))
      (is (= (cb-client/get-json (tf/get-client) :async-replace-json1) "11"))
      (is (= (cb-client/get-json (tf/get-client) :async-replace-json2) "22"))
      (is (= (cb-client/get-json (tf/get-client) :async-replace-json3) {:a 1}))
      (is (= (cb-client/get-json (tf/get-client) :async-replace-json4) [1 2])))))

(deftest replace-json-test
  (testing "Synchronously replacing a JSON string value converted
to a Clojure data."
    (cb-client/delete (tf/get-client) :replace-json1)
    (cb-client/delete (tf/get-client) :replace-json2)
    (cb-client/delete (tf/get-client) :replace-json3)
    (cb-client/delete (tf/get-client) :replace-json4)
    (cb-client/delete (tf/get-client) :replace-json5)
    (cb-client/add (tf/get-client) :replace-json1 1)
    (cb-client/add (tf/get-client) :replace-json2 2)
    (cb-client/add (tf/get-client) :replace-json3 3)
    (cb-client/add (tf/get-client) :replace-json4 4)
    (cb-client/add (tf/get-client) :replace-json5 5)
    (let [rs1 (cb-client/replace-json (tf/get-client) :replace-json1 "11")
          rs2 (cb-client/replace-json (tf/get-client)
                                      "replace-json2"
                                      "22"
                                      {:expiry 30})
          rs3 (cb-client/replace-json (tf/get-client)
                                      'replace-json3
                                      {:a 1}
                                      {:transcoder (cb-client/get-transcoder
                                                    (tf/get-client))})
          rs4 (cb-client/replace-json (tf/get-client)
                                      :replace-json4
                                      [1 2]
                                      {:observe true
                                       :persist :master
                                       :replicate :zero})
          rs5 (cb-client/replace-json (tf/get-client)
                                      :replace-json5
                                      "33"
                                      {:timeout 1000})]
      (is (true? rs1))
      (is (true? rs2))
      (is (true? rs3))
      (is (true? rs4))
      (is (true? rs5))
      (is (= (cb-client/get-json (tf/get-client) :replace-json1) "11"))
      (is (= (cb-client/get-json (tf/get-client) :replace-json2) "22"))
      (is (= (cb-client/get-json (tf/get-client) :replace-json3) {:a 1}))
      (is (= (cb-client/get-json (tf/get-client) :replace-json4) [1 2]))
      (is (= (cb-client/get-json (tf/get-client) :replace-json5) "33")))))

(deftest async-set-test
  (testing "Asynchronously set a value."
    (cb-client/delete (tf/get-client) :async-set1)
    (cb-client/delete (tf/get-client) :async-set2)
    (cb-client/delete (tf/get-client) :async-set3)
    (cb-client/delete (tf/get-client) :async-set4)
    (cb-client/add (tf/get-client) :async-set1 1)
    (cb-client/add (tf/get-client) :async-set2 2)
    (cb-client/add (tf/get-client) :async-set3 3)
    (cb-client/add (tf/get-client) :async-set4 4)
    (let [fut1 (cb-client/async-set (tf/get-client) :async-set1 11)
          fut2 (cb-client/async-set (tf/get-client) "async-set2" 22 {:expiry 30})
          fut3 (cb-client/async-set (tf/get-client)
                                    'async-set3
                                    "11"
                                    {:transcoder (cb-client/get-transcoder
                                                  (tf/get-client))})
          fut4 (cb-client/async-set (tf/get-client)
                                    :async-set4
                                    "aa"
                                    {:observe true
                                     :persist :master
                                     :replicate :zero})]
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut1))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut2))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut3))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut4))
      (is (true? @fut1))
      (is (true? @fut2))
      (is (true? @fut3))
      (is (true? @fut4))
      (is (= (cb-client/get (tf/get-client) :async-set1) 11))
      (is (= (cb-client/get (tf/get-client) :async-set2) 22))
      (is (= (cb-client/get (tf/get-client) :async-set3) "11"))
      (is (= (cb-client/get (tf/get-client) :async-set4) "aa")))))

(deftest set-test
  (testing "Synchronously set a value."
    (cb-client/delete (tf/get-client) :set1)
    (cb-client/delete (tf/get-client) :set2)
    (cb-client/delete (tf/get-client) :set3)
    (cb-client/delete (tf/get-client) :set4)
    (cb-client/delete (tf/get-client) :set5)
    (cb-client/add (tf/get-client) :set1 1)
    (cb-client/add (tf/get-client) :set2 2)
    (cb-client/add (tf/get-client) :set3 3)
    (cb-client/add (tf/get-client) :set4 4)
    (cb-client/add (tf/get-client) :set5 5)
    (let [rs1 (cb-client/set (tf/get-client) :set1 11)
          rs2 (cb-client/set (tf/get-client) "set2" 22 {:expiry 30})
          rs3 (cb-client/set (tf/get-client)
                             'set3
                             "11"
                             {:transcoder (cb-client/get-transcoder
                                           (tf/get-client))})
          rs4 (cb-client/set (tf/get-client)
                             :set4
                             "aa"
                             {:observe true
                              :persist :master
                              :replicate :zero})
          rs5 (cb-client/set (tf/get-client) :set5 33 {:timeout 1000})]
      (is (true? rs1))
      (is (true? rs2))
      (is (true? rs3))
      (is (true? rs4))
      (is (true? rs5))
      (is (= (cb-client/get (tf/get-client) :set1) 11))
      (is (= (cb-client/get (tf/get-client) :set2) 22))
      (is (= (cb-client/get (tf/get-client) :set3) "11"))
      (is (= (cb-client/get (tf/get-client) :set4) "aa"))
      (is (= (cb-client/get (tf/get-client) :set5) 33)))))

(deftest async-set-json-test
  (testing "Asynchronously set a JSON string value converted to a Clojure data."
    (cb-client/delete (tf/get-client) :async-set-json1)
    (cb-client/delete (tf/get-client) :async-set-json2)
    (cb-client/delete (tf/get-client) :async-set-json3)
    (cb-client/delete (tf/get-client) :async-set-json4)
    (cb-client/add (tf/get-client) :async-set-json1 1)
    (cb-client/add (tf/get-client) :async-set-json2 2)
    (cb-client/add (tf/get-client) :async-set-json3 3)
    (cb-client/add (tf/get-client) :async-set-json4 4)
    (let [fut1 (cb-client/async-set-json (tf/get-client) :async-set-json1 "11")
          fut2 (cb-client/async-set-json (tf/get-client)
                                         "async-set-json2"
                                         "22"
                                         {:expiry 30})
          fut3 (cb-client/async-set-json (tf/get-client)
                                         'async-set-json3
                                         {:a 1}
                                         {:transcoder (cb-client/get-transcoder
                                                       (tf/get-client))})
          fut4 (cb-client/async-set-json (tf/get-client)
                                         :async-set-json4
                                         [1 2]
                                         {:observe true
                                          :persist :master
                                          :replicate :zero})]
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut1))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut2))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut3))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut4))
      (is (true? @fut1))
      (is (true? @fut2))
      (is (true? @fut3))
      (is (true? @fut4))
      (is (= (cb-client/get-json (tf/get-client) :async-set-json1) "11"))
      (is (= (cb-client/get-json (tf/get-client) :async-set-json2) "22"))
      (is (= (cb-client/get-json (tf/get-client) :async-set-json3) {:a 1}))
      (is (= (cb-client/get-json (tf/get-client) :async-set-json4) [1 2])))))

(deftest set-json-test
  (testing "Synchronously set a JSON string value converted to a Clojure data."
    (cb-client/delete (tf/get-client) :set-json1)
    (cb-client/delete (tf/get-client) :set-json2)
    (cb-client/delete (tf/get-client) :set-json3)
    (cb-client/delete (tf/get-client) :set-json4)
    (cb-client/delete (tf/get-client) :set-json5)
    (cb-client/add (tf/get-client) :set-json1 1)
    (cb-client/add (tf/get-client) :set-json2 2)
    (cb-client/add (tf/get-client) :set-json3 3)
    (cb-client/add (tf/get-client) :set-json4 4)
    (cb-client/add (tf/get-client) :set-json5 5)
    (let [rs1 (cb-client/set-json (tf/get-client) :set-json1 "11")
          rs2 (cb-client/set-json (tf/get-client) "set-json2" "22" {:expiry 30})
          rs3 (cb-client/set-json (tf/get-client)
                                  'set-json3
                                  {:a 1}
                                  {:transcoder (cb-client/get-transcoder
                                                (tf/get-client))})
          rs4 (cb-client/set-json (tf/get-client)
                                  :set-json4
                                  [1 2]
                                  {:observe true
                                   :persist :master
                                   :replicate :zero})
          rs5 (cb-client/set-json (tf/get-client)
                                  :set-json5
                                  "33"
                                  {:timeout 1000})]
      (is (true? rs1))
      (is (true? rs2))
      (is (true? rs3))
      (is (true? rs4))
      (is (true? rs5))
      (is (= (cb-client/get-json (tf/get-client) :set-json1) "11"))
      (is (= (cb-client/get-json (tf/get-client) :set-json2) "22"))
      (is (= (cb-client/get-json (tf/get-client) :set-json3) {:a 1}))
      (is (= (cb-client/get-json (tf/get-client) :set-json4) [1 2]))
      (is (= (cb-client/get-json (tf/get-client) :set-json5) "33")))))

(deftest async-set-cas-test
  (testing "Asynchronously compare and set a value."
    (cb-client/delete (tf/get-client) :async-set-cas1)
    (cb-client/delete (tf/get-client) :async-set-cas2)
    (cb-client/delete (tf/get-client) :async-set-cas3)
    (cb-client/add (tf/get-client) :async-set-cas1 1)
    (cb-client/add (tf/get-client) :async-set-cas2 2)
    (cb-client/add (tf/get-client) :async-set-cas3 3)
    (let [fut1 (cb-client/async-set-cas (tf/get-client)
                                        :async-set-cas1
                                        11
                                        (cb-client/get-cas-id (tf/get-client)
                                                              :async-set-cas1))
          fut2 (cb-client/async-set-cas (tf/get-client)
                                        "async-set-cas2" 22
                                        (cb-client/get-cas-id (tf/get-client)
                                                              "async-set-cas2")
                                        {:expiry 30})
          fut3 (cb-client/async-set-cas (tf/get-client)
                                        'async-set-cas3
                                        "11"
                                        (cb-client/get-cas-id (tf/get-client)
                                                              'async-set-cas3)
                                        {:transcoder (cb-client/get-transcoder
                                                      (tf/get-client))})]
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut1))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut2))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut3))
      (is (= (@fut1 :ok)))
      (is (= (@fut2 :ok)))
      (is (= (@fut3 :ok)))
      (is (= (cb-client/get (tf/get-client) :async-set-cas1) 11))
      (is (= (cb-client/get (tf/get-client) :async-set-cas2) 22))
      (is (= (cb-client/get (tf/get-client) :async-set-cas3) "11")))))

(deftest set-cas-test
  (testing "Synchronously compare and set a value."
    (cb-client/delete (tf/get-client) :set-cas1)
    (cb-client/delete (tf/get-client) :set-cas2)
    (cb-client/delete (tf/get-client) :set-cas3)
    (cb-client/add (tf/get-client) :set-cas1 1)
    (cb-client/add (tf/get-client) :set-cas2 2)
    (cb-client/add (tf/get-client) :set-cas3 3)
    (let [rs1 (cb-client/set-cas (tf/get-client)
                                 :set-cas1
                                 11
                                 (cb-client/get-cas-id (tf/get-client)
                                                       :set-cas1))
          rs2 (cb-client/set-cas (tf/get-client)
                                 "set-cas2"
                                 22
                                 (cb-client/get-cas-id (tf/get-client)
                                                       "set-cas2")
                                 {:expiry 30})
          rs3 (cb-client/set-cas (tf/get-client)
                                 'set-cas3
                                 "11"
                                 (cb-client/get-cas-id (tf/get-client)
                                                       'set-cas3)
                                 {:transcoder (cb-client/get-transcoder
                                               (tf/get-client))})]
      (is (= rs1 :ok))
      (is (= rs2 :ok))
      (is (= rs3 :ok))
      (is (= (cb-client/get (tf/get-client) :set-cas1) 11))
      (is (= (cb-client/get (tf/get-client) :set-cas2) 22))
      (is (= (cb-client/get (tf/get-client) :set-cas3) "11")))))

(deftest async-set-cas-json-test
  (testing "Asynchronously compare and set a JSON string value converted
to a Clojure data."
    (cb-client/delete (tf/get-client) :async-set-cas-json1)
    (cb-client/delete (tf/get-client) :async-set-cas-json2)
    (cb-client/delete (tf/get-client) :async-set-cas-json3)
    (cb-client/add (tf/get-client) :async-set-cas-json1 1)
    (cb-client/add (tf/get-client) :async-set-cas-json2 2)
    (cb-client/add (tf/get-client) :async-set-cas-json3 3)
    (let [fut1 (cb-client/async-set-cas-json
                (tf/get-client)
                :async-set-cas-json1
                "11"
                (cb-client/get-cas-id (tf/get-client) :async-set-cas-json1))
          fut2 (cb-client/async-set-cas-json
                (tf/get-client)
                "async-set-cas-json2"
                "22"
                (cb-client/get-cas-id (tf/get-client) "async-set-cas-json2")
                {:expiry 30})
          fut3 (cb-client/async-set-cas-json
                (tf/get-client)
                'async-set-cas-json3
                {:a 1}
                (cb-client/get-cas-id (tf/get-client) 'async-set-cas-json3)
                {:transcoder (cb-client/get-transcoder (tf/get-client))})]
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut1))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut2))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut3))
      (is (= @fut1 :ok))
      (is (= @fut2 :ok))
      (is (= @fut3 :ok))
      (is (= (cb-client/get-json (tf/get-client) :async-set-cas-json1) "11"))
      (is (= (cb-client/get-json (tf/get-client) :async-set-cas-json2) "22"))
      (is (= (cb-client/get-json (tf/get-client) :async-set-cas-json3)
             {:a 1})))))

(deftest set-cas-json-test
  (testing "Synchronously compare and set a JSON string value converted
to a Clojure data."
    (cb-client/delete (tf/get-client) :set-cas-json1)
    (cb-client/delete (tf/get-client) :set-cas-json2)
    (cb-client/delete (tf/get-client) :set-cas-json3)
    (cb-client/add (tf/get-client) :set-cas-json1 1)
    (cb-client/add (tf/get-client) :set-cas-json2 2)
    (cb-client/add (tf/get-client) :set-cas-json3 3)
    (let [rs1 (cb-client/set-cas-json (tf/get-client)
                                      :set-cas-json1
                                      "11"
                                      (cb-client/get-cas-id (tf/get-client)
                                                            :set-cas-json1))
          rs2 (cb-client/set-cas-json (tf/get-client)
                                      "set-cas-json2"
                                      "22"
                                      (cb-client/get-cas-id (tf/get-client)
                                                            "set-cas-json2")
                                      {:expiry 30})
          rs3 (cb-client/set-cas-json
                (tf/get-client)
                'set-cas-json3 {:a 1}
                (cb-client/get-cas-id (tf/get-client) 'set-cas-json3)
                {:transcoder (cb-client/get-transcoder (tf/get-client))})]
      (is (= rs1 :ok))
      (is (= rs2 :ok))
      (is (= rs3 :ok))
      (is (= (cb-client/get-json (tf/get-client) :set-cas-json1) "11"))
      (is (= (cb-client/get-json (tf/get-client) :set-cas-json2) "22"))
      (is (= (cb-client/get-json (tf/get-client) :set-cas-json3) {:a 1})))))

(deftest async-touch-test
  (testing "Asynchronously update the expiry."
    (cb-client/delete (tf/get-client) :async-touch1)
    (cb-client/delete (tf/get-client) :async-touch2)
    (cb-client/add (tf/get-client) :async-touch1 1)
    (cb-client/add (tf/get-client) :async-touch2 2)
    (let [fut1 (cb-client/async-touch (tf/get-client) :async-touch1)
          fut2 (cb-client/async-touch (tf/get-client)
                                      "async-touch2"
                                      {:expiry 0})]
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut1))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut2))
      (is (true? @fut1))
      (is (true? @fut2)))))

(deftest touch-test
  (testing "Synchronously update the expiry."
    (cb-client/delete (tf/get-client) :async-touch1)
    (cb-client/delete (tf/get-client) :async-touch2)
    (cb-client/delete (tf/get-client) :async-touch3)
    (cb-client/add (tf/get-client) :async-touch1 1)
    (cb-client/add (tf/get-client) :async-touch2 2)
    (cb-client/add (tf/get-client) :async-touch3 3)
    (let [rs1 (cb-client/touch (tf/get-client) :async-touch1)
          rs2 (cb-client/touch (tf/get-client) "async-touch2" {:expiry 0})
          rs3 (cb-client/touch (tf/get-client) 'async-touch1 {:timeout 1000})]
      (is (true? rs1))
      (is (true? rs2))
      (is (true? rs3)))))

(deftest async-unlock-test
  (testing "Asynchronously unlock the key."
    (cb-client/delete (tf/get-client) :async-unlock)
    (cb-client/delete (tf/get-client) :async-unlock)
    (cb-client/add (tf/get-client) :async-unlock1 1)
    (cb-client/add (tf/get-client) :async-unlock2 2)
    (let [fut1 (cb-client/async-unlock (tf/get-client)
                                       :async-unlock1
                                       (-> (cb-client/get-lock (tf/get-client)
                                                               :async-unlock1)
                                           cb-client/cas-id))
          fut2 (cb-client/async-unlock (tf/get-client)
                                       "async-unlock2"
                                       (-> (cb-client/get-lock (tf/get-client)
                                                               "async-unlock2")
                                           cb-client/cas-id)
                                       {:transcoder (cb-client/get-transcoder
                                                     (tf/get-client))})]
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut1))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut2))
      (is (true? @fut1))
      (is (true? @fut2)))))

(deftest unlock-test
  (testing "Synchronously unlock the key."
    (cb-client/delete (tf/get-client) :unlock)
    (cb-client/delete (tf/get-client) :unlock)
    (cb-client/add (tf/get-client) :unlock1 1)
    (cb-client/add (tf/get-client) :unlock2 2)
    (let [rs1 (cb-client/unlock (tf/get-client)
                                :unlock1
                                (-> (cb-client/get-lock (tf/get-client) :unlock1)
                                    cb-client/cas-id))
          rs2 (cb-client/unlock (tf/get-client)
                                "unlock2"
                                (-> (cb-client/get-lock (tf/get-client)
                                                        "unlock2")
                                    cb-client/cas-id)
                                {:transcoder (cb-client/get-transcoder
                                              (tf/get-client))})]
      (is (true? rs1))
      (is (true? rs2)))))

(deftest async-get-view-test
  (testing "Asynchronously get a view."
    (let [fut (cb-client/async-get-view (tf/get-client) tf/design-doc tf/view)]
      (is (instance? couchbase_clj.future.CouchbaseCljHttpFuture fut))
      (is (instance? View @fut)))))

(deftest get-view-test
  (testing "Synchronously get a view."
    (let [view (cb-client/get-view (tf/get-client) tf/design-doc tf/view)]
      (is (instance? View view)))))


;; TODO: Currently not supported due to API change in the Couchbase Client.
;(deftest async-get-views-test
;  (testing "Asynchronously get a sequence of views."
;    (let [fut (cb-client/async-get-views (tf/get-client) tf/design-doc)]
;      (is (instance? couchbase_clj.future.CouchbaseCljHttpFuture fut))
;      (is (seq? @fut)))))
;
;(deftest get-views-test
;  (testing "Synchronously get a sequence of views."
;    (let [views (cb-client/get-views (tf/get-client) tf/design-doc)]
;      (is (seq? views)))))

(deftest async-query-test
  (testing "Asynchronously query a view."
    (let [q (cb-query/create-query {:limit 100})
          view (cb-client/get-view (tf/get-client) tf/design-doc tf/view)
          fut1 (cb-client/async-query (tf/get-client) view q)
          fut2 (cb-client/async-query (tf/get-client) tf/design-doc tf/view q)
          fut3 (cb-client/async-query (tf/get-client)
                                      tf/design-doc
                                      tf/view
                                      {:limit 100})]
      (is (instance? couchbase_clj.future.CouchbaseCljHttpFuture fut1))
      (is (instance? couchbase_clj.future.CouchbaseCljHttpFuture fut2))
      (is (instance? couchbase_clj.future.CouchbaseCljHttpFuture fut3))
      (is (seq? @fut1))
      (is (seq? @fut2))
      (is (seq? @fut3)))))

(deftest query-test
  (testing "Synchronously query a view."
    (let [q (cb-query/create-query {:limit 100})
          view (cb-client/get-view (tf/get-client) tf/design-doc tf/view)
          rs1 (cb-client/query (tf/get-client) view q)
          rs2 (cb-client/query (tf/get-client) tf/design-doc tf/view q)
          rs3 (cb-client/query (tf/get-client)
                               tf/design-doc
                               tf/view
                               {:limit 100})]
      (is (seq? rs1))
      (is (seq? rs2))
      (is (seq? rs3)))))

(deftest lazy-query-test
  (testing "Lazily query a view."
    (let [q (cb-query/create-query {})
          view (cb-client/get-view (tf/get-client) tf/design-doc tf/view)
          rs1 (cb-client/lazy-query (tf/get-client) view q 5)
          rs2 (cb-client/lazy-query (tf/get-client) tf/design-doc tf/view q 5)
          rs3 (cb-client/lazy-query (tf/get-client) tf/design-doc tf/view {} 5)]
      (is (seq? rs1))
      (is (seq? rs2))
      (is (seq? rs3)))))

(deftest wait-queue-test
  (testing "Synchronously wait for queues."
    (is (true? (cb-client/wait-queue (tf/get-client))))
    (is (true? (cb-client/wait-queue (tf/get-client) 2000)))))

;; TODO: Currently not working
;(deftest flush-test
;  (testing "Flushing of all cache and persistent data."
;    (is (true? (cb-client/flush (tf/get-client))))
;    (is (true? (cb-client/flush (tf/get-client) 1000)))))

(deftest shutdown-test
  (testing "Shutdown of Couchbase client."
    (let [c (cb-client/create-client {:bucket tf/bucket
                                :username tf/bucket-username
                                :password tf/bucket-password
                                :uris tf/uris
                                :op-timeout 20000})]
      (is (true? (cb-client/shutdown c)))
      (is (false? (cb-client/shutdown c))))))

(deftest view-id-test
  (testing "Get the ID of query result."
    (let [rs (cb-client/query (tf/get-client) tf/design-doc tf/view {})
          row (first rs)]
      (is (instance? ViewRow row))
      (is (not (nil? (cb-client/view-id row)))))))

(deftest view-key-test
  (testing "Get the key of query result."
    (let [rs (cb-client/query (tf/get-client) tf/design-doc tf/view {})
          row (first rs)]
      (is (instance? ViewRow row))
      (is (not (nil? (cb-client/view-key row)))))))

(deftest view-key-json-est
  (testing "Get the JSON string key of query result"
    (let [rs (cb-client/query (tf/get-client) tf/design-doc tf/view2 {})
          row (first rs)]
      (is (instance? ViewRow row))
      (is (not (nil? (cb-client/view-val row)))))))

(deftest view-val-test
  (testing "Get the value of query result."
    (let [rs (cb-client/query (tf/get-client) tf/design-doc tf/view {})
          row (first rs)]
      (is (instance? ViewRow row))
      (is (not (nil? (cb-client/view-val row)))))))

(deftest view-val-json-test
  (testing "Get the JSON string value of query result converted
to a Clojure data."
    (let [rs (cb-client/query (tf/get-client) tf/design-doc tf/view2 {})
          row (first rs)]
      (is (instance? ViewRow row))
      (is (not (nil? (cb-client/view-val-json row)))))))

(deftest view-doc-test
  (testing "Get the document of query result."
    (let [rs (cb-client/query (tf/get-client)
                              tf/design-doc
                              tf/view
                              {:include-docs true})
          row (first rs)]
      (is (instance? ViewRow row))
      (is (not (nil? (cb-client/view-doc row)))))))

(deftest view-doc-json-test
  (testing "Get the JSON string document of query result converted
to a Clojure data"
    (let [rs (cb-client/query (tf/get-client)
                              tf/design-doc
                              tf/view2
                              {:include-docs true})
          row (first rs)]
      (is (instance? ViewRow row))
      (is (not (nil? (cb-client/view-doc-json row)))))))

(deftest create-client-test
  (testing "Creation of couchbase client."
    (let [c (cb-client/create-client
             {:bucket tf/bucket
              :username tf/bucket-username
              :password tf/bucket-password
              :uris tf/uris
              :auth-descriptor (AuthDescriptor. (into-array ["plain"])
                                                (PlainCallbackHandler. "" ""))
              :daemon true
              :failure-mode :retry
              :hash-alg :crc-hash
              :max-reconnect-delay 500
              ;:min-reconnect-interval 2000
              :obs-poll-interval 1000
              :obs-poll-max 30
              :op-queue-max-block-time 20000
              :op-timeout 20000
              :read-buffer-size 17000
              :should-optimize false
              :timeout-exception-threshold 33333
              :transcoder (LongTranscoder.)
              :use-nagle-algorithm true})]
      (cb-client/add c :create-client1 1)
      (is (= (cb-client/get c :create-client1) 1))
      (is (instance? AuthDescriptor (cb-client/get-auth-descriptor c)))
      (is (= (cb-client/daemon? c) true))
      (is (= (cb-client/get-failure-mode c) FailureMode/Retry))
      (is (= (cb-client/get-hash-alg c) DefaultHashAlgorithm/CRC_HASH))
      (is (= (cb-client/get-max-reconnect-delay c) 500))
      ;(is (= (cb-client/get-min-reconnect-interval c) 2000))
      (is (= (cb-client/get-op-queue-max-block-time c) 20000))
      (is (= (cb-client/get-op-timeout c) 20000))
      (is (= (cb-client/get-read-buffer-size c) 17000))
      (is (false? (cb-client/should-optimize? c)))
      (is (= (cb-client/get-timeout-exception-threshold c) 33331))
      (is (instance? LongTranscoder (cb-client/get-transcoder c)))
      (is (= (cb-client/use-nagle-algorithm? c) true)))))

(deftest defclient-test
  (testing "Creation of couchbase client as a Var."
    (cb-client/defclient c {:bucket tf/bucket
                            :username tf/bucket-username
                            :password tf/bucket-password
                            :uris tf/uris})
    (cb-client/add c :defclient 1)
    (is (= (cb-client/get c :defclient) 1))))
