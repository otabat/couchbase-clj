(ns couchbase-clj.test.client
  (:import [net.spy.memcached PersistTo]
           [net.spy.memcached ReplicateTo]
           [net.spy.memcached CASValue]
           [net.spy.memcached CASResponse]
           [net.spy.memcached FailureMode]
           [net.spy.memcached DefaultHashAlgorithm]
           [net.spy.memcached.auth AuthDescriptor]
           [net.spy.memcached.auth PlainCallbackHandler]
           [net.spy.memcached.transcoders Transcoder]
           [net.spy.memcached.transcoders LongTranscoder]
           [net.spy.memcached.transcoders SerializingTranscoder]
           [net.spy.memcached.internal GetFuture]
           [net.spy.memcached.internal BulkGetFuture]
           [net.spy.memcached.internal OperationFuture]
           [com.couchbase.client CouchbaseClient]
           [com.couchbase.client CouchbaseConnectionFactory]
           [com.couchbase.client.vbucket VBucketNodeLocator]
           [com.couchbase.client.internal HttpFuture]
           [com.couchbase.client.protocol.views View]
           [com.couchbase.client.protocol.views ViewRow]
           [couchbaes_clj.future.CouchbaseCljBulkGetFuture]
           [couchbaes_clj.future.CouchbaseCljGetFuture]
           [couchbaes_clj.future.CouchbaseCljOperationFuture]
           [couchbaes_clj.future.CouchbaseCljHttpFuture]
           )
  (:require [couchbase-clj.client :as cbc]
            [couchbase-clj.query :as cbq]
            [couchbase-clj.util :as cbu]
            [couchbase-clj.test.fixture :as tf])
  (:use [clojure.test :exclude [set-test]]))

;(use-fixtures :once tf/setup-client tf/flush-data)
(use-fixtures :once tf/setup-client)

(deftest persist-to-test
  (testing "Conversion of a keyword to PersistTo object."
    (is (= (cbc/persist-to :master) PersistTo/MASTER))
    (is (= (cbc/persist-to :one) PersistTo/ONE))
    (is (= (cbc/persist-to :two) PersistTo/TWO))
    (is (= (cbc/persist-to :three) PersistTo/THREE))
    (is (= (cbc/persist-to :four) PersistTo/FOUR))
    (is (= (cbc/persist-to :else) PersistTo/MASTER))))
(deftest replicate-to-test
  (testing "Conversion of a keyword to a ReplicateTo object."
    (is (= (cbc/replicate-to :zero) ReplicateTo/ZERO))
    (is (= (cbc/replicate-to :one) ReplicateTo/ONE))
    (is (= (cbc/replicate-to :two) ReplicateTo/TWO))
    (is (= (cbc/replicate-to :three) ReplicateTo/THREE))
    (is (= (cbc/replicate-to :else) ReplicateTo/ZERO))))

(deftest cas-id-test
  (testing "Get the cas-id from the CASValue object."
    (cbc/add (tf/get-client) :cas-id 1)
    (is (instance? Long (cbc/cas-id (cbc/get-cas (tf/get-client) :cas-id))))))

(deftest cas-val-test
  (testing "Get the value from the CASValue object."
    (cbc/add (tf/get-client) :cas-val "cas-id")
    (is (= (cbc/cas-val (cbc/get-cas (tf/get-client) :cas-val)) "cas-id"))))

(deftest cas-val-json-test
  (testing "Get the JSONG string value converted to Clojure data from the CASValue object."
    (cbc/add-json (tf/get-client) :cas-val-json {:cas-val-json 1})
    (is (= (cbc/cas-val-json (cbc/get-cas (tf/get-client) :cas-val-json)) {:cas-val-json 1}))))

(deftest get-client-test
  (testing "Get the CouchbaseClient object."
    (is (->> (tf/get-client)
             cbc/get-client
             (instance? CouchbaseClient)))))

(deftest get-factory-test
  (testing "Get the CouchbaseConnectionFactory object."
    (is (->> (tf/get-client)
             cbc/get-factory
             (instance? CouchbaseConnectionFactory)))))
(deftest get-available-servers-test
  (testing "Get the addresses of available servers."
    (let [servers (cbc/get-available-servers (tf/get-client))]
      (is (or (vector? servers)
              (nil? servers))))))

(deftest get-unavailable-servers-test
  (testing "Get the addresses of unavailable servers."
    (let [servers (cbc/get-unavailable-servers (tf/get-client))]
      (is (or (vector? servers)
              (nil? servers))))))

(deftest get-node-locator-test
  (testing "Get the VBucketNodeLocator object."
    (is (instance? VBucketNodeLocator (cbc/get-node-locator (tf/get-client))))))

(deftest get-versions-test
  (testing "Get versions of all servers."
    (is (map? (cbc/get-versions (tf/get-client))))))

(deftest get-sasl-mechanisms-test
  (testing "Get the list of sasl mechanisms."
    (is (set? (cbc/get-sasl-mechanisms (tf/get-client))))))

(deftest get-client-status-test
  (testing "Get all stats of the connections."
    (is (map? (cbc/get-client-status (tf/get-client))))))

(deftest get-auth-descriptor-test
  (testing "Get the auth descriptor."
    (let [dsc (cbc/get-auth-descriptor (tf/get-client))]
      (is (or (instance? AuthDescriptor dsc)
              (nil? dsc))))))

(deftest get-failure-mode-test
  (testing "Get the FailureMode object."
    (is (instance? FailureMode (cbc/get-failure-mode (tf/get-client))))))

(deftest get-hash-alg-test
  (testing "Get the DefaultHashAlgorithm object."
    (is (instance? DefaultHashAlgorithm (cbc/get-hash-alg (tf/get-client))))))

(deftest get-max-reconnect-delay-test
  (testing "Get the max reconnect delay."
    (is (= (cbc/get-max-reconnect-delay (tf/get-client)) 30))))

;; Test failed, not working.
;(deftest get-min-reconnect-interval-test
;  (testing "Get the min reconnect interval."
;    (is (= (cbc/get-min-reconnect-interval (tf/get-client)) 1100))))

(deftest get-op-queue-max-block-time-test
  (testing "Get the op queue max block time."
    (is (= (cbc/get-op-queue-max-block-time (tf/get-client)) 10000))))

(deftest get-op-timeout-test
  (testing "Get the op timeout."
    (is (= (cbc/get-op-timeout (tf/get-client)) 2500))))

(deftest get-read-buffer-size-test
  (testing "Get the read buffer size."
    (is (= (cbc/get-read-buffer-size (tf/get-client)) 16384))))

(deftest get-timeout-exception-threshold-test
  (testing "Get the timeout exception threshold."
    (is (= (cbc/get-timeout-exception-threshold (tf/get-client)) 998))))

(deftest get-transcoder-test
  (testing "Get the transcoder."
    (is (instance? Transcoder (cbc/get-transcoder (tf/get-client))))))

(deftest daemon?-test
  (testing "Get the Boolean value of daemon option."
    (is (false? (cbc/daemon? (tf/get-client))))))

(deftest should-optimize?-test
  (testing "Get the Boolean value of should optimize option."
    (is (false? (cbc/should-optimize? (tf/get-client))))))

(deftest use-nagle-algorithm?-test
  (testing "Get the Boolean value of use nagle algorithm option."
    (is (false? (cbc/use-nagle-algorithm? (tf/get-client))))))

(deftest async-add-test
  (testing "Asynchronously adding a value."
    (cbc/delete (tf/get-client) :async-add1)
    (cbc/delete (tf/get-client) :async-add2)
    (cbc/delete (tf/get-client) :async-add3)
    (cbc/delete (tf/get-client) :async-add4)
    (let [fut1 (cbc/async-add (tf/get-client) :async-add1 1)
          fut2 (cbc/async-add (tf/get-client) "async-add2" 2 {:expiry 30})
          fut3 (cbc/async-add
                (tf/get-client)
                'async-add3 "1" {:transcoder (cbc/get-transcoder (tf/get-client))})
          fut4 (cbc/async-add
                (tf/get-client) :async-add4 "a"
                {:observe true :persist :master :replicate :zero})]
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut1))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut2))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut3))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut4))
      (is (true? @fut1))
      (is (true? @fut2))
      (is (true? @fut3))
      (is (true? @fut4))
      (is (= (cbc/get (tf/get-client) :async-add1) 1))
      (is (= (cbc/get (tf/get-client) :async-add2) 2))
      (is (= (cbc/get (tf/get-client) :async-add3) "1"))
      (is (= (cbc/get (tf/get-client) :async-add4) "a")))))

(deftest add-test
  (testing "Synchronously adding a value."
    (cbc/delete (tf/get-client) :add1)
    (cbc/delete (tf/get-client) :add2)
    (cbc/delete (tf/get-client) :add3)
    (cbc/delete (tf/get-client) :add4)
    (cbc/delete (tf/get-client) :add5)
    (cbc/add (tf/get-client) :add1 1)
    (cbc/add (tf/get-client) "add2" 2 {:expiry 30})
    (cbc/add (tf/get-client)
             :add3 "1" {:transcoder (cbc/get-transcoder (tf/get-client))})
    (cbc/async-add (tf/get-client) 'add4 "a"
                   {:observe true :persist :master :replicate :zero})
    (cbc/add (tf/get-client) :add5 {:a 1} {:timeout 1000})
    (is (= (cbc/get (tf/get-client) :add1) 1))
    (is (= (cbc/get (tf/get-client) :add2) 2))
    (is (= (cbc/get (tf/get-client) :add3) "1"))
    (is (= (cbc/get (tf/get-client) :add4) "a"))
    (is (= (cbc/get (tf/get-client) :add5) {:a 1}))))

(deftest async-add-json-test
  (testing "Asynchronously adding a Clojure data converted to a JSON string value."
    (cbc/delete (tf/get-client) :async-add-json1)
    (cbc/delete (tf/get-client) :async-add-json2)
    (cbc/delete (tf/get-client) :async-add-json3)
    (cbc/delete (tf/get-client) :async-add-json4)
    (let [fut1 (cbc/async-add-json (tf/get-client) :async-add-json1 "1")
          fut2 (cbc/async-add-json (tf/get-client) "async-add-json2" "2" {:expiry 30})
          fut3 (cbc/async-add-json
                (tf/get-client)
                'async-add-json3 {:a 1} {:transcoder (cbc/get-transcoder (tf/get-client))})
          fut4 (cbc/async-add-json
                (tf/get-client) :async-add-json4 "a"
                {:observe true :persist :master :replicate :zero})]
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut1))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut2))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut3))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut4))
      (is (true? @fut1))
      (is (true? @fut2))
      (is (true? @fut3))
      (is (true? @fut4))
      (is (= (cbc/get-json (tf/get-client) :async-add-json1) "1"))
      (is (= (cbc/get-json (tf/get-client) :async-add-json2) "2"))
      (is (= (cbc/get-json (tf/get-client) :async-add-json3) {:a 1}))
      (is (= (cbc/get-json (tf/get-client) :async-add-json4) "a")))))

(deftest add-json-test
  (testing "Synchronously adding a Clojure data converted to a JSON string value."
    (cbc/delete (tf/get-client) :add-json1)
    (cbc/delete (tf/get-client) :add-json2)
    (cbc/delete (tf/get-client) :add-json3)
    (cbc/delete (tf/get-client) :add-json4)
    (cbc/delete (tf/get-client) :add-json5)
    (cbc/add-json (tf/get-client) :add-json1 "1")
    (cbc/add-json (tf/get-client) "add-json2" "2" {:expiry 30})
    (cbc/add-json (tf/get-client)
                  'add-json3 {:a 1} {:transcoder (cbc/get-transcoder (tf/get-client))})
    (cbc/add-json (tf/get-client) :add-json4 "a"
                  {:observe true :persist :master :replicate :zero})
    (cbc/add-json (tf/get-client) :add-json5 [1 2] {:timeout 1000})
    (is (= (cbc/get-json (tf/get-client) :add-json1) "1"))
    (is (= (cbc/get-json (tf/get-client) :add-json2) "2"))
    (is (= (cbc/get-json (tf/get-client) :add-json3) {:a 1}))
    (is (= (cbc/get-json (tf/get-client) :add-json4) "a"))
    (is (= (cbc/get-json (tf/get-client) :add-json5) [1 2]))))

(deftest async-append-test
  (testing "Asynchronously append a value."
    (cbc/delete (tf/get-client) :async-append1)
    (cbc/delete (tf/get-client) :async-append2)
    (cbc/add (tf/get-client) :async-append1 "1")
    (cbc/add (tf/get-client) :async-append2 "3")
    (let [fut1 (cbc/async-append
                (tf/get-client) 
                :async-append1 ",2" (cbc/get-cas-id (tf/get-client) :async-append1))
          fut2 (cbc/async-append
                (tf/get-client) 
                "async-append2" ",4" (cbc/get-cas-id (tf/get-client) :async-append2)
                {:transcoder (cbc/get-transcoder (tf/get-client))})]
      (is (true? @fut1))
      (is (true? @fut2))
      (is (= (cbc/get (tf/get-client) :async-append1) "1,2"))
      (is (= (cbc/get (tf/get-client) :async-append2) "3,4")))))

(deftest append-test
  (testing "Synchronously append a value."
    (cbc/delete (tf/get-client) :append1)
    (cbc/delete (tf/get-client) :append2)
    (cbc/delete (tf/get-client) :append3)
    (cbc/add (tf/get-client) :append1 "1")
    (cbc/add (tf/get-client) :append2 "3")
    (cbc/add (tf/get-client) :append3 "5")
    (cbc/append (tf/get-client) 
                :append1 ",2" (cbc/get-cas-id (tf/get-client) :append1))
    (cbc/append (tf/get-client) 
                "append2" ",4" (cbc/get-cas-id (tf/get-client) :append2)
                {:transcoder (cbc/get-transcoder (tf/get-client))})
    (cbc/append (tf/get-client) 
                'append3 ",6" (cbc/get-cas-id (tf/get-client) :append3)
                {:timeout 1000})
    (is (= (cbc/get (tf/get-client) :append1) "1,2"))
    (is (= (cbc/get (tf/get-client) :append2) "3,4"))
    (is (= (cbc/get (tf/get-client) :append3) "5,6"))))

(deftest async-prepend-test
  (testing "Asynchronously prepend a value."
    (cbc/delete (tf/get-client) :async-prepend1)
    (cbc/delete (tf/get-client) :async-prepend2)
    (cbc/add (tf/get-client) :async-prepend1 "1")
    (cbc/add (tf/get-client) :async-prepend2 "3")
    (let [fut1 (cbc/async-prepend
                (tf/get-client) 
                :async-prepend1 "2," (cbc/get-cas-id (tf/get-client) :async-prepend1))
          fut2 (cbc/async-prepend
                (tf/get-client) 
                "async-prepend2" "4," (cbc/get-cas-id (tf/get-client) :async-prepend2)
                {:transcoder (cbc/get-transcoder (tf/get-client))})]
      (is (true? @fut1))
      (is (true? @fut2))
      (is (= (cbc/get (tf/get-client) :async-prepend1) "2,1"))
      (is (= (cbc/get (tf/get-client) :async-prepend2) "4,3")))))

(deftest prepend-test
  (testing "Synchronously prepend a value."
    (cbc/delete (tf/get-client) :prepend1)
    (cbc/delete (tf/get-client) :prepend2)
    (cbc/delete (tf/get-client) :prepend3)
    (cbc/add (tf/get-client) :prepend1 "1")
    (cbc/add (tf/get-client) :prepend2 "3")
    (cbc/add (tf/get-client) :prepend3 "5")
    (cbc/prepend (tf/get-client) 
                 :prepend1 "2," (cbc/get-cas-id (tf/get-client) :prepend1))
    (cbc/prepend (tf/get-client) 
                 "prepend2" "4," (cbc/get-cas-id (tf/get-client) :prepend2)
                 {:transcoder (cbc/get-transcoder (tf/get-client))})
    (cbc/prepend (tf/get-client) 
                 'prepend3 "6," (cbc/get-cas-id (tf/get-client) :prepend3)
                 {:timeout 1000})
    (is (= (cbc/get (tf/get-client) :prepend1) "2,1"))
    (is (= (cbc/get (tf/get-client) :prepend2) "4,3"))
    (is (= (cbc/get (tf/get-client) :prepend3) "6,5"))))


(deftest async-delete-test
  (testing "Asynchronously delete a value."
    (cbc/add (tf/get-client) :async-delete1 1)
    (cbc/add (tf/get-client) :async-delete2 2)
    (let [fut1 (cbc/async-delete (tf/get-client) :async-delete1)
          fut2 (cbc/async-delete (tf/get-client) "async-delete2")]
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut1))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut2))
      (is (true? @fut1))
      (is (true? @fut2))
      (is (nil? (cbc/get (tf/get-client) :async-delete1)))
      (is (nil? (cbc/get (tf/get-client) :async-delete2))))))

(deftest delete-test
  (testing "Synchronously delete a value."
    (cbc/add (tf/get-client) :delete1 1)
    (cbc/add (tf/get-client) :delete2 2)
    (cbc/delete (tf/get-client) :delete1)
    (cbc/delete (tf/get-client) "delete2" {:timeout 1000})
    (is (nil? (cbc/get (tf/get-client) :delete1)))
    (is (nil? (cbc/get (tf/get-client) :delete2)))))

(deftest async-get-test
  (testing "Asynchronously get a value."
    (cbc/delete (tf/get-client) :async-get1)
    (cbc/delete (tf/get-client) :async-get2)
    (cbc/add (tf/get-client) :async-get1 1)
    (cbc/add (tf/get-client) :async-get2 2)
    (let [fut1 (cbc/async-get (tf/get-client) :async-get1)
          fut2 (cbc/async-get
                (tf/get-client)
                "async-get2" {:transcoder (cbc/get-transcoder (tf/get-client))})]
      (is (instance? couchbase_clj.future.CouchbaseCljGetFuture fut1))
      (is (instance? couchbase_clj.future.CouchbaseCljGetFuture fut2))
      (is (= @fut1) 1)
      (is (= @fut2) 2))))

(deftest get-test
  (testing "Synchronously get a value."
    (cbc/delete (tf/get-client) :get1)
    (cbc/delete (tf/get-client) :get2)
    (cbc/add (tf/get-client) :get1 1)
    (cbc/add (tf/get-client) :get2 2)
    (is (= (cbc/get (tf/get-client) :get1) 1))
    (is (= (cbc/get (tf/get-client)
                    "get2" {:transcoder (cbc/get-transcoder (tf/get-client))}) 2))))

(deftest get-json-test
  (testing "Synchronously get a JSON string value converted to a Clojure data."
    (cbc/delete (tf/get-client) :get-json1)
    (cbc/delete (tf/get-client) :get-json2)
    (cbc/delete (tf/get-client) :get-json3)
    (cbc/add-json (tf/get-client) :get-json1 "1")
    (cbc/add-json (tf/get-client) :get-json2 {:a 1})
    (cbc/add-json (tf/get-client) :get-json3 [1 2])
    (is (= (cbc/get-json (tf/get-client) :get-json1) "1"))
    (is (= (cbc/get-json
            (tf/get-client) "get-json2" {:transcoder (cbc/get-transcoder (tf/get-client))})
           {:a 1}))
    (is (= (cbc/get-json (tf/get-client) :get-json3) [1 2]))))

(deftest async-get-touch-test
  (testing "Asynchronously get the value and update the expiry."
    (cbc/delete (tf/get-client) :async-get-touch1)
    (cbc/delete (tf/get-client) :async-get-touch2)
    (cbc/delete (tf/get-client) :async-get-touch3)
    (cbc/add (tf/get-client) :async-get-touch1 1)
    (cbc/add (tf/get-client) :async-get-touch2 2)
    (cbc/add (tf/get-client) :async-get-touch3 3)
    (let [fut1 (cbc/async-get-touch (tf/get-client) :async-get-touch1)
          fut2 (cbc/async-get-touch (tf/get-client) "async-get-touch2" {:expiry 0})
          fut3 (cbc/async-get-touch
                (tf/get-client)
                'async-get-touch3 {:transcoder (cbc/get-transcoder (tf/get-client))})]
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut1))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut2))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut3))
      (is (instance? CASValue @fut1))
      (is (instance? CASValue @fut2))
      (is (instance? CASValue @fut3))
      (is (= (cbc/get (tf/get-client) :async-get-touch1) (cbc/cas-val @fut1)))
      (is (= (cbc/get (tf/get-client) :async-get-touch2) 2))
      (is (= (cbc/get (tf/get-client) :async-get-touch3) (cbc/cas-val @fut3))))))

(deftest get-touch-test
  (testing "Synchronously get the value and update the expiry."
    (cbc/delete (tf/get-client) :get-touch1)
    (cbc/delete (tf/get-client) :get-touch2)
    (cbc/delete (tf/get-client) :get-touch3)
    (cbc/add (tf/get-client) :get-touch1 1)
    (cbc/add (tf/get-client) :get-touch2 2)
    (cbc/add (tf/get-client) :get-touch3 3)
    (let [rs1 (cbc/get-touch (tf/get-client) :get-touch1)
          rs2 (cbc/get-touch (tf/get-client) "get-touch2" {:expiry 0})
          rs3 (cbc/get-touch
               (tf/get-client)
               'get-touch3 {:transcoder (cbc/get-transcoder (tf/get-client))})]
      (is (instance? CASValue rs1))
      (is (instance? CASValue rs2))
      (is (instance? CASValue rs3))
      (is (= (cbc/get (tf/get-client) :get-touch1) (cbc/cas-val rs1)))
      (is (= (cbc/get (tf/get-client) :get-touch2) 2))
      (is (= (cbc/get (tf/get-client) :get-touch3) (cbc/cas-val rs3))))))

(deftest async-get-multi-test
  (testing "Asynchronously get value values."
    (cbc/delete (tf/get-client) :async-get-multi1)
    (cbc/delete (tf/get-client) :async-get-multi2)
    (cbc/add (tf/get-client) :async-get-multi1 1)
    (cbc/add (tf/get-client) :async-get-multi2 2)
    (let [fut1 (cbc/async-get-multi (tf/get-client) [:async-get-multi1])
          fut2 (cbc/async-get-multi
                (tf/get-client)
                ["async-get-multi1" "async-get-multi2"]
                {:transcoder (cbc/get-transcoder (tf/get-client))})]
      (is (instance? couchbase_clj.future.CouchbaseCljBulkGetFuture fut1))
      (is (instance? couchbase_clj.future.CouchbaseCljBulkGetFuture fut2))
      (is (= @fut1) {"async-get-multi1" 1})
      (is (= @fut2) {"async-get-multi1" 1 "async-get-multi2" 2}))))

(deftest get-multi-test
  (testing "Synchronously get multi values."
    (cbc/delete (tf/get-client) :get-multi1)
    (cbc/delete (tf/get-client) :get-multi2)
    (cbc/add (tf/get-client) :get-multi1 1)
    (cbc/add (tf/get-client) :get-multi2 2)
    (is (= (cbc/get-multi (tf/get-client) [:get-multi1]) {"get-multi1" 1}))
    (is (= (cbc/get-multi (tf/get-client)
                          ["get-multi1" "get-multi2"]
                          {:transcoder (cbc/get-transcoder (tf/get-client))})
           {"get-multi1" 1 "get-multi2" 2}))))

(deftest get-multi-json-test
  (testing "Synchronously get multi JSON string values that are converted to Clojure data."
    (cbc/delete (tf/get-client) :get-multi-json1)
    (cbc/delete (tf/get-client) :get-multi-json2)
    (cbc/delete (tf/get-client) :get-multi-json3)
    (cbc/add-json (tf/get-client) :get-multi-json1 "1")
    (cbc/add-json (tf/get-client) :get-multi-json2 {:a 1})
    (cbc/add-json (tf/get-client) :get-multi-json3 [1 2])
    (is (= (cbc/get-multi-json (tf/get-client) [:get-multi-json1]) {"get-multi-json1" "1"}))
    (is (= (cbc/get-multi-json (tf/get-client)
                               ["get-multi-json1" "get-multi-json2"]
                               {:transcoder (cbc/get-transcoder (tf/get-client))})
           {"get-multi-json1" "1" "get-multi-json2" {:a 1}}))
    (is (= (cbc/get-multi-json (tf/get-client)
                               ["get-multi-json3"]
                               {:transcoder (cbc/get-transcoder (tf/get-client))})
           {"get-multi-json3" [1 2]}))))

(deftest async-get-lock-test
  (testing "Asynchronously get a value and lock."
    (cbc/delete (tf/get-client) :async-get-lock1)
    (cbc/delete (tf/get-client) :async-get-lock2)
    (cbc/delete (tf/get-client) :async-get-lock3)
    (cbc/add (tf/get-client) :async-get-lock1 1)
    (cbc/add (tf/get-client) :async-get-lock2 2)
    (cbc/add (tf/get-client) :async-get-lock3 3)
    (let [fut1 (cbc/async-get-lock (tf/get-client) :async-get-lock1)
          fut2 (cbc/async-get-lock (tf/get-client) "async-get-lock2" {:expiry 30})
          fut3 (cbc/async-get-lock
                (tf/get-client)
                'async-get-lock3 {:transcoder (cbc/get-transcoder (tf/get-client))})]
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut1))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut2))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut3))
      (is (instance? CASValue @fut1))
      (is (instance? CASValue @fut2))
      (is (instance? CASValue @fut3))
      (is (cbc/locked? (tf/get-client) :async-get-lock1))
      (is (cbc/locked? (tf/get-client) :async-get-lock2))
      (is (cbc/locked? (tf/get-client) :async-get-lock3)))))

(deftest get-lock-test
  (testing "Synchronously get a value and lock."
    (cbc/delete (tf/get-client) :get-lock1)
    (cbc/delete (tf/get-client) :get-lock2)
    (cbc/delete (tf/get-client) :get-lock3)
    (cbc/add (tf/get-client) :get-lock1 1)
    (cbc/add (tf/get-client) :get-lock2 2)
    (cbc/add (tf/get-client) :get-lock3 3)
    (let [rs1 (cbc/get-lock (tf/get-client) :get-lock1)
          rs2 (cbc/get-lock (tf/get-client) "get-lock2" {:expiry 30})
          rs3 (cbc/get-lock
                (tf/get-client)
                'get-lock3 {:transcoder (cbc/get-transcoder (tf/get-client))})]
      (is (instance? CASValue rs1))
      (is (instance? CASValue rs2))
      (is (instance? CASValue rs3))
      (is (cbc/locked? (tf/get-client) :get-lock1))
      (is (cbc/locked? (tf/get-client) :get-lock2))
      (is (cbc/locked? (tf/get-client) :get-lock3)))))

(deftest locked?-test
  (testing "Whether key is locked or not."
    (cbc/add (tf/get-client) :locked 1)
    (cbc/get-lock (tf/get-client) :locked)
    (is (cbc/locked? (tf/get-client) :locked))))

(deftest async-get-cas-test
  (testing "Asynchronously get a value and CASValue object."
    (cbc/delete (tf/get-client) :async-get-cas1)
    (cbc/delete (tf/get-client) :async-get-cas2)
    (cbc/delete (tf/get-client) :async-get-cas3)
    (cbc/add (tf/get-client) :async-get-cas1 1)
    (cbc/add (tf/get-client) :async-get-cas2 2)
    (cbc/add (tf/get-client) :async-get-cas3 3)
    (let [fut1 (cbc/async-get-cas (tf/get-client) :async-get-cas1)
          fut2 (cbc/async-get-cas (tf/get-client) "async-get-cas2" {:expiry 30})
          fut3 (cbc/async-get-cas
                (tf/get-client)
                'async-get-cas3 {:transcoder (cbc/get-transcoder (tf/get-client))})]
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut1))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut2))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut3))
      (is (instance? CASValue @fut1))
      (is (instance? CASValue @fut2))
      (is (instance? CASValue @fut3)))))

(deftest get-cas-test
  (testing "Synchronously get a value and CASValue object."
    (cbc/delete (tf/get-client) :get-cas1)
    (cbc/delete (tf/get-client) :get-cas2)
    (cbc/delete (tf/get-client) :get-cas3)
    (cbc/add (tf/get-client) :get-cas1 1)
    (cbc/add (tf/get-client) :get-cas2 2)
    (cbc/add (tf/get-client) :get-cas3 3)
    (let [rs1 (cbc/get-cas (tf/get-client) :get-cas1)
          rs2 (cbc/get-cas (tf/get-client) "get-cas2" {:expiry 30})
          rs3 (cbc/get-cas
                (tf/get-client)
                'get-cas3 {:transcoder (cbc/get-transcoder (tf/get-client))})]
      (is (instance? CASValue rs1))
      (is (instance? CASValue rs2))
      (is (instance? CASValue rs3)))))

(deftest get-cas-id-test
  (testing "Synchronously get a value and CASValue object."
    (cbc/delete (tf/get-client) :get-cas-id1)
    (cbc/delete (tf/get-client) :get-cas-id2)
    (cbc/delete (tf/get-client) :get-cas-id3)
    (cbc/add (tf/get-client) :get-cas-id1 1)
    (cbc/add (tf/get-client) :get-cas-id2 2)
    (cbc/add (tf/get-client) :get-cas-id3 3)
    (let [rs1 (cbc/get-cas-id (tf/get-client) :get-cas-id1)
          rs2 (cbc/get-cas-id (tf/get-client) "get-cas-id2" {:expiry 30})
          rs3 (cbc/get-cas-id
                (tf/get-client)
                'get-cas-id3 {:transcoder (cbc/get-transcoder (tf/get-client))})]
      (is (instance? Long rs1))
      (is (instance? Long rs2))
      (is (instance? Long rs3)))))

(deftest async-inc-test
  (testing "Asynchronously increment a value."
    (cbc/delete (tf/get-client) :async-inc1)
    (cbc/delete (tf/get-client) :async-inc2)
    (cbc/add (tf/get-client) :async-inc1 "1")
    (cbc/add (tf/get-client) :async-inc2 "2")
    (let [fut1 (cbc/async-inc (tf/get-client) :async-inc1)
          fut2 (cbc/async-inc (tf/get-client) "async-inc2" {:offset 2})]
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut1))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut2))
      (is (= @fut1 2))
      (is (= @fut2 4)))))
  
(deftest inc-test
  (testing "Synchronously increment a value."
    (cbc/delete (tf/get-client) :inc1)
    (cbc/delete (tf/get-client) :inc2)
    (cbc/delete (tf/get-client) :inc3)
    (cbc/delete (tf/get-client) :inc4)
    (cbc/add (tf/get-client) :inc1 "1")
    (cbc/add (tf/get-client) :inc2 "2")
    (cbc/add (tf/get-client) :inc3 "3")
    (let [rs1 (cbc/inc (tf/get-client) :inc1)
          rs2 (cbc/inc (tf/get-client) "inc2" {:expiry 30})
          rs3 (cbc/inc (tf/get-client) 'inc3 {:offset 3})
          rs4 (cbc/inc (tf/get-client) :inc4 {:default 4})]
      (is (= rs1 2))
      (is (= rs2 3))
      (is (= rs3 6))
      (is (= rs4 4)))))

(deftest async-dec-test
  (testing "Asynchronously decrement a value."
    (cbc/delete (tf/get-client) :async-dec1)
    (cbc/delete (tf/get-client) :async-dec2)
    (cbc/add (tf/get-client) :async-dec1 "1")
    (cbc/add (tf/get-client) :async-dec2 "2")
    (let [fut1 (cbc/async-dec (tf/get-client) :async-dec1)
          fut2 (cbc/async-dec (tf/get-client) "async-dec2" {:offset 2})]
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut1))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut2))
      (is (= @fut1 0))
      (is (= @fut2 0)))))
  
(deftest dec-test
  (testing "Synchronously decrement a value."
    (cbc/delete (tf/get-client) :dec1)
    (cbc/delete (tf/get-client) :dec2)
    (cbc/delete (tf/get-client) :dec3)
    (cbc/delete (tf/get-client) :dec4)
    (cbc/add (tf/get-client) :dec1 "1")
    (cbc/add (tf/get-client) :dec2 "2")
    (cbc/add (tf/get-client) :dec3 "3")
    (let [rs1 (cbc/dec (tf/get-client) :dec1)
          rs2 (cbc/dec (tf/get-client) "dec2" {:expiry 30})
          rs3 (cbc/dec (tf/get-client) 'dec3 {:offset 3})
          rs4 (cbc/dec (tf/get-client) :dec4 {:default 4})]
      (is (= rs1 0))
      (is (= rs2 1))
      (is (= rs3 0))
      (is (= rs4 4)))))

(deftest async-replace-test
  (testing "Asynchronously replacing a value."
    (cbc/delete (tf/get-client) :async-replace1)
    (cbc/delete (tf/get-client) :async-replace2)
    (cbc/delete (tf/get-client) :async-replace3)
    (cbc/delete (tf/get-client) :async-replace4)
    (cbc/add (tf/get-client) :async-replace1 1)
    (cbc/add (tf/get-client) :async-replace2 2)
    (cbc/add (tf/get-client) :async-replace3 3)
    (cbc/add (tf/get-client) :async-replace4 4)
    (let [fut1 (cbc/async-replace (tf/get-client) :async-replace1 11)
          fut2 (cbc/async-replace (tf/get-client) "async-replace2" 22 {:expiry 30})
          fut3 (cbc/async-replace
                (tf/get-client)
                'async-replace3 "11" {:transcoder (cbc/get-transcoder (tf/get-client))})
          fut4 (cbc/async-replace
                (tf/get-client) :async-replace4 "aa"
                {:observe true :persist :master :replicate :zero})]
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut1))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut2))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut3))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut4))
      (is (true? @fut1))
      (is (true? @fut2))
      (is (true? @fut3))
      (is (true? @fut4))
      (is (= (cbc/get (tf/get-client) :async-replace1) 11))
      (is (= (cbc/get (tf/get-client) :async-replace2) 22))
      (is (= (cbc/get (tf/get-client) :async-replace3) "11"))
      (is (= (cbc/get (tf/get-client) :async-replace4) "aa")))))

(deftest replace-test
  (testing "Synchronously replacing a value."
    (cbc/delete (tf/get-client) :replace1)
    (cbc/delete (tf/get-client) :replace2)
    (cbc/delete (tf/get-client) :replace3)
    (cbc/delete (tf/get-client) :replace4)
    (cbc/delete (tf/get-client) :replace5)
    (cbc/add (tf/get-client) :replace1 1)
    (cbc/add (tf/get-client) :replace2 2)
    (cbc/add (tf/get-client) :replace3 3)
    (cbc/add (tf/get-client) :replace4 4)
    (cbc/add (tf/get-client) :replace5 5)
    (let [rs1 (cbc/replace (tf/get-client) :replace1 11)
          rs2 (cbc/replace (tf/get-client) "replace2" 22 {:expiry 30})
          rs3 (cbc/replace
             (tf/get-client)
             'replace3 "11" {:transcoder (cbc/get-transcoder (tf/get-client))})
          rs4 (cbc/replace
                (tf/get-client) :replace4 "aa"
                {:observe true :persist :master :replicate :zero})
          rs5 (cbc/replace (tf/get-client) :replace5 33 {:timeout 1000})]
      (is (true? rs1))
      (is (true? rs2))
      (is (true? rs3))
      (is (true? rs4))
      (is (true? rs5))
      (is (= (cbc/get (tf/get-client) :replace1) 11))
      (is (= (cbc/get (tf/get-client) :replace2) 22))
      (is (= (cbc/get (tf/get-client) :replace3) "11"))
      (is (= (cbc/get (tf/get-client) :replace4) "aa"))
      (is (= (cbc/get (tf/get-client) :replace5) 33)))))

(deftest async-replace-json-test
  (testing "Asynchronously replacing a JSON string value converted to Clojure data."
    (cbc/delete (tf/get-client) :async-replace-json1)
    (cbc/delete (tf/get-client) :async-replace-json2)
    (cbc/delete (tf/get-client) :async-replace-json3)
    (cbc/delete (tf/get-client) :async-replace-json4)
    (cbc/add (tf/get-client) :async-replace-json1 1)
    (cbc/add (tf/get-client) :async-replace-json2 2)
    (cbc/add (tf/get-client) :async-replace-json3 3)
    (cbc/add (tf/get-client) :async-replace-json4 4)
    (let [fut1 (cbc/async-replace-json (tf/get-client) :async-replace-json1 "11")
          fut2 (cbc/async-replace-json (tf/get-client) "async-replace-json2" "22" {:expiry 30})
          fut3 (cbc/async-replace-json
                (tf/get-client)
                'async-replace-json3 {:a 1} {:transcoder (cbc/get-transcoder (tf/get-client))})
          fut4 (cbc/async-replace-json
                (tf/get-client) :async-replace-json4 [1 2]
                {:observe true :persist :master :replicate :zero})]
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut1))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut2))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut3))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut4))
      (is (true? @fut1))
      (is (true? @fut2))
      (is (true? @fut3))
      (is (true? @fut4))
      (is (= (cbc/get-json (tf/get-client) :async-replace-json1) "11"))
      (is (= (cbc/get-json (tf/get-client) :async-replace-json2) "22"))
      (is (= (cbc/get-json (tf/get-client) :async-replace-json3) {:a 1}))
      (is (= (cbc/get-json (tf/get-client) :async-replace-json4) [1 2])))))

(deftest replace-json-test
  (testing "Synchronously replacing a JSON string value converted to Clojure data."
    (cbc/delete (tf/get-client) :replace-json1)
    (cbc/delete (tf/get-client) :replace-json2)
    (cbc/delete (tf/get-client) :replace-json3)
    (cbc/delete (tf/get-client) :replace-json4)
    (cbc/delete (tf/get-client) :replace-json5)
    (cbc/add (tf/get-client) :replace-json1 1)
    (cbc/add (tf/get-client) :replace-json2 2)
    (cbc/add (tf/get-client) :replace-json3 3)
    (cbc/add (tf/get-client) :replace-json4 4)
    (cbc/add (tf/get-client) :replace-json5 5)
    (let [rs1 (cbc/replace-json (tf/get-client) :replace-json1 "11")
          rs2 (cbc/replace-json (tf/get-client) "replace-json2" "22" {:expiry 30})
          rs3 (cbc/replace-json
                (tf/get-client)
                'replace-json3 {:a 1} {:transcoder (cbc/get-transcoder (tf/get-client))})
          rs4 (cbc/replace-json
                (tf/get-client) :replace-json4 [1 2]
                {:observe true :persist :master :replicate :zero})
          rs5 (cbc/replace-json (tf/get-client) :replace-json5 "33" {:timeout 1000})]
      (is (true? rs1))
      (is (true? rs2))
      (is (true? rs3))
      (is (true? rs4))
      (is (true? rs5))
      (is (= (cbc/get-json (tf/get-client) :replace-json1) "11"))
      (is (= (cbc/get-json (tf/get-client) :replace-json2) "22"))
      (is (= (cbc/get-json (tf/get-client) :replace-json3) {:a 1}))
      (is (= (cbc/get-json (tf/get-client) :replace-json4) [1 2]))
      (is (= (cbc/get-json (tf/get-client) :replace-json5) "33")))))

(deftest async-set-test
  (testing "Asynchronously set a value."
    (cbc/delete (tf/get-client) :async-set1)
    (cbc/delete (tf/get-client) :async-set2)
    (cbc/delete (tf/get-client) :async-set3)
    (cbc/delete (tf/get-client) :async-set4)
    (cbc/add (tf/get-client) :async-set1 1)
    (cbc/add (tf/get-client) :async-set2 2)
    (cbc/add (tf/get-client) :async-set3 3)
    (cbc/add (tf/get-client) :async-set4 4)
    (let [fut1 (cbc/async-set (tf/get-client) :async-set1 11)
          fut2 (cbc/async-set (tf/get-client) "async-set2" 22 {:expiry 30})
          fut3 (cbc/async-set
                (tf/get-client)
                'async-set3 "11" {:transcoder (cbc/get-transcoder (tf/get-client))})
          fut4 (cbc/async-set
                (tf/get-client) :async-set4 "aa"
                {:observe true :persist :master :replicate :zero})]
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut1))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut2))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut3))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut4))
      (is (true? @fut1))
      (is (true? @fut2))
      (is (true? @fut3))
      (is (true? @fut4))
      (is (= (cbc/get (tf/get-client) :async-set1) 11))
      (is (= (cbc/get (tf/get-client) :async-set2) 22))
      (is (= (cbc/get (tf/get-client) :async-set3) "11"))
      (is (= (cbc/get (tf/get-client) :async-set4) "aa")))))

(deftest set-test
  (testing "Synchronously set a value."
    (cbc/delete (tf/get-client) :set1)
    (cbc/delete (tf/get-client) :set2)
    (cbc/delete (tf/get-client) :set3)
    (cbc/delete (tf/get-client) :set4)
    (cbc/delete (tf/get-client) :set5)
    (cbc/add (tf/get-client) :set1 1)
    (cbc/add (tf/get-client) :set2 2)
    (cbc/add (tf/get-client) :set3 3)
    (cbc/add (tf/get-client) :set4 4)
    (cbc/add (tf/get-client) :set5 5)
    (let [rs1 (cbc/set (tf/get-client) :set1 11)
          rs2 (cbc/set (tf/get-client) "set2" 22 {:expiry 30})
          rs3 (cbc/set
             (tf/get-client)
             'set3 "11" {:transcoder (cbc/get-transcoder (tf/get-client))})
          rs4 (cbc/set
                (tf/get-client) :set4 "aa"
                {:observe true :persist :master :replicate :zero})
          rs5 (cbc/set (tf/get-client) :set5 33 {:timeout 1000})]
      (is (true? rs1))
      (is (true? rs2))
      (is (true? rs3))
      (is (true? rs4))
      (is (true? rs5))
      (is (= (cbc/get (tf/get-client) :set1) 11))
      (is (= (cbc/get (tf/get-client) :set2) 22))
      (is (= (cbc/get (tf/get-client) :set3) "11"))
      (is (= (cbc/get (tf/get-client) :set4) "aa"))
      (is (= (cbc/get (tf/get-client) :set5) 33)))))

(deftest async-set-json-test
  (testing "Asynchronously set a JSON string value converted to Clojure data."
    (cbc/delete (tf/get-client) :async-set-json1)
    (cbc/delete (tf/get-client) :async-set-json2)
    (cbc/delete (tf/get-client) :async-set-json3)
    (cbc/delete (tf/get-client) :async-set-json4)
    (cbc/add (tf/get-client) :async-set-json1 1)
    (cbc/add (tf/get-client) :async-set-json2 2)
    (cbc/add (tf/get-client) :async-set-json3 3)
    (cbc/add (tf/get-client) :async-set-json4 4)
    (let [fut1 (cbc/async-set-json (tf/get-client) :async-set-json1 "11")
          fut2 (cbc/async-set-json (tf/get-client) "async-set-json2" "22" {:expiry 30})
          fut3 (cbc/async-set-json
                (tf/get-client)
                'async-set-json3 {:a 1} {:transcoder (cbc/get-transcoder (tf/get-client))})
          fut4 (cbc/async-set-json
                (tf/get-client) :async-set-json4 [1 2]
                {:observe true :persist :master :replicate :zero})]
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut1))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut2))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut3))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut4))
      (is (true? @fut1))
      (is (true? @fut2))
      (is (true? @fut3))
      (is (true? @fut4))
      (is (= (cbc/get-json (tf/get-client) :async-set-json1) "11"))
      (is (= (cbc/get-json (tf/get-client) :async-set-json2) "22"))
      (is (= (cbc/get-json (tf/get-client) :async-set-json3) {:a 1}))
      (is (= (cbc/get-json (tf/get-client) :async-set-json4) [1 2])))))

(deftest set-json-test
  (testing "Synchronously set a JSON string value converted to Clojure data."
    (cbc/delete (tf/get-client) :set-json1)
    (cbc/delete (tf/get-client) :set-json2)
    (cbc/delete (tf/get-client) :set-json3)
    (cbc/delete (tf/get-client) :set-json4)
    (cbc/delete (tf/get-client) :set-json5)
    (cbc/add (tf/get-client) :set-json1 1)
    (cbc/add (tf/get-client) :set-json2 2)
    (cbc/add (tf/get-client) :set-json3 3)
    (cbc/add (tf/get-client) :set-json4 4)
    (cbc/add (tf/get-client) :set-json5 5)
    (let [rs1 (cbc/set-json (tf/get-client) :set-json1 "11")
          rs2 (cbc/set-json (tf/get-client) "set-json2" "22" {:expiry 30})
          rs3 (cbc/set-json
                (tf/get-client)
                'set-json3 {:a 1} {:transcoder (cbc/get-transcoder (tf/get-client))})
          rs4 (cbc/set-json
                (tf/get-client) :set-json4 [1 2]
                {:observe true :persist :master :replicate :zero})
          rs5 (cbc/set-json (tf/get-client) :set-json5 "33" {:timeout 1000})]
      (is (true? rs1))
      (is (true? rs2))
      (is (true? rs3))
      (is (true? rs4))
      (is (true? rs5))
      (is (= (cbc/get-json (tf/get-client) :set-json1) "11"))
      (is (= (cbc/get-json (tf/get-client) :set-json2) "22"))
      (is (= (cbc/get-json (tf/get-client) :set-json3) {:a 1}))
      (is (= (cbc/get-json (tf/get-client) :set-json4) [1 2]))
      (is (= (cbc/get-json (tf/get-client) :set-json5) "33")))))

(deftest async-set-cas-test
  (testing "Asynchronously compare and set a value."
    (cbc/delete (tf/get-client) :async-set-cas1)
    (cbc/delete (tf/get-client) :async-set-cas2)
    (cbc/delete (tf/get-client) :async-set-cas3)
    (cbc/add (tf/get-client) :async-set-cas1 1)
    (cbc/add (tf/get-client) :async-set-cas2 2)
    (cbc/add (tf/get-client) :async-set-cas3 3)
    (let [fut1 (cbc/async-set-cas (tf/get-client) :async-set-cas1 11
                                  (cbc/get-cas-id (tf/get-client) :async-set-cas1))
          fut2 (cbc/async-set-cas (tf/get-client)
                                  "async-set-cas2" 22
                                  (cbc/get-cas-id (tf/get-client) "async-set-cas2")
                                  {:expiry 30})
          fut3 (cbc/async-set-cas
                (tf/get-client)
                'async-set-cas3 "11"
                (cbc/get-cas-id (tf/get-client) 'async-set-cas3)
                {:transcoder (cbc/get-transcoder (tf/get-client))})]
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut1))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut2))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut3))
      (is (= (@fut1 :ok)))
      (is (= (@fut2 :ok)))
      (is (= (@fut3 :ok)))
      (is (= (cbc/get (tf/get-client) :async-set-cas1) 11))
      (is (= (cbc/get (tf/get-client) :async-set-cas2) 22))
      (is (= (cbc/get (tf/get-client) :async-set-cas3) "11")))))

(deftest set-cas-test
  (testing "Synchronously compare and set a value."
    (cbc/delete (tf/get-client) :set-cas1)
    (cbc/delete (tf/get-client) :set-cas2)
    (cbc/delete (tf/get-client) :set-cas3)
    (cbc/add (tf/get-client) :set-cas1 1)
    (cbc/add (tf/get-client) :set-cas2 2)
    (cbc/add (tf/get-client) :set-cas3 3)
    (let [rs1 (cbc/set-cas (tf/get-client)
                           :set-cas1 11
                           (cbc/get-cas-id (tf/get-client) :set-cas1))
          rs2 (cbc/set-cas (tf/get-client)
                           "set-cas2" 22
                           (cbc/get-cas-id (tf/get-client) "set-cas2")
                           {:expiry 30})
          rs3 (cbc/set-cas
             (tf/get-client)
             'set-cas3 "11"
             (cbc/get-cas-id (tf/get-client) 'set-cas3)
             {:transcoder (cbc/get-transcoder (tf/get-client))})]
      (is (= rs1 :ok))
      (is (= rs2 :ok))
      (is (= rs3 :ok))
      (is (= (cbc/get (tf/get-client) :set-cas1) 11))
      (is (= (cbc/get (tf/get-client) :set-cas2) 22))
      (is (= (cbc/get (tf/get-client) :set-cas3) "11")))))

(deftest async-set-cas-json-test
  (testing "Asynchronously compare and set a JSON string value converted to Clojure data."
    (cbc/delete (tf/get-client) :async-set-cas-json1)
    (cbc/delete (tf/get-client) :async-set-cas-json2)
    (cbc/delete (tf/get-client) :async-set-cas-json3)
    (cbc/add (tf/get-client) :async-set-cas-json1 1)
    (cbc/add (tf/get-client) :async-set-cas-json2 2)
    (cbc/add (tf/get-client) :async-set-cas-json3 3)
    (let [fut1 (cbc/async-set-cas-json (tf/get-client)
                                       :async-set-cas-json1 "11"
                                       (cbc/get-cas-id (tf/get-client) :async-set-cas-json1))
          fut2 (cbc/async-set-cas-json (tf/get-client)
                                       "async-set-cas-json2" "22"
                                       (cbc/get-cas-id (tf/get-client) "async-set-cas-json2")
                                       {:expiry 30})
          fut3 (cbc/async-set-cas-json
                (tf/get-client)
                'async-set-cas-json3 {:a 1}
                (cbc/get-cas-id (tf/get-client) 'async-set-cas-json3)
                {:transcoder (cbc/get-transcoder (tf/get-client))})]
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut1))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut2))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut3))
      (is (= @fut1 :ok))
      (is (= @fut2 :ok))
      (is (= @fut3 :ok))
      (is (= (cbc/get-json (tf/get-client) :async-set-cas-json1) "11"))
      (is (= (cbc/get-json (tf/get-client) :async-set-cas-json2) "22"))
      (is (= (cbc/get-json (tf/get-client) :async-set-cas-json3) {:a 1})))))

(deftest set-cas-json-test
  (testing "Synchronously compare and set a JSON string value converted to Clojure data."
    (cbc/delete (tf/get-client) :set-cas-json1)
    (cbc/delete (tf/get-client) :set-cas-json2)
    (cbc/delete (tf/get-client) :set-cas-json3)
    (cbc/add (tf/get-client) :set-cas-json1 1)
    (cbc/add (tf/get-client) :set-cas-json2 2)
    (cbc/add (tf/get-client) :set-cas-json3 3)
    (let [rs1 (cbc/set-cas-json (tf/get-client)
                                :set-cas-json1 "11"
                                (cbc/get-cas-id (tf/get-client) :set-cas-json1))
          rs2 (cbc/set-cas-json (tf/get-client)
                                "set-cas-json2" "22"
                                (cbc/get-cas-id (tf/get-client) "set-cas-json2")
                                {:expiry 30})
          rs3 (cbc/set-cas-json
                (tf/get-client)
                'set-cas-json3 {:a 1}
                (cbc/get-cas-id (tf/get-client) 'set-cas-json3)
                {:transcoder (cbc/get-transcoder (tf/get-client))})]
      (is (= rs1 :ok))
      (is (= rs2 :ok))
      (is (= rs3 :ok))
      (is (= (cbc/get-json (tf/get-client) :set-cas-json1) "11"))
      (is (= (cbc/get-json (tf/get-client) :set-cas-json2) "22"))
      (is (= (cbc/get-json (tf/get-client) :set-cas-json3) {:a 1})))))

(deftest async-touch-test
  (testing "Asynchronously update the expiry."
    (cbc/delete (tf/get-client) :async-touch1)
    (cbc/delete (tf/get-client) :async-touch2)
    (cbc/add (tf/get-client) :async-touch1 1)
    (cbc/add (tf/get-client) :async-touch2 2)
    (let [fut1 (cbc/async-touch (tf/get-client) :async-touch1)
          fut2 (cbc/async-touch (tf/get-client) "async-touch2" {:expiry 0})]
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut1))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut2))
      (is (true? @fut1))
      (is (true? @fut2)))))

(deftest touch-test
  (testing "Synchronously update the expiry."
    (cbc/delete (tf/get-client) :async-touch1)
    (cbc/delete (tf/get-client) :async-touch2)
    (cbc/delete (tf/get-client) :async-touch3)
    (cbc/add (tf/get-client) :async-touch1 1)
    (cbc/add (tf/get-client) :async-touch2 2)
    (cbc/add (tf/get-client) :async-touch3 3)
    (let [rs1 (cbc/touch (tf/get-client) :async-touch1)
          rs2 (cbc/touch (tf/get-client) "async-touch2" {:expiry 0})
          rs3 (cbc/touch (tf/get-client) 'async-touch1 {:timeout 1000})]
      (is (true? rs1))
      (is (true? rs2))
      (is (true? rs3)))))

(deftest async-unlock-test
  (testing "Asynchronously unlock the key."
    (cbc/delete (tf/get-client) :async-unlock)
    (cbc/delete (tf/get-client) :async-unlock)
    (cbc/add (tf/get-client) :async-unlock1 1)
    (cbc/add (tf/get-client) :async-unlock2 2)
    (let [fut1 (cbc/async-unlock (tf/get-client)
                                 :async-unlock1
                                 (cbc/cas-id (cbc/get-lock (tf/get-client) :async-unlock1)))
          fut2 (cbc/async-unlock
                (tf/get-client)
                "async-unlock2"
                (cbc/cas-id (cbc/get-lock (tf/get-client) "async-unlock2"))
                {:transcoder (cbc/get-transcoder (tf/get-client))})]
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut1))
      (is (instance? couchbase_clj.future.CouchbaseCljOperationFuture fut2))
      (is (true? @fut1))
      (is (true? @fut2)))))

(deftest unlock-test
  (testing "Synchronously unlock the key."
    (cbc/delete (tf/get-client) :unlock)
    (cbc/delete (tf/get-client) :unlock)
    (cbc/add (tf/get-client) :unlock1 1)
    (cbc/add (tf/get-client) :unlock2 2)
    (let [rs1 (cbc/unlock (tf/get-client)
                           :unlock1
                           (cbc/cas-id (cbc/get-lock (tf/get-client) :unlock1)))
          rs2 (cbc/unlock
                (tf/get-client)
                "unlock2"
                (cbc/cas-id (cbc/get-lock (tf/get-client) "unlock2"))
                {:transcoder (cbc/get-transcoder (tf/get-client))})]
      (is (true? rs1))
      (is (true? rs2)))))

(deftest async-get-view-test
  (testing "Asynchronously get a view."
    (let [fut (cbc/async-get-view (tf/get-client) tf/design-doc tf/view)]
      (is (instance? couchbase_clj.future.CouchbaseCljHttpFuture fut))
      (is (instance? View @fut)))))

(deftest get-view-test
  (testing "Synchronously get a view."
    (let [view (cbc/get-view (tf/get-client) tf/design-doc tf/view)]
      (is (instance? View view)))))


;; TODO: Currently not supported due to API change in the Couchbase Client.
;(deftest async-get-views-test
;  (testing "Asynchronously get a sequence of views."
;    (let [fut (cbc/async-get-views (tf/get-client) tf/design-doc)]
;      (is (instance? couchbase_clj.future.CouchbaseCljHttpFuture fut))
;      (is (seq? @fut)))))
;
;(deftest get-views-test
;  (testing "Synchronously get a sequence of views."
;    (let [views (cbc/get-views (tf/get-client) tf/design-doc)]
;      (is (seq? views)))))

(deftest async-query-test
  (testing "Asynchronously query a view."
    (let [q (cbq/create-query {:limit 100})
          view (cbc/get-view (tf/get-client) tf/design-doc tf/view)
          fut1 (cbc/async-query (tf/get-client) view q)
          fut2 (cbc/async-query (tf/get-client) tf/design-doc tf/view q)
          fut3 (cbc/async-query (tf/get-client) tf/design-doc tf/view {:limit 100})]
      (is (instance? couchbase_clj.future.CouchbaseCljHttpFuture fut1))
      (is (instance? couchbase_clj.future.CouchbaseCljHttpFuture fut2))
      (is (instance? couchbase_clj.future.CouchbaseCljHttpFuture fut3))
      (is (seq? @fut1))
      (is (seq? @fut2))
      (is (seq? @fut3)))))

(deftest query-test
  (testing "Synchronously query a view."
    (let [q (cbq/create-query {:limit 100})
          view (cbc/get-view (tf/get-client) tf/design-doc tf/view)
          rs1 (cbc/query (tf/get-client) view q)
          rs2 (cbc/query (tf/get-client) tf/design-doc tf/view q)
          rs3 (cbc/query (tf/get-client) tf/design-doc tf/view {:limit 100})]
      (is (seq? rs1))
      (is (seq? rs2))
      (is (seq? rs3)))))

(deftest lazy-query-test
  (testing "Lazily query a view."
    (let [q (cbq/create-query {})
          view (cbc/get-view (tf/get-client) tf/design-doc tf/view)
          rs1 (cbc/lazy-query (tf/get-client) view q 5)
          rs2 (cbc/lazy-query (tf/get-client) tf/design-doc tf/view q 5)
          rs3 (cbc/lazy-query (tf/get-client) tf/design-doc tf/view {} 5)]
      (is (seq? rs1))
      (is (seq? rs2))
      (is (seq? rs3)))))

(deftest wait-queue-test
  (testing "Synchronously wait for queues."
    (is (true? (cbc/wait-queue (tf/get-client))))
    (is (true? (cbc/wait-queue (tf/get-client) 2000)))))

;; TODO: Currently not working
;(deftest flush-test
;  (testing "Flushing of all cache and persistent data."
;    (is (true? (cbc/flush (tf/get-client))))
;    (is (true? (cbc/flush (tf/get-client) 1000)))))

(deftest shutdown-test
  (testing "Shutdown of Couchbase client."
    (let [c (cbc/create-client {:bucket tf/bucket
                                :username tf/bucket-username
                                :password tf/bucket-password
                                :uris tf/uris
                                :op-timeout 20000})]
      (is (true? (cbc/shutdown c)))
      (is (false? (cbc/shutdown c))))))

(deftest view-id-test
  (testing "Get the ID of query result."
    (let [rs (cbc/query (tf/get-client) tf/design-doc tf/view {})
          row (first rs)]
      (is (instance? ViewRow row))
      (is (not (nil? (cbc/view-id row)))))))

(deftest view-key-test
  (testing "Get the key of query result."
    (let [rs (cbc/query (tf/get-client) tf/design-doc tf/view {})
          row (first rs)]
      (is (instance? ViewRow row))
      (is (not (nil? (cbc/view-key row)))))))

(deftest view-key-json-est
  (testing "Get the JSON string key of query result"
    (let [rs (cbc/query (tf/get-client) tf/design-doc tf/view2 {})
          row (first rs)]
      (is (instance? ViewRow row))
      (is (not (nil? (cbc/view-val row)))))))

(deftest view-val-test
  (testing "Get the value of query result."
    (let [rs (cbc/query (tf/get-client) tf/design-doc tf/view {})
          row (first rs)]
      (is (instance? ViewRow row))
      (is (not (nil? (cbc/view-val row)))))))

(deftest view-val-json-test
  (testing "Get the JSON string value of query result converted to Clojure data."
    (let [rs (cbc/query (tf/get-client) tf/design-doc tf/view2 {})
          row (first rs)]
      (is (instance? ViewRow row))
      (is (not (nil? (cbc/view-val-json row)))))))

(deftest view-doc-test
  (testing "Get the document of query result."
    (let [rs (cbc/query (tf/get-client) tf/design-doc tf/view {:include-docs true})
          row (first rs)]
      (is (instance? ViewRow row))
      (is (not (nil? (cbc/view-doc row)))))))

(deftest view-doc-json-test
  (testing "Get the JSON string document of query result converted to Clojure data"
    (let [rs (cbc/query (tf/get-client) tf/design-doc tf/view2 {:include-docs true})
          row (first rs)]
      (is (instance? ViewRow row))
      (is (not (nil? (cbc/view-doc-json row)))))))

(deftest create-client-test
  (testing "Creation of couchbase client."
    (let [c (cbc/create-client {:bucket tf/bucket
                                :username tf/bucket-username
                                :password tf/bucket-password
                                :uris tf/uris
                                :auth-descriptor (AuthDescriptor. (into-array [ "plain" ])
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
      (cbc/add c :create-client1 1)
      (is (= (cbc/get c :create-client1) 1))
      (is (instance? AuthDescriptor (cbc/get-auth-descriptor c)))
      (is (= (cbc/daemon? c) true))
      (is (= (cbc/get-failure-mode c) FailureMode/Retry))
      (is (= (cbc/get-hash-alg c) DefaultHashAlgorithm/CRC_HASH))
      (is (= (cbc/get-max-reconnect-delay c) 500))
      ;(is (= (cbc/get-min-reconnect-interval c) 2000))
      (is (= (cbc/get-op-queue-max-block-time c) 20000))
      (is (= (cbc/get-op-timeout c) 20000))
      (is (= (cbc/get-read-buffer-size c) 17000))
      (is (false? (cbc/should-optimize? c)))
      (is (= (cbc/get-timeout-exception-threshold c) 33331))
      (is (instance? LongTranscoder (cbc/get-transcoder c)))
      (is (= (cbc/use-nagle-algorithm? c) true)))))

(deftest defclient-test
  (testing "Creation of couchbase client as a Var."
    (cbc/defclient c {:bucket tf/bucket
                      :username tf/bucket-username
                      :password tf/bucket-password
                      :uris tf/uris})
    (cbc/add c :defclient 1)
    (is (= (cbc/get c :defclient) 1))))
