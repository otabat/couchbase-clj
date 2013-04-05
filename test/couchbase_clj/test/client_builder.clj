(ns couchbase-clj.test.client-builder
  (:import [java.net URI]
           [net.spy.memcached FailureMode]
           [net.spy.memcached DefaultHashAlgorithm]
           [net.spy.memcached.auth AuthDescriptor]
           [net.spy.memcached.auth PlainCallbackHandler]
           [net.spy.memcached.transcoders LongTranscoder]
           [net.spy.memcached.transcoders SerializingTranscoder]
           [com.couchbase.client CouchbaseClient]
           [com.couchbase.client CouchbaseConnectionFactory]
           [com.couchbase.client CouchbaseConnectionFactoryBuilder]
           [couchbase_clj.client_builder.CouchbaseCljClientBuilder])
  (:require [couchbase-clj.client :as cbc]
            [couchbase-clj.client-builder :as cbb]
            [couchbase-clj.test.fixture :as tf])
  (:use [clojure.test]))

;(use-fixtures :once tf/setup-client tf/flush-data)
(use-fixtures :once tf/setup-client)

(deftest str->uri-test
  (testing "Convert string to URI object."
    (is (instance? URI (cbb/str->uri "http://localhost/")))
    (is (instance? URI (URI. "http://127.0.0.1:8091/pools")))))

(deftest get-factory-builder-test
  (testing "Get CouchbaseConnectionFactoryBuilder object."
    (let [cb (cbb/create-client-builder {:hash-alg :native-hash
                                         :failure-mode :redistribute})]
      (is (instance? CouchbaseConnectionFactoryBuilder (cbb/get-factory-builder cb))))))

(deftest set-auth-descriptor-test
  (testing "Set auth-descriptor option to a client."
    (let [cf (cbb/build {:bucket tf/bucket
                         :password tf/bucket-password
                         :uris (map #(URI. %) tf/uris)
                         :hash-alg :native-hash
                         :failure-mode :redistribute
                         :auth-descriptor (AuthDescriptor. (into-array [ "plain" ])
                                                           (PlainCallbackHandler. "" ""))})]
      (is (instance? AuthDescriptor (.getAuthDescriptor cf))))))

(deftest set-daemon-test
  (testing "Set daemon option to a client."
    (let [cf (cbb/build {:bucket tf/bucket
                         :password tf/bucket-password
                         :uris (map #(URI. %) tf/uris)
                         :hash-alg :native-hash
                         :failure-mode :redistribute
                         :daemon true})]
      (is (= (.isDaemon cf) true)))))

(deftest set-failure-mode-test
  (testing "Set failure-mode option to a client."
    (let [m {:bucket tf/bucket
             :password tf/bucket-password
             :uris (map #(URI. %) tf/uris)
             :hash-alg :native-hash}
          cf1 (cbb/build (assoc m :failure-mode :redistribute))
          cf2 (cbb/build (assoc m :failure-mode :retry))
          cf3 (cbb/build (assoc m :failure-mode :cancel))
          cf4 (cbb/build (assoc m :failure-mode :else))]
      (is (= (.getFailureMode cf1) FailureMode/Redistribute))
      (is (= (.getFailureMode cf2) FailureMode/Retry))
      (is (= (.getFailureMode cf3) FailureMode/Cancel))
      (is (= (.getFailureMode cf4) FailureMode/Redistribute)))))

(deftest set-hash-alg-test
  (testing "Set hash-alg option to a client."
    (let [m {:bucket tf/bucket
             :password tf/bucket-password
             :uris (map #(URI. %) tf/uris)
             :failure-mode :redistribute}
          cf1 (cbb/build (assoc m :hash-alg :native-hash))
          cf2 (cbb/build (assoc m :hash-alg :ketama-hash))
          cf3 (cbb/build (assoc m :hash-alg :crc-hash))
          cf4 (cbb/build (assoc m :hash-alg :fnv1-64-hash))
          cf5 (cbb/build (assoc m :hash-alg :fnv1a-64-hash))
          cf6 (cbb/build (assoc m :hash-alg :fnv1-32-hash))
          cf7 (cbb/build (assoc m :hash-alg :fnv1a-32-hash))
          cf8 (cbb/build (assoc m :hash-alg :else))]
      (is (= (.getHashAlg cf1) DefaultHashAlgorithm/NATIVE_HASH))
      (is (= (.getHashAlg cf2) DefaultHashAlgorithm/KETAMA_HASH))
      (is (= (.getHashAlg cf3) DefaultHashAlgorithm/CRC_HASH))
      (is (= (.getHashAlg cf4) DefaultHashAlgorithm/FNV1_64_HASH))
      (is (= (.getHashAlg cf5) DefaultHashAlgorithm/FNV1A_64_HASH))
      (is (= (.getHashAlg cf6) DefaultHashAlgorithm/FNV1_32_HASH))
      (is (= (.getHashAlg cf7) DefaultHashAlgorithm/FNV1A_32_HASH))
      (is (= (.getHashAlg cf8) DefaultHashAlgorithm/NATIVE_HASH)))))

(deftest set-max-reconnect-delay-test
  (testing "Set max-reconnect-delay option to a client."
    (let [cf (cbb/build {:bucket tf/bucket
                         :password tf/bucket-password
                         :uris (map #(URI. %) tf/uris)
                         :hash-alg :native-hash
                         :failure-mode :redistribute
                         :max-reconnect-delay 500})]
      (is (= (.getMaxReconnectDelay cf) 500)))))

;; TODO: Test failed, not working.
;(deftest set-min-reconnect-interval-test
;  (testing "Set min-reconnect-interval option to a client."
;    (let [cf (cbb/build {:bucket tf/bucket
;                         :password tf/bucket-password
;                         :uris (map #(URI. %) tf/uris)
;                         :hash-alg :native-hash
;                         :failure-mode :redistribute
;                         :min-reconnect-interval 2000})]
;      (is (= (.getMinReconnectInterval cf) 2000)))))

; APIs not provided?
;(deftest set-obs-poll-interval-test)
;(deftest set-obs-poll-max-test)

(deftest set-op-queue-max-block-time-test
  (testing "Set op-queue-max-block-time option to a client."
    (let [cf (cbb/build {:bucket tf/bucket
                         :password tf/bucket-password
                         :uris (map #(URI. %) tf/uris)
                         :hash-alg :native-hash
                         :failure-mode :redistribute
                         :op-queue-max-block-time 15000})]
      (is (= (.getOpQueueMaxBlockTime cf) 15000)))))

(deftest set-op-timeout-test
  (testing "Set op-timeout option to a client."
    (let [cf (cbb/build {:bucket tf/bucket
                         :password tf/bucket-password
                         :uris (map #(URI. %) tf/uris)
                         :hash-alg :native-hash
                         :failure-mode :redistribute
                         :op-timeout 10000})]
      (is (= (.getOperationTimeout cf) 10000)))))

(deftest set-read-buffer-size-test
  (testing "Set read-buffer-size option to a client."
    (let [cf (cbb/build {:bucket tf/bucket
                         :password tf/bucket-password
                         :uris (map #(URI. %) tf/uris)
                         :hash-alg :native-hash
                         :failure-mode :redistribute
                         :read-buffer-size 10000})]
      (is (= (.getReadBufSize cf) 10000)))))

(deftest set-should-optimize-test
  (testing "Set should-optimize option to a client."
    (let [cf (cbb/build {:bucket tf/bucket
                         :password tf/bucket-password
                         :uris (map #(URI. %) tf/uris)
                         :hash-alg :native-hash
                         :failure-mode :redistribute
                         :should-optimize false})]
      (is (= (.shouldOptimize cf) false)))))

(deftest set-timeout-exception-threshold-test
  (testing "Set timeout-exception-threshold option to a client builder."
    (let [cf (cbb/build {:bucket tf/bucket
                         :password tf/bucket-password
                         :uris (map #(URI. %) tf/uris)
                         :hash-alg :native-hash
                         :failure-mode :redistribute
                         :timeout-exception-threshold 10000})]
      (is (= (.getTimeoutExceptionThreshold cf) 9998)))))

(deftest set-transcoder-test
  (testing "Set use-nagle-algorithm option to a client builder."
    (let [cl (cbc/create-client {:bucket tf/bucket
                                 :password tf/bucket-password
                                 :uris (map #(URI. %) tf/uris)
                                 :hash-alg :native-hash
                                 :failure-mode :redistribute
                                 :transcoder (SerializingTranscoder.)})]
      (is (instance? SerializingTranscoder (.getTranscoder (cbc/get-client cl)))))))

(deftest set-use-nagle-algorithm-test
  (testing "Set use-nagle-algorithm option to a client builder."
    (let [cf (cbb/build {:bucket tf/bucket
                         :password tf/bucket-password
                         :uris (map #(URI. %) tf/uris)
                         :hash-alg :native-hash
                         :failure-mode :redistribute
                         :use-nagle-algorithm true})]
      (is (= (.useNagleAlgorithm cf) true)))))

(deftest create-client-builder-test
  (testing "Creation of CouchbaseCljClientBuilder."
    (let [cb (cbb/create-client-builder {:hash-alg :native-hash
                                         :failure-mode :redistribute})]
      (is (instance? couchbase_clj.client_builder.CouchbaseCljClientBuilder cb)))))

(deftest create-factory-test
  (testing "Creation of CouchbaseConnectionFactory"
    (let [fb (cbb/get-factory-builder (cbb/create-client-builder
                                       {:hash-alg :native-hash
                                        :failure-mode :redistribute}))
          cf (cbb/create-factory{:bucket tf/bucket
                                 :username tf/bucket-username
                                 :password tf/bucket-password
                                 :uris (map #(URI. %) tf/uris)
                                 :factory-builder fb})]
      (is (instance? CouchbaseConnectionFactory cf)))))

(deftest build-test
  (testing "Building of CouchbaseConnectionFactory with default value insertion."
    (let [cf (cbb/build {:bucket tf/bucket
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
                         :should-optimize true
                         :timeout-exception-threshold 33333
                         :transcoder (SerializingTranscoder.)
                         :use-nagle-algorithm true})] 
      (is (instance? CouchbaseConnectionFactory cf)))))
