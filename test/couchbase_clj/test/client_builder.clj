(ns couchbase-clj.test.client-builder
  (:import [java.net URI]
           [java.util ArrayList]
           [net.spy.memcached FailureMode DefaultHashAlgorithm]
           [net.spy.memcached.auth AuthDescriptor PlainCallbackHandler]
           [net.spy.memcached.transcoders LongTranscoder SerializingTranscoder]
           [com.couchbase.client
            CouchbaseClient
            CouchbaseConnectionFactory
            CouchbaseConnectionFactoryBuilder]
           [couchbase_clj.client_builder CouchbaseCljClientBuilder])
  (:use [clojure.test])
  (:require [couchbase-clj.client :as cb-client]
            [couchbase-clj.client-builder :as cb-client-builder]
            [couchbase-clj.test.fixture :as tf]))

;(use-fixtures :once tf/setup-client tf/flush-data)
(use-fixtures :once tf/setup-client)

(def base-opts {:bucket tf/bucket
                :username tf/bucket-username
                :password tf/bucket-password
                :uris (map #(URI. %) tf/uris)})

(def base-opts-with-hash-alg-and-failure-mode
  (merge base-opts {:hash-alg :native-hash
                    :failure-mode :redistribute}))

(def base-opts-with-hash-alg-failure-mode-array-list-uris
  (merge base-opts-with-hash-alg-and-failure-mode
         {:uris (ArrayList. (:uris base-opts-with-hash-alg-and-failure-mode))}))

(deftest str->uri-test
  (testing "Convert string to URI object."
    (is (instance? URI (cb-client-builder/str->uri "http://localhost/")))
    (is (instance? URI (URI. "http://127.0.0.1:8091/pools")))))

(deftest get-factory-builder-test
  (testing "Get CouchbaseConnectionFactoryBuilder object."
    (let [cb (cb-client-builder/create-client-builder
              {:hash-alg :native-hash
               :failure-mode :redistribute})]
      (is (instance? CouchbaseConnectionFactoryBuilder
                     (cb-client-builder/get-factory-builder cb))))))

(deftest set-auth-descriptor!-test
  (testing "Set auth-descriptor option to a client."
    (let [auth-descriptor (AuthDescriptor. (into-array [ "plain" ])
                                           (PlainCallbackHandler. "" ""))
          cb (cb-client-builder/create-client-builder {})
          cf (cb-client-builder/build
              (merge base-opts-with-hash-alg-and-failure-mode
                     {:auth-descriptor auth-descriptor}))]
      (cb-client-builder/set-auth-descriptor! cb auth-descriptor)
      (is (instance? CouchbaseCljClientBuilder cb))
      (is (instance? AuthDescriptor (.getAuthDescriptor cf))))))

(deftest set-daemon!-test
  (testing "Set daemon option to a client."
    (let [cb (cb-client-builder/create-client-builder {})
          cf (cb-client-builder/build
              (merge base-opts-with-hash-alg-and-failure-mode {:daemon true}))]
      (cb-client-builder/set-daemon! cb true)
      (is (instance? CouchbaseCljClientBuilder cb))
      (is (= (.isDaemon cf) true)))))

(deftest set-failure-mode!-test
  (testing "Set failure-mode option to a client."
    (let [cb (cb-client-builder/create-client-builder {})
          cf1 (cb-client-builder/build
               (merge base-opts-with-hash-alg-and-failure-mode
                      {:failure-mode :redistribute}))
          cf2 (cb-client-builder/build
               (merge base-opts-with-hash-alg-and-failure-mode
                      {:failure-mode :retry}))
          cf3 (cb-client-builder/build
               (merge base-opts-with-hash-alg-and-failure-mode
                      {:failure-mode :cancel}))
          cf4 (cb-client-builder/build
               (merge base-opts-with-hash-alg-and-failure-mode
                      {:failure-mode :else}))]
      (cb-client-builder/set-failure-mode! cb :redistribute)
      (is (instance? CouchbaseCljClientBuilder cb))
      (is (= (.getFailureMode cf1) FailureMode/Redistribute))
      (is (= (.getFailureMode cf2) FailureMode/Retry))
      (is (= (.getFailureMode cf3) FailureMode/Cancel))
      (is (= (.getFailureMode cf4) FailureMode/Redistribute)))))

(deftest set-hash-alg!-test
  (testing "Set hash-alg option to a client."
    (let [m (merge base-opts {:failure-mode :redistribute})
          cb (cb-client-builder/create-client-builder {})
          cf1 (cb-client-builder/build (assoc m :hash-alg :native-hash))
          cf2 (cb-client-builder/build (assoc m :hash-alg :ketama-hash))
          cf3 (cb-client-builder/build (assoc m :hash-alg :crc-hash))
          cf4 (cb-client-builder/build (assoc m :hash-alg :fnv1-64-hash))
          cf5 (cb-client-builder/build (assoc m :hash-alg :fnv1a-64-hash))
          cf6 (cb-client-builder/build (assoc m :hash-alg :fnv1-32-hash))
          cf7 (cb-client-builder/build (assoc m :hash-alg :fnv1a-32-hash))
          cf8 (cb-client-builder/build (assoc m :hash-alg :else))]
      (cb-client-builder/set-hash-alg! cb :ketama-hash)
      (is (instance? CouchbaseCljClientBuilder cb))
      (is (= (.getHashAlg cf1) DefaultHashAlgorithm/NATIVE_HASH))
      (is (= (.getHashAlg cf2) DefaultHashAlgorithm/KETAMA_HASH))
      (is (= (.getHashAlg cf3) DefaultHashAlgorithm/CRC_HASH))
      (is (= (.getHashAlg cf4) DefaultHashAlgorithm/FNV1_64_HASH))
      (is (= (.getHashAlg cf5) DefaultHashAlgorithm/FNV1A_64_HASH))
      (is (= (.getHashAlg cf6) DefaultHashAlgorithm/FNV1_32_HASH))
      (is (= (.getHashAlg cf7) DefaultHashAlgorithm/FNV1A_32_HASH))
      (is (= (.getHashAlg cf8) DefaultHashAlgorithm/NATIVE_HASH)))))

(deftest set-max-reconnect-delay!-test
  (testing "Set max-reconnect-delay option to a client."
    (let [cb (cb-client-builder/create-client-builder {})
          cf (cb-client-builder/build
              (merge base-opts-with-hash-alg-and-failure-mode
                     {:max-reconnect-delay 500}))]
      (cb-client-builder/set-max-reconnect-delay! cb 1000)
      (is (instance? CouchbaseCljClientBuilder cb))
      (is (= (.getMaxReconnectDelay cf) 500)))))

;; TODO: Test failed, not working.
;(deftest set-min-reconnect-interval!-test
;  (testing "Set min-reconnect-interval option to a client."
;    (let [cf (cb-client-builder/build {:bucket tf/bucket
;                                       :password tf/bucket-password
;                                       :uris (map #(URI. %) tf/uris)
;                                       :hash-alg :native-hash
;                                       :failure-mode :redistribute
;                                       :min-reconnect-interval 2000})]
;      (is (= (.getMinReconnectInterval cf) 2000)))))

; APIs not provided?
;(deftest set-obs-poll-interval!-test)
;(deftest set-obs-poll-max!-test)

(deftest set-op-queue-max-block-time!-test
  (testing "Set op-queue-max-block-time option to a client."
    (let [cb (cb-client-builder/create-client-builder {})
          cf (cb-client-builder/build
              (merge base-opts-with-hash-alg-and-failure-mode
                     {:op-queue-max-block-time 15000}))]
      (cb-client-builder/set-op-queue-max-block-time! cb 1000)
      (is (instance? CouchbaseCljClientBuilder cb))
      (is (= (.getOpQueueMaxBlockTime cf) 15000)))))

(deftest set-op-timeout!-test
  (testing "Set op-timeout option to a client."
    (let [cb (cb-client-builder/create-client-builder {})
          cf (cb-client-builder/build
              (merge base-opts-with-hash-alg-and-failure-mode
                     {:op-timeout 10000}))]
      (cb-client-builder/set-op-timeout! cb 1000)
      (is (instance? CouchbaseCljClientBuilder cb))
      (is (= (.getOperationTimeout cf) 10000)))))

(deftest set-read-buffer-size!-test
  (testing "Set read-buffer-size option to a client."
    (let [cb (cb-client-builder/create-client-builder {})
          cf (cb-client-builder/build
              (merge base-opts-with-hash-alg-and-failure-mode
                     {:read-buffer-size 10000}))]
      (cb-client-builder/set-read-buffer-size! cb 1000)
      (is (instance? CouchbaseCljClientBuilder cb))
      (is (= (.getReadBufSize cf) 10000)))))

(deftest set-should-optimize!-test
  (testing "Set should-optimize option to a client."
    (let [cb (cb-client-builder/create-client-builder {})
          cf (cb-client-builder/build
              (merge base-opts-with-hash-alg-and-failure-mode
                     {:should-optimize false}))]
      (cb-client-builder/set-should-optimize! cb false)
      (is (instance? CouchbaseCljClientBuilder cb))
      (is (= (.shouldOptimize cf) false)))))

(deftest set-timeout-exception-threshold!-test
  (testing "Set timeout-exception-threshold option to a client builder."
    (let [cb (cb-client-builder/create-client-builder {})
          cf (cb-client-builder/build
              (merge base-opts-with-hash-alg-and-failure-mode
                     {:timeout-exception-threshold 10000}))]
      (cb-client-builder/set-timeout-exception-threshold! cb 1000)
      (is (instance? CouchbaseCljClientBuilder cb))
      (is (= (.getTimeoutExceptionThreshold cf) 9998)))))

(deftest set-transcoder!-test
  (testing "Set use-nagle-algorithm option to a client builder."
    (let [transcoder (SerializingTranscoder.)
          cb (cb-client-builder/create-client-builder {})
          cl (cb-client/create-client
              (merge base-opts-with-hash-alg-and-failure-mode
                     {:transcoder transcoder}))]
      (cb-client-builder/set-transcoder! cb transcoder)
      (is (instance? CouchbaseCljClientBuilder cb))
      (is (instance? SerializingTranscoder
                     (.getTranscoder (cb-client/get-client cl)))))))

(deftest set-use-nagle-algorithm!-test
  (testing "Set use-nagle-algorithm option to a client builder."
    (let [cb (cb-client-builder/create-client-builder {})
          cf (cb-client-builder/build
              (merge base-opts-with-hash-alg-and-failure-mode
                     {:use-nagle-algorithm true}))]
      (cb-client-builder/set-use-nagle-algorithm! cb true)
      (is (instance? CouchbaseCljClientBuilder cb))
      (is (= (.useNagleAlgorithm cf) true)))))

(deftest create-client-builder-test
  (testing "Creation of CouchbaseCljClientBuilder."
    (let [cb (cb-client-builder/create-client-builder
              {:hash-alg :native-hash
               :failure-mode :redistribute})]
      (is (instance? couchbase_clj.client_builder.CouchbaseCljClientBuilder
                     cb)))))

(deftest create-factory-test
  (testing "Creation of CouchbaseConnectionFactory"
    (let [fb (cb-client-builder/get-factory-builder
              (cb-client-builder/create-client-builder
               {:hash-alg :native-hash
                :failure-mode :redistribute}))
          cf (cb-client-builder/create-factory
              (merge base-opts-with-hash-alg-failure-mode-array-list-uris
                     {:factory-builder fb}))]
      (is (instance? CouchbaseConnectionFactory cf)))))

(deftest build-test
  (testing "Building of CouchbaseConnectionFactory with default value insertion."
    (let [cf (cb-client-builder/build
              (merge base-opts
                     {:auth-descriptor (AuthDescriptor.
                                        (into-array [ "plain" ])
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
                      :use-nagle-algorithm true}))] 
      (is (instance? CouchbaseConnectionFactory cf)))))
