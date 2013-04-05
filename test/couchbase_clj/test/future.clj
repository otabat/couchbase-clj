(ns couchbase-clj.test.future
  (:import [net.spy.memcached CASResponse]
           [net.spy.memcached.internal BulkGetFuture]
           [net.spy.memcached.internal GetFuture]
           [net.spy.memcached.internal OperationFuture]
           [net.spy.memcached.ops OperationStatus]
           [net.spy.memcached.ops CASOperationStatus]
           [com.couchbase.client.internal HttpFuture]
           [com.couchbase.client.protocol.views View])
  (:require [couchbase-clj.client :as cbc]
            [couchbase-clj.future :as cbf]
            [couchbase-clj.test.fixture :as tf])
  (:use [clojure.test]))

;(use-fixtures :once tf/setup-client tf/flush-data)
(use-fixtures :once tf/setup-client)

(deftest cas-response-test
  (testing "Get the CASResponse converted to a Clojure keyword."
    (is (= (cbf/cas-response CASResponse/OK) :ok))
    (is (= (cbf/cas-response CASResponse/NOT_FOUND) :not-found))
    (is (= (cbf/cas-response CASResponse/EXISTS) :exists))
    (is (= (cbf/cas-response CASResponse/OBSERVE_ERROR_IN_ARGS) :observe-error-in-args))
    (is (= (cbf/cas-response CASResponse/OBSERVE_MODIFIED) :observe-modified))
    (is (= (cbf/cas-response CASResponse/OBSERVE_TIMEOUT) :observe-timeout))
    (is (= (cbf/cas-response :else) nil))))

(deftest bgf-deref-test
  (testing "BulkGetFuture deref."
    (cbc/delete (tf/get-client) :bgf-deref1)
    (cbc/delete (tf/get-client) :bgf-deref2)
    (cbc/add (tf/get-client) :bgf-deref1 1)
    (cbc/add (tf/get-client) :bgf-deref2 2)
    (is (= @(cbc/async-get-multi (tf/get-client) [:bgf-deref1]) {"bgf-deref1" 1}))
    (is (= @(cbc/async-get-multi (tf/get-client) [:bgf-deref1 :bgf-deref2])
           {"bgf-deref1" 1 "bgf-deref2" 2}))
    (is (= (deref (cbc/async-get-multi (tf/get-client) [:bgf-deref1]) 5000 :timeout)
           {"bgf-deref1" 1}))
    (is (= (deref (cbc/async-get-multi (tf/get-client) [:bgf-deref1 :bgf-deref2]) 5000 :timeout)
           {"bgf-deref1" 1 "bgf-deref2" 2}))))

(deftest bgf-realized?-test
  (testing "BulkGetFuture realized?"
    (cbc/delete (tf/get-client) :bgf-realized)
    (cbc/add (tf/get-client) :bgf-realized 1)
    (let [fut (cbc/async-get-multi (tf/get-client) [:bgf-realized])]
      (is (true? (realized? (do @fut fut)))))))

(deftest bgf-get-future-test
  (testing "BulkGetFuture get-future."
    (cbc/delete (tf/get-client) :bgf-get-future)
    (cbc/add (tf/get-client) :bgf-get-future 1)
    (let [fut (cbf/get-future (cbc/async-get-multi (tf/get-client) [:bgf-get-future]))] 
      (is (instance? BulkGetFuture fut)))))

(deftest bgf-deref-json-test
  (testing "BulkGetFuture deref-json."
    (cbc/delete (tf/get-client) :bgf-deref-json1)
    (cbc/delete (tf/get-client) :bgf-deref-json2)
    (cbc/add-json (tf/get-client) :bgf-deref-json1 {:a 1})
    (cbc/add-json (tf/get-client) :bgf-deref-json2 {:b 2})
    (is (= (cbf/deref-json (cbc/async-get-multi (tf/get-client) [:bgf-deref-json1]))
           {"bgf-deref-json1" {:a 1}}))
    (is (= (cbf/deref-json (cbc/async-get-multi (tf/get-client)
                                                [:bgf-deref-json1 :bgf-deref-json2]))
           {"bgf-deref-json1" {:a 1} "bgf-deref-json2" {:b 2}}))
    (is (= (cbf/deref-json (cbc/async-get-multi (tf/get-client)
                                                [:bgf-deref-json1]) 5000 :timeout)
           {"bgf-deref-json1" {:a 1}}))
    (is (= (cbf/deref-json (cbc/async-get-multi
                            (tf/get-client)
                            [:bgf-deref-json1 :bgf-deref-json2]) 5000 :timeout)
           {"bgf-deref-json1" {:a 1} "bgf-deref-json2" {:b 2}}))))

(deftest bgf-future-get-test
  (testing "BulkGetFuture future-get."
    (cbc/delete (tf/get-client) :bgf-future-get1)
    (cbc/delete (tf/get-client) :bgf-future-get2)
    (cbc/add (tf/get-client) :bgf-future-get1 1)
    (cbc/add (tf/get-client) :bgf-future-get2 2)
    (is (= (cbf/future-get (cbc/async-get-multi (tf/get-client) [:bgf-future-get1]))
           {"bgf-future-get1" 1}))
    (is (= (cbf/future-get (cbc/async-get-multi (tf/get-client)
                                                [:bgf-future-get1 :bgf-future-get2]))
           {"bgf-future-get1" 1 "bgf-future-get2" 2}))
    (is (= (cbf/future-get (cbc/async-get-multi (tf/get-client) [:bgf-future-get1]) 5000)
           {"bgf-future-get1" 1}))
    (is (= (cbf/future-get (cbc/async-get-multi (tf/get-client)
                                                [:bgf-future-get1 :bgf-future-get2]) 5000)
           {"bgf-future-get1" 1 "bgf-future-get2" 2}))))

(deftest bgf-future-get-json-test
  (testing "BulkGetFuture future-get-json."
    (cbc/delete (tf/get-client) :bgf-future-get-json1)
    (cbc/delete (tf/get-client) :bgf-future-get-json2)
    (cbc/add-json (tf/get-client) :bgf-future-get-json1 {:a 1})
    (cbc/add-json (tf/get-client) :bgf-future-get-json2 {:b 2})
    (is (= (cbf/future-get-json (cbc/async-get-multi (tf/get-client)
                                                     [:bgf-future-get-json1]))
           {"bgf-future-get-json1" {:a 1}}))
    (is (= (cbf/future-get-json (cbc/async-get-multi
                            (tf/get-client)
                            [:bgf-future-get-json1 :bgf-future-get-json2]))
           {"bgf-future-get-json1" {:a 1} "bgf-future-get-json2" {:b 2}}))
    (is (= (cbf/future-get-json (cbc/async-get-multi (tf/get-client)
                                                [:bgf-future-get-json1]) 5000)
           {"bgf-future-get-json1" {:a 1}}))
    (is (= (cbf/future-get-json (cbc/async-get-multi
                            (tf/get-client)
                            [:bgf-future-get-json1 :bgf-future-get-json2]) 5000)
           {"bgf-future-get-json1" {:a 1} "bgf-future-get-json2" {:b 2}}))))

(deftest bgf-future-status-test
  (testing "BulkGetFuture future-status."
    (cbc/delete (tf/get-client) :bgf-future-status)
    (cbc/add (tf/get-client) :bgf-future-status 1)
    (let [st (cbf/future-status (cbc/async-get-multi (tf/get-client) [:bgf-future-status]))]
      (is (instance? CASOperationStatus st)))))

(deftest bgf-future-cancel-test
  (testing "BulkGetFuture future-cancel."
    (cbc/delete (tf/get-client) :bgf-future-cancel)
    (cbc/add (tf/get-client) :bgf-future-cancel 1)
    (let [rs (cbf/future-cancel (cbc/async-get-multi (tf/get-client) [:bgf-future-cancel]))]
      (is (instance? Boolean rs)))))

(deftest bgf-future-cancelled?-test
  (testing "BulkGetFuture future-cancelled?."
    (cbc/delete (tf/get-client) :bgf-future-cancelled)
    (cbc/add (tf/get-client) :bgf-future-cancelled 1)
    (let [fut (cbc/async-get-multi (tf/get-client) [:bgf-future-cancel])]
      (cbf/future-cancel fut)
      (is (true? (cbf/future-cancelled? fut))))))

(deftest bgf-future-done?-test
  (testing "BulkGetFuture future-done?."
    (cbc/delete (tf/get-client) :bgf-future-done)
    (cbc/add (tf/get-client) :bgf-future-done 1)
    (let [fut (cbc/async-get-multi (tf/get-client) [:bgf-future-done])]
      @fut
      (is (true? (cbf/future-done? fut))))))

(deftest gf-deref-test
  (testing "GetFuture deref."
    (cbc/delete (tf/get-client) :gf-deref)
    (cbc/add (tf/get-client) :gf-deref 1)
    (is (= @(cbc/async-get (tf/get-client) :gf-deref) 1))
    (is (= (deref (cbc/async-get (tf/get-client) :gf-deref) 5000 :timeout) 1))))

(deftest gf-realized?-test
  (testing "GetFuture realized?"
    (cbc/delete (tf/get-client) :gf-realized)
    (cbc/add (tf/get-client) :gf-realized 1)
    (let [fut (cbc/async-get (tf/get-client) :gf-realized)]
      (is (true? (realized? (do @fut fut)))))))

(deftest gf-get-future-test
  (testing "GetFuture get-future."
    (cbc/delete (tf/get-client) :gf-get-future)
    (cbc/add (tf/get-client) :gf-get-future 1)
    (let [fut (cbf/get-future (cbc/async-get (tf/get-client) :gf-get-future))] 
      (is (instance? GetFuture fut)))))

(deftest gf-deref-json-test
  (testing "GetFuture deref-json."
    (cbc/delete (tf/get-client) :gf-deref-json)
    (cbc/add-json (tf/get-client) :gf-deref-json {:a 1})
    (is (= (cbf/deref-json (cbc/async-get (tf/get-client) :gf-deref-json))
           {:a 1}))
    (is (= (cbf/deref-json (cbc/async-get (tf/get-client) :gf-deref-json) 5000 :timeout)
           {:a 1}))))

(deftest gf-future-get-test
  (testing "GetFuture future-get."
    (cbc/delete (tf/get-client) :gf-future-get)
    (cbc/add (tf/get-client) :gf-future-get 1)
    (is (= (cbf/future-get (cbc/async-get (tf/get-client) :gf-future-get)) 1))
    (is (= (cbf/future-get (cbc/async-get (tf/get-client) :gf-future-get) 5000) 1))))

(deftest gf-future-get-json-test
  (testing "GetFuture future-get-json."
    (cbc/delete (tf/get-client) :gf-future-get-json)
    (cbc/add-json (tf/get-client) :gf-future-get-json {:a 1})
    (is (= (cbf/future-get-json (cbc/async-get (tf/get-client) :gf-future-get-json))
           {:a 1}))
    (is (= (cbf/future-get-json (cbc/async-get (tf/get-client) :gf-future-get-json) 5000)
           {:a 1}))))

(deftest gf-future-status-test
  (testing "GetFuture future-status."
    (cbc/delete (tf/get-client) :gf-future-status)
    (cbc/add (tf/get-client) :gf-future-status 1)
    (let [st (cbf/future-status (cbc/async-get (tf/get-client) :gf-future-status))]
      (is (instance? CASOperationStatus st)))))

(deftest gf-future-cancel-test
  (testing "GetFuture future-cancel."
    (cbc/delete (tf/get-client) :gf-future-cancel)
    (cbc/add (tf/get-client) :gf-future-cancel 1)
    (let [rs (cbf/future-cancel (cbc/async-get (tf/get-client) :gf-future-cancel))]
      (is (instance? Boolean rs)))))

(deftest gf-future-cancelled?-test
  (testing "GetFuture future-cancelled?."
    (cbc/delete (tf/get-client) :gf-future-cancelled)
    (cbc/add (tf/get-client) :gf-future-cancelled 1)
    (let [fut (cbc/async-get (tf/get-client) :gf-future-cancel)]
      (cbf/future-cancel fut)
      (is (true? (cbf/future-cancelled? fut))))))

(deftest gf-future-done?-test
  (testing "GetFuture future-done?."
    (cbc/delete (tf/get-client) :gf-future-done)
    (cbc/add (tf/get-client) :gf-future-done 1)
    (let [fut (cbc/async-get (tf/get-client) :gf-future-done)]
      @fut
      (is (true? (cbf/future-done? fut))))))

(deftest of-deref-test
  (testing "OperationFuture deref."
    (cbc/delete (tf/get-client) :of-deref)
    (cbc/add (tf/get-client) :of-deref 1)
    (is (= (cbc/cas-val @(cbc/async-get-cas (tf/get-client) :of-deref)) 1))
    (is (= (->  (cbc/async-get-cas (tf/get-client) :of-deref)
                (deref 5000 :timeout)
                cbc/cas-val)
           1))))

(deftest of-realized?-test
  (testing "OperationFuture realized?"
    (cbc/delete (tf/get-client) :of-realized)
    (cbc/add (tf/get-client) :of-realized 1)
    (let [fut (cbc/async-get-cas (tf/get-client) :of-realized)]
      (is (true? (realized? (do @fut fut)))))))

(deftest of-get-future-test
  (testing "OperationFuture get-future."
    (cbc/delete (tf/get-client) :of-get-future)
    (cbc/add (tf/get-client) :of-get-future 1)
    (let [fut (cbf/get-future (cbc/async-get-cas (tf/get-client) :of-get-future))] 
      (is (instance? OperationFuture fut)))))

(deftest of-future-get-test
  (testing "OperationFuture future-get."
    (cbc/delete (tf/get-client) :of-future-get)
    (cbc/add (tf/get-client) :of-future-get 1)
    (is (= (-> (cbc/async-get-cas (tf/get-client) :of-future-get)
               cbf/future-get
               cbc/cas-val)
           1))
    (is (= (-> (cbc/async-get-cas (tf/get-client) :of-future-get)
               (cbf/future-get 5000)
               cbc/cas-val)
           1))))

(deftest of-future-status-test
  (testing "OperationFuture future-status."
    (cbc/delete (tf/get-client) :of-future-status)
    (cbc/add (tf/get-client) :of-future-status 1)
    (let [st (cbf/future-status (cbc/async-get-cas (tf/get-client) :of-future-status))]
      (is (instance? CASOperationStatus st)))))

(deftest of-future-cancel-test
  (testing "OperationFuture future-cancel."
    (cbc/delete (tf/get-client) :of-future-cancel)
    (cbc/add (tf/get-client) :of-future-cancel 1)
    (let [rs (cbf/future-cancel (cbc/async-get-cas (tf/get-client) :of-future-cancel))]
      (is (instance? Boolean rs)))))

(deftest of-future-cancelled?-test
  (testing "OperationFuture future-cancelled?."
    (cbc/delete (tf/get-client) :of-future-cancelled)
    (cbc/add (tf/get-client) :of-future-cancelled 1)
    (let [fut (cbc/async-get-cas (tf/get-client) :of-future-cancel)]
      (cbf/future-cancel fut)
      (is (true? (cbf/future-cancelled? fut))))))

(deftest of-future-done?-test
  (testing "OperationFuture future-done?."
    (cbc/delete (tf/get-client) :of-future-done)
    (cbc/add (tf/get-client) :of-future-done 1)
    (let [fut (cbc/async-get (tf/get-client) :of-future-done)]
      @fut
      (is (= (cbf/future-done? fut) true)))))

(deftest hf-deref-test
  (testing "HttpFuture deref."
    (is (instance? View @(cbc/async-get-view (tf/get-client) tf/design-doc tf/view)))
    (is (->  (cbc/async-get-view (tf/get-client) tf/design-doc tf/view)
             (deref 5000 :timeout)
             (->> (instance? View))))))

(deftest hf-realized?-test
  (testing "HttpFuture realized?"
    (let [fut (cbc/async-get-view (tf/get-client) tf/design-doc tf/view)]
      (is (true? (realized? (do @fut fut)))))))

(deftest hf-get-future-test
  (testing "HttpFuture get-future."
    (let [fut (cbf/get-future (cbc/async-get-view (tf/get-client) tf/design-doc tf/view))] 
      (is (instance? HttpFuture fut)))))

(deftest hf-future-get-test
  (testing "HttpFuture future-get."
    (cbc/delete (tf/get-client) :of-future-get)
    (cbc/add (tf/get-client) :of-future-get 1)
    (is (-> (cbc/async-get-view (tf/get-client) tf/design-doc tf/view)
            cbf/future-get
            (->> (instance? View))))
    (is (-> (cbc/async-get-view (tf/get-client) tf/design-doc tf/view)
            (cbf/future-get 5000)
            (->> (instance? View))))))

(deftest hf-future-status-test
  (testing "HttpFuture future-status."
    (let [st (cbf/future-status (cbc/async-get-view (tf/get-client) tf/design-doc tf/view))]
      (is (instance? OperationStatus st)))))

(deftest hf-future-cancel-test
  (testing "HttpFuture future-cancel."
    (let [rs (cbf/future-cancel (cbc/async-get-view (tf/get-client) tf/design-doc tf/view))]
      (is (instance? Boolean rs)))))

(deftest hf-future-cancelled?-test
  (testing "HttpFuture future-cancelled?."
    (let [fut (cbc/async-get-view (tf/get-client) tf/design-doc tf/view)]
      (cbf/future-cancel fut)
      (is (true? (cbf/future-cancelled? fut))))))

(deftest hf-future-done?-test
  (testing "HttpFuture future-done?."
    (let [fut (cbc/async-get (tf/get-client) tf/design-doc tf/view)]
      @fut
      (is (true? (cbf/future-done? fut))))))
