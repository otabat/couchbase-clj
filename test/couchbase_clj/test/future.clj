(ns couchbase-clj.test.future
  (:import [net.spy.memcached CASResponse]
           [net.spy.memcached.internal BulkGetFuture GetFuture OperationFuture]
           [net.spy.memcached.ops OperationStatus CASOperationStatus]
           [com.couchbase.client.internal HttpFuture]
           [com.couchbase.client.protocol.views View])
  (:use [clojure.test])
  (:require [couchbase-clj.client :as cb-client]
            [couchbase-clj.future :as cb-future]
            [couchbase-clj.test.fixture :as tf]))

;(use-fixtures :once tf/setup-client tf/flush-data)
(use-fixtures :once tf/setup-client)

(deftest cas-response-test
  (testing "Get the CASResponse converted to a Clojure keyword."
    (is (= (cb-future/cas-response CASResponse/OK) :ok))
    (is (= (cb-future/cas-response CASResponse/NOT_FOUND) :not-found))
    (is (= (cb-future/cas-response CASResponse/EXISTS) :exists))
    (is (= (cb-future/cas-response CASResponse/OBSERVE_ERROR_IN_ARGS)
           :observe-error-in-args))
    (is (= (cb-future/cas-response CASResponse/OBSERVE_MODIFIED)
           :observe-modified))
    (is (= (cb-future/cas-response CASResponse/OBSERVE_TIMEOUT)
           :observe-timeout))
    (is (= (cb-future/cas-response :else) nil))))

(deftest bgf-deref-test
  (testing "BulkGetFuture deref."
    (cb-client/delete (tf/get-client) :bgf-deref1)
    (cb-client/delete (tf/get-client) :bgf-deref2)
    (cb-client/add (tf/get-client) :bgf-deref1 1)
    (cb-client/add (tf/get-client) :bgf-deref2 2)
    (is (= @(cb-client/async-get-multi (tf/get-client) [:bgf-deref1])
           {"bgf-deref1" 1}))
    (is (= @(cb-client/async-get-multi (tf/get-client) [:bgf-deref1 :bgf-deref2])
           {"bgf-deref1" 1 "bgf-deref2" 2}))
    (is (= (-> (cb-client/async-get-multi (tf/get-client) [:bgf-deref1])
               (deref 5000 :timeout))
           {"bgf-deref1" 1}))
    (is (= (-> (cb-client/async-get-multi (tf/get-client)
                                          [:bgf-deref1 :bgf-deref2])
               (deref 5000 :timeout))
           {"bgf-deref1" 1 "bgf-deref2" 2}))))

(deftest bgf-realized?-test
  (testing "BulkGetFuture realized?"
    (cb-client/delete (tf/get-client) :bgf-realized)
    (cb-client/add (tf/get-client) :bgf-realized 1)
    (let [fut (cb-client/async-get-multi (tf/get-client) [:bgf-realized])]
      (is (true? (realized? (do @fut fut)))))))

(deftest bgf-get-future-test
  (testing "BulkGetFuture get-future."
    (cb-client/delete (tf/get-client) :bgf-get-future)
    (cb-client/add (tf/get-client) :bgf-get-future 1)
    (let [fut (-> (cb-client/async-get-multi (tf/get-client) [:bgf-get-future])
                  cb-future/get-future)] 
      (is (instance? BulkGetFuture fut)))))

(deftest bgf-deref-json-test
  (testing "BulkGetFuture deref-json."
    (cb-client/delete (tf/get-client) :bgf-deref-json1)
    (cb-client/delete (tf/get-client) :bgf-deref-json2)
    (cb-client/add-json (tf/get-client) :bgf-deref-json1 {:a 1})
    (cb-client/add-json (tf/get-client) :bgf-deref-json2 {:b 2})
    (is (= (-> (cb-client/async-get-multi (tf/get-client) [:bgf-deref-json1])
               cb-future/deref-json)
           {"bgf-deref-json1" {:a 1}}))
    (is (= (-> (cb-client/async-get-multi (tf/get-client)
                                          [:bgf-deref-json1 :bgf-deref-json2])
               cb-future/deref-json)
           {"bgf-deref-json1" {:a 1} "bgf-deref-json2" {:b 2}}))
    (is (= (-> (cb-client/async-get-multi (tf/get-client)
                                          [:bgf-deref-json1])
               (cb-future/deref-json 5000 :timeout))
           {"bgf-deref-json1" {:a 1}}))
    (is (= (-> (cb-client/async-get-multi (tf/get-client)
                                          [:bgf-deref-json1 :bgf-deref-json2])
               (cb-future/deref-json 5000 :timeout))
           {"bgf-deref-json1" {:a 1} "bgf-deref-json2" {:b 2}}))))

(deftest bgf-future-get-test
  (testing "BulkGetFuture future-get."
    (cb-client/delete (tf/get-client) :bgf-future-get1)
    (cb-client/delete (tf/get-client) :bgf-future-get2)
    (cb-client/add (tf/get-client) :bgf-future-get1 1)
    (cb-client/add (tf/get-client) :bgf-future-get2 2)
    (is (= (-> (cb-client/async-get-multi (tf/get-client) [:bgf-future-get1])
               cb-future/future-get)
           {"bgf-future-get1" 1}))
    (is (= (-> (cb-client/async-get-multi (tf/get-client)
                                          [:bgf-future-get1 :bgf-future-get2])
               cb-future/future-get)
           {"bgf-future-get1" 1 "bgf-future-get2" 2}))
    (is (= (-> (cb-client/async-get-multi (tf/get-client) [:bgf-future-get1])
               (cb-future/future-get 5000))
           {"bgf-future-get1" 1}))
    (is (= (-> (cb-client/async-get-multi (tf/get-client)
                                       [:bgf-future-get1 :bgf-future-get2])
            (cb-future/future-get 5000))
           {"bgf-future-get1" 1 "bgf-future-get2" 2}))))

(deftest bgf-future-get-json-test
  (testing "BulkGetFuture future-get-json."
    (cb-client/delete (tf/get-client) :bgf-future-get-json1)
    (cb-client/delete (tf/get-client) :bgf-future-get-json2)
    (cb-client/add-json (tf/get-client) :bgf-future-get-json1 {:a 1})
    (cb-client/add-json (tf/get-client) :bgf-future-get-json2 {:b 2})
    (is (= (-> (cb-client/async-get-multi (tf/get-client)
                                          [:bgf-future-get-json1])
               cb-future/future-get-json)
           {"bgf-future-get-json1" {:a 1}}))
    (is (= (-> (cb-client/async-get-multi (tf/get-client)
                                          [:bgf-future-get-json1
                                           :bgf-future-get-json2])
               cb-future/future-get-json)
           {"bgf-future-get-json1" {:a 1} "bgf-future-get-json2" {:b 2}}))
    (is (= (-> (cb-client/async-get-multi (tf/get-client)
                                          [:bgf-future-get-json1])
               (cb-future/future-get-json 5000))
           {"bgf-future-get-json1" {:a 1}}))
    (is (= (-> (cb-client/async-get-multi (tf/get-client)
                                          [:bgf-future-get-json1
                                           :bgf-future-get-json2])
               (cb-future/future-get-json 5000))
           {"bgf-future-get-json1" {:a 1} "bgf-future-get-json2" {:b 2}}))))

(deftest bgf-future-status-test
  (testing "BulkGetFuture future-status."
    (cb-client/delete (tf/get-client) :bgf-future-status)
    (cb-client/add (tf/get-client) :bgf-future-status 1)
    (let [st (-> (cb-client/async-get-multi (tf/get-client) [:bgf-future-status])
                 (cb-future/future-status))]
      (is (instance? CASOperationStatus st)))))

(deftest bgf-future-cancel-test
  (testing "BulkGetFuture future-cancel."
    (cb-client/delete (tf/get-client) :bgf-future-cancel)
    (cb-client/add (tf/get-client) :bgf-future-cancel 1)
    (let [rs (-> (cb-client/async-get-multi (tf/get-client) [:bgf-future-cancel])
                 cb-future/future-cancel)]
      (is (instance? Boolean rs)))))

(deftest bgf-future-cancelled?-test
  (testing "BulkGetFuture future-cancelled?."
    (cb-client/delete (tf/get-client) :bgf-future-cancelled)
    (cb-client/add (tf/get-client) :bgf-future-cancelled 1)
    (let [fut (cb-client/async-get-multi (tf/get-client) [:bgf-future-cancel])]
      (cb-future/future-cancel fut)
      (is (true? (cb-future/future-cancelled? fut))))))

(deftest bgf-future-done?-test
  (testing "BulkGetFuture future-done?."
    (cb-client/delete (tf/get-client) :bgf-future-done)
    (cb-client/add (tf/get-client) :bgf-future-done 1)
    (let [fut (cb-client/async-get-multi (tf/get-client) [:bgf-future-done])]
      @fut
      (is (true? (cb-future/future-done? fut))))))

(deftest gf-deref-test
  (testing "GetFuture deref."
    (cb-client/delete (tf/get-client) :gf-deref)
    (cb-client/add (tf/get-client) :gf-deref 1)
    (is (= @(cb-client/async-get (tf/get-client) :gf-deref) 1))
    (is (= (deref (cb-client/async-get (tf/get-client) :gf-deref) 5000 :timeout)
           1))))

(deftest gf-realized?-test
  (testing "GetFuture realized?"
    (cb-client/delete (tf/get-client) :gf-realized)
    (cb-client/add (tf/get-client) :gf-realized 1)
    (let [fut (cb-client/async-get (tf/get-client) :gf-realized)]
      (is (true? (realized? (do @fut fut)))))))

(deftest gf-get-future-test
  (testing "GetFuture get-future."
    (cb-client/delete (tf/get-client) :gf-get-future)
    (cb-client/add (tf/get-client) :gf-get-future 1)
    (let [fut (-> (cb-client/async-get (tf/get-client) :gf-get-future)
                  cb-future/get-future)] 
      (is (instance? GetFuture fut)))))

(deftest gf-deref-json-test
  (testing "GetFuture deref-json."
    (cb-client/delete (tf/get-client) :gf-deref-json)
    (cb-client/add-json (tf/get-client) :gf-deref-json {:a 1})
    (is (= (-> (cb-client/async-get (tf/get-client) :gf-deref-json)
               cb-future/deref-json)
           {:a 1}))
    (is (= (-> (cb-client/async-get (tf/get-client) :gf-deref-json)
               (cb-future/deref-json 5000 :timeout))
           {:a 1}))))

(deftest gf-future-get-test
  (testing "GetFuture future-get."
    (cb-client/delete (tf/get-client) :gf-future-get)
    (cb-client/add (tf/get-client) :gf-future-get 1)
    (is (= (-> (cb-client/async-get (tf/get-client) :gf-future-get)
               cb-future/future-get)
           1))
    (is (= (-> (cb-client/async-get (tf/get-client) :gf-future-get)
               (cb-future/future-get 5000))
           1))))

(deftest gf-future-get-json-test
  (testing "GetFuture future-get-json."
    (cb-client/delete (tf/get-client) :gf-future-get-json)
    (cb-client/add-json (tf/get-client) :gf-future-get-json {:a 1})
    (is (= (-> (cb-client/async-get (tf/get-client) :gf-future-get-json)
               cb-future/future-get-json)
           {:a 1}))
    (is (= (-> (cb-client/async-get (tf/get-client) :gf-future-get-json)
               (cb-future/future-get-json 5000))
           {:a 1}))))

(deftest gf-future-status-test
  (testing "GetFuture future-status."
    (cb-client/delete (tf/get-client) :gf-future-status)
    (cb-client/add (tf/get-client) :gf-future-status 1)
    (let [st (-> (cb-client/async-get (tf/get-client) :gf-future-status)
                 cb-future/future-status)]
      (is (instance? CASOperationStatus st)))))

(deftest gf-future-cancel-test
  (testing "GetFuture future-cancel."
    (cb-client/delete (tf/get-client) :gf-future-cancel)
    (cb-client/add (tf/get-client) :gf-future-cancel 1)
    (let [rs (cb-future/future-cancel
              (cb-client/async-get (tf/get-client) :gf-future-cancel))]
      (is (instance? Boolean rs)))))

(deftest gf-future-cancelled?-test
  (testing "GetFuture future-cancelled?."
    (cb-client/delete (tf/get-client) :gf-future-cancelled)
    (cb-client/add (tf/get-client) :gf-future-cancelled 1)
    (let [fut (cb-client/async-get (tf/get-client) :gf-future-cancel)]
      (cb-future/future-cancel fut)
      (is (true? (cb-future/future-cancelled? fut))))))

(deftest gf-future-done?-test
  (testing "GetFuture future-done?."
    (cb-client/delete (tf/get-client) :gf-future-done)
    (cb-client/add (tf/get-client) :gf-future-done 1)
    (let [fut (cb-client/async-get (tf/get-client) :gf-future-done)]
      @fut
      (is (true? (cb-future/future-done? fut))))))

(deftest of-deref-test
  (testing "OperationFuture deref."
    (cb-client/delete (tf/get-client) :of-deref)
    (cb-client/add (tf/get-client) :of-deref 1)
    (is (= (-> @(cb-client/async-get-cas (tf/get-client) :of-deref)
               cb-client/cas-val)
           1))
    (is (= (-> (cb-client/async-get-cas (tf/get-client) :of-deref)
               (deref 5000 :timeout)
               cb-client/cas-val)
           1))))

(deftest of-realized?-test
  (testing "OperationFuture realized?"
    (cb-client/delete (tf/get-client) :of-realized)
    (cb-client/add (tf/get-client) :of-realized 1)
    (let [fut (cb-client/async-get-cas (tf/get-client) :of-realized)]
      (is (true? (realized? (do @fut fut)))))))

(deftest of-get-future-test
  (testing "OperationFuture get-future."
    (cb-client/delete (tf/get-client) :of-get-future)
    (cb-client/add (tf/get-client) :of-get-future 1)
    (let [fut (-> (cb-client/async-get-cas (tf/get-client) :of-get-future)
                  cb-future/get-future)] 
      (is (instance? OperationFuture fut)))))

(deftest of-future-get-test
  (testing "OperationFuture future-get."
    (cb-client/delete (tf/get-client) :of-future-get)
    (cb-client/add (tf/get-client) :of-future-get 1)
    (is (= (-> (cb-client/async-get-cas (tf/get-client) :of-future-get)
               cb-future/future-get
               cb-client/cas-val)
           1))
    (is (= (-> (cb-client/async-get-cas (tf/get-client) :of-future-get)
               (cb-future/future-get 5000)
               cb-client/cas-val)
           1))))

(deftest of-future-status-test
  (testing "OperationFuture future-status."
    (cb-client/delete (tf/get-client) :of-future-status)
    (cb-client/add (tf/get-client) :of-future-status 1)
    (let [st (-> (cb-client/async-get-cas (tf/get-client) :of-future-status)
                 cb-future/future-status)]
      (is (instance? CASOperationStatus st)))))

(deftest of-future-cancel-test
  (testing "OperationFuture future-cancel."
    (cb-client/delete (tf/get-client) :of-future-cancel)
    (cb-client/add (tf/get-client) :of-future-cancel 1)
    (let [rs (-> (cb-client/async-get-cas (tf/get-client) :of-future-cancel)
                 cb-future/future-cancel)]
      (is (instance? Boolean rs)))))

(deftest of-future-cancelled?-test
  (testing "OperationFuture future-cancelled?."
    (cb-client/delete (tf/get-client) :of-future-cancelled)
    (cb-client/add (tf/get-client) :of-future-cancelled 1)
    (let [fut (cb-client/async-get-cas (tf/get-client) :of-future-cancel)]
      (cb-future/future-cancel fut)
      (is (true? (cb-future/future-cancelled? fut))))))

(deftest of-future-done?-test
  (testing "OperationFuture future-done?."
    (cb-client/delete (tf/get-client) :of-future-done)
    (cb-client/add (tf/get-client) :of-future-done 1)
    (let [fut (cb-client/async-get (tf/get-client) :of-future-done)]
      @fut
      (is (= (cb-future/future-done? fut) true)))))

(deftest hf-deref-test
  (testing "HttpFuture deref."
    (is (->> @(cb-client/async-get-view (tf/get-client)
                                        tf/design-doc tf/view)
             (instance? View)))
    (is (->  (cb-client/async-get-view (tf/get-client) tf/design-doc tf/view)
             (deref 5000 :timeout)
             (->> (instance? View))))))

(deftest hf-realized?-test
  (testing "HttpFuture realized?"
    (let [fut (cb-client/async-get-view (tf/get-client) tf/design-doc tf/view)]
      (is (true? (realized? (do @fut fut)))))))

(deftest hf-get-future-test
  (testing "HttpFuture get-future."
    (let [fut (-> (cb-client/async-get-view (tf/get-client)
                                            tf/design-doc
                                            tf/view)
                  cb-future/get-future)] 
      (is (instance? HttpFuture fut)))))

(deftest hf-future-get-test
  (testing "HttpFuture future-get."
    (cb-client/delete (tf/get-client) :of-future-get)
    (cb-client/add (tf/get-client) :of-future-get 1)
    (is (-> (cb-client/async-get-view (tf/get-client) tf/design-doc tf/view)
            cb-future/future-get
            (->> (instance? View))))
    (is (-> (cb-client/async-get-view (tf/get-client) tf/design-doc tf/view)
            (cb-future/future-get 5000)
            (->> (instance? View))))))

(deftest hf-future-status-test
  (testing "HttpFuture future-status."
    (let [st (-> (cb-client/async-get-view (tf/get-client) tf/design-doc tf/view)
                 cb-future/future-status)]
      (is (instance? OperationStatus st)))))

(deftest hf-future-cancel-test
  (testing "HttpFuture future-cancel."
    (let [rs (-> (cb-client/async-get-view (tf/get-client) tf/design-doc tf/view)
                 cb-future/future-cancel)]
      (is (instance? Boolean rs)))))

(deftest hf-future-cancelled?-test
  (testing "HttpFuture future-cancelled?."
    (let [fut (cb-client/async-get-view (tf/get-client) tf/design-doc tf/view)]
      (cb-future/future-cancel fut)
      (is (true? (cb-future/future-cancelled? fut))))))

(deftest hf-future-done?-test
  (testing "HttpFuture future-done?."
    (let [fut (cb-client/async-get (tf/get-client) tf/design-doc tf/view)]
      @fut
      (is (true? (cb-future/future-done? fut))))))
