(ns couchbase-clj.future
  (:import [java.util.concurrent Future TimeUnit TimeoutException]
           [net.spy.memcached CASResponse]
           [net.spy.memcached.internal BulkGetFuture GetFuture OperationFuture]
           [com.couchbase.client CouchbaseConnectionFactory]
           [com.couchbase.client.internal HttpFuture])
  (:refer-clojure :exclude [future-cancel future-cancelled? future-done?])
  (:require [couchbase-clj.util :as cb-util]))

(defn cas-response
  "Get the CASResponse values converted to a corresponding keyword.
  If cas-reponse equals to:
    CASResponse/OK then :ok is returned
    CASResponse/NOT_FOUND then :not-found is returned
    CASResponse/EXISTS then :exits is returned
    CASResponse/OBSERVE_ERROR_IN_ARGS :observe-error-in-args is returned
    CASResponse/OBSERVE_MODIFIED :observe-modified is returned
    CASResponse/OBSERVE_TIMEOUT :observe-modified is returnd"
  [^CASResponse cas-response]
  (cond (= cas-response CASResponse/OK) :ok
        (= cas-response CASResponse/NOT_FOUND) :not-found
        (= cas-response CASResponse/EXISTS) :exists
        (= cas-response CASResponse/OBSERVE_ERROR_IN_ARGS) :observe-error-in-args
        (= cas-response CASResponse/OBSERVE_MODIFIED) :observe-modified
        (= cas-response CASResponse/OBSERVE_TIMEOUT) :observe-timeout))

(defprotocol ICouchbaseCljFuture
  (get-future [clj-future] "Get the Future object.")
  (deref-json
    [clj-future]
    [clj-future timeout default]
    "Deref a JSON string and return the converted Clojure data.
  With no arguments, the default operation timeout will be set,
  and if it has reached, the TimeoutException will be thrown.
  The variant taking a timeout will return default and continue processing,
  if the timeout (in milliseconds) has reached and catched a TimeoutException.")
  (future-get
    [clj-future]
    [clj-future timeout]
    "Get the result data from the Future object.
  With no arguments, the default operation timeout will be set,
  and if has reached, the TimeoutException will be thrown.
  You can pass a optional timeout argument in milliseconds.")
  (future-get-json
    [clj-future]
    [clj-future timeout]
    "Get the result data from the Future object that is a JSON string.
  With no arguments, the default operation timeout will be set,
  and if has reached, the OperationTimeoutException will be thrown.
  You can pass a optional timeout argument in milliseconds.")
  (future-status [clj-future]
    "Return the status of the Future object.
  Internally calls a getStatus method.")
  (future-cancel [clj-future] "Cancel the future, if possible.")
  (future-cancelled? [clj-future] "Returns true if future is cancelled?")
  (future-done? [clj-future] "Returns true if future is done."))

(deftype CouchbaseCljBulkGetFuture
  [^CouchbaseConnectionFactory cf ^BulkGetFuture fut]
  clojure.lang.IDeref
  (deref [clj-future]
    (let [m (into {} (.get fut (.getOperationTimeout cf) TimeUnit/MILLISECONDS))]
      (when-not (empty? m)
        m)))

  clojure.lang.IBlockingDeref
  (deref [clj-future timeout default]
    (try (let [m (into {} (.get fut timeout TimeUnit/MILLISECONDS))]
           (when-not (empty? m)
             m))
         (catch TimeoutException e
           default)))

  clojure.lang.IPending
  (isRealized [clj-future] (.isDone fut))

  ICouchbaseCljFuture
  (get-future [clj-future] fut)
  (deref-json [clj-future]
    (let [m (deref clj-future)]
      (reduce #(merge %1 {(key %2)
                          (cb-util/read-json (val %2))}) nil m)))
  (deref-json [clj-future timeout default]
    (let [m (deref clj-future timeout default)]
      (reduce #(merge %1 {(key %2)
                          (cb-util/read-json (val %2))}) nil m)))
  (future-get [clj-future]
    (future-get clj-future  (.getOperationTimeout cf)))
  (future-get [clj-future timeout]
    (let [m (into {} (.get fut ^long timeout TimeUnit/MILLISECONDS))]
      (when-not (empty? m)
        m)))
  (future-get-json [clj-future]
    (let [m (future-get clj-future)]
      (when-not (empty? m)
        (reduce #(merge %1 {(key %2)
                            (cb-util/read-json (val %2))}) nil m))))
  (future-get-json [clj-future timeout]
    (let [m (future-get clj-future timeout)]
      (when-not (empty? m)
        (reduce #(merge %1 {(key %2)
                            (cb-util/read-json (val %2))}) nil m))))
  (future-status [clj-future] (.getStatus ^BulkGetFuture fut))
  (future-cancel [clj-future] (.cancel fut true))
  (future-cancelled? [clj-future] (.isCancelled fut))
  (future-done? [clj-future] (.isDone fut)))

(deftype CouchbaseCljGetFuture [^CouchbaseConnectionFactory cf ^GetFuture fut]
  clojure.lang.IDeref
  (deref [clj-future]
    (.get fut (.getOperationTimeout cf) TimeUnit/MILLISECONDS))

  clojure.lang.IBlockingDeref
  (deref [clj-future timeout default]
    (try (.get fut timeout TimeUnit/MILLISECONDS)
         (catch TimeoutException e
           default)))

  clojure.lang.IPending
  (isRealized [clj-future] (.isDone fut))

  ICouchbaseCljFuture
  (get-future [clj-future] fut)
  (deref-json [clj-future] (cb-util/read-json (deref clj-future)))
  (deref-json [clj-future timeout default]
    (cb-util/read-json (deref clj-future timeout default)))
  (future-get [clj-future] (future-get clj-future (.getOperationTimeout cf)))
  (future-get [clj-future timeout]
    (.get fut ^long timeout TimeUnit/MILLISECONDS))
  (future-get-json [clj-future]
    (future-get-json clj-future (.getOperationTimeout cf)))
  (future-get-json [clj-future timeout]
    (cb-util/read-json (future-get clj-future timeout)))
  (future-status [clj-future] (.getStatus ^GetFuture fut))
  (future-cancel [clj-future] (.cancel fut true))
  (future-cancelled? [clj-future] (.isCancelled fut))
  (future-done? [clj-future] (.isDone fut)))

(deftype CouchbaseCljOperationFuture
  [^CouchbaseConnectionFactory cf ^OperationFuture fut]
  clojure.lang.IDeref
  (deref [clj-future]
    (let [rs (.get fut (.getOperationTimeout cf) TimeUnit/MILLISECONDS)]
      (if (instance? CASResponse rs)
        (cas-response rs)
        rs)))

  clojure.lang.IBlockingDeref
  (deref [clj-future timeout default]
    (try
      (let [rs (.get fut timeout TimeUnit/MILLISECONDS)]
        (if (instance? CASResponse rs)
          (cas-response rs)
          rs))
         (catch TimeoutException e
           default)))

  clojure.lang.IPending
  (isRealized [clj-future] (.isDone fut))

  ICouchbaseCljFuture
  (get-future [clj-future] fut)
  (future-get [clj-future]
    (future-get clj-future (.getOperationTimeout cf)))
  (future-get [clj-future timeout]
    (let [rs (.get fut ^long timeout TimeUnit/MILLISECONDS)]
      (if (instance? CASResponse rs)
        (cas-response rs)
        rs)))
  (future-status [clj-future] (.getStatus ^OperationFuture fut))
  (future-cancel [clj-future] (.cancel fut true))
  (future-cancelled? [clj-future] (.isCancelled fut))
  (future-done? [clj-future] (.isDone fut)))

(deftype CouchbaseCljHttpFuture [^CouchbaseConnectionFactory cf ^HttpFuture fut]
  clojure.lang.IDeref
  (deref [clj-future]
    (let [rs (.get fut (.getOperationTimeout cf) TimeUnit/MILLISECONDS)]
      (if (instance? Iterable rs)
        (seq rs)
        rs)))
  
  clojure.lang.IBlockingDeref
  (deref [clj-future timeout default]
    (try (let [rs (.get fut timeout TimeUnit/MILLISECONDS)]
           (if (instance? Iterable rs)
             (seq rs)
             rs))
         (catch TimeoutException e
           default)))

  clojure.lang.IPending
  (isRealized [clj-future] (.isDone fut))

  ICouchbaseCljFuture
  (get-future [clj-future] fut)
  (future-get [clj-future]
    (future-get clj-future (.getOperationTimeout cf)))
  (future-get [clj-future timeout]
    (.get fut ^long timeout TimeUnit/MILLISECONDS))
  (future-status [clj-future] (.getStatus ^HttpFuture fut))
  (future-cancel [clj-future] (.cancel fut true))
  (future-cancelled? [clj-future] (.isCancelled fut))
  (future-done? [clj-future] (.isDone fut)))
