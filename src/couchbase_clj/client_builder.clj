(ns couchbase-clj.client-builder
  (:import [java.net URI]
           [java.util Collection ArrayList]
           [java.util.concurrent TimeUnit]
           [net.spy.memcached FailureMode DefaultHashAlgorithm]
           [com.couchbase.client CouchbaseConnectionFactoryBuilder])
  (:require [couchbase-clj.config :as cb-config]))

(defn- hash-alg
  [k]
  (cond (= k :native-hash) DefaultHashAlgorithm/NATIVE_HASH
        (= k :ketama-hash) DefaultHashAlgorithm/KETAMA_HASH
        (= k :crc-hash) DefaultHashAlgorithm/CRC_HASH
        (= k :fnv1-64-hash) DefaultHashAlgorithm/FNV1_64_HASH
        (= k :fnv1a-64-hash) DefaultHashAlgorithm/FNV1A_64_HASH
        (= k :fnv1-32-hash) DefaultHashAlgorithm/FNV1_32_HASH
        (= k :fnv1a-32-hash) DefaultHashAlgorithm/FNV1A_32_HASH
        :else DefaultHashAlgorithm/NATIVE_HASH))

(defn- failure-mode
  [k]
  (cond (= k :redistribute) FailureMode/Redistribute
        (= k :retry) FailureMode/Retry
        (= k :cancel) FailureMode/Cancel
        :else FailureMode/Redistribute))

(defprotocol URIFactory
  (^java.net.URI str->uri [s] "Creates java.net.URI from the input string."))

(extend-protocol URIFactory
  URI
  (str->uri [^URI uri] uri)
  String
  (str->uri [^String s] (URI. s)))

(defprotocol ICouchbaseCljClientBuilder
  (get-factory-builder [clj-client-builder]
    "Get the CouchbaseConnectionFactoryBuilder object.")
  (set-auth-descriptor! [clj-client-builder dsc]
    "Set the auth descriptor to enable authentication on new connections.
  dsc is an AuthDescriptor object.")
  (set-daemon! [clj-client-builder b]
    "If true, the IO thread should be a daemon thread.")
  (set-failure-mode! [clj-client-builder k]
    "Set the failure mode as a keyword value.
  Default values is :redistribute.
  Other values are :retry, :cancel.
  In redistribute mode, the failure of a node will cause its current queue
  and future requests to move to the next logical node in the cluster for a
  given key.
  Retry mode is appropriate when you have a rare short downtime of a
  couchbase node that will be back quickly, and your app is written to not
  wait very long for async command completion.
  In cancel mode, all operations are automatically cancelled")
  (set-hash-alg! [clj-client-builder k]
    "Set the hashing algorithm as a keyword value.
  Default value is :native-hash.
  Other values are :ketama-hash, :crc-hash,
  :fnv1-64-hash, :fnv1a-64-hash, :fnv1-32-hash, :fnv1a-32-hash.")
  (set-max-reconnect-delay! [clj-client-builder delay]
    "Set maximum number of milliseconds to wait between reconnect attempts.
  Default value is 30000.
  You can set this value lower when there is intermittent
  and frequent connection failures.")
  ;(set-min-reconnect-interval! [clj-client-builder interval]
  ;  "Set the default minimum reconnect interval in millisecs.
  ;Default values is 1100
  ;This value means that if a reconnect is needed,
  ;it won't try to reconnect more frequently than default value.
  ;The internal connections take up to 500ms per request.
  ;You can set this to higher to try reconnecting less frequently.")
  (set-obs-poll-interval! [clj-client-builder interval]
    "Set the polling interval for Observe operations.
  Default value is 100.
  Set this higher or lower depending on whether the polling needs
  to happen less or more frequently depending on the tolerance limits
  for the Observe operation as compared to other operations.")
  (set-obs-poll-max! [clj-client-builder poll]
    "Set the maximum times to poll the master and replica(s) to meet
  the desired durability requirements.
  Default value is 400.
  You could set this value higher if the Observe operations do not complete
  after the normal polling.")
  (set-op-queue-max-block-time! [clj-client-builder time]
    "Set the maximum time to block waiting for op queue operations to complete,
  in milliseconds.
  Default value is 10000.
  The default has been set with the expectation that most requests
  are interactive and waiting for more than a few seconds is thus
  more undesirable than failing the request.
  However, this value could be lowered for operations
  not to block for this time.")
  (set-op-timeout! [clj-client-builder timeout]
    "Set the time for an operation to Timeout.
  Default values is 2500.
  You can set this value higher when there is heavy network traffic
  and timeouts happen frequently.
  This is used as a default timeout value for sync and async operations.
  For async operations, it is internally used as a default timeout value to get
  the result from Future objects.")
  (set-read-buffer-size! [clj-client-builder size]
    "Set the read buffer size.
  Default value is 16384.")
  (set-should-optimize! [clj-client-builder b]
    "Set the optimize behavior for the network.
  Default values is false.
  You can set this value to be true if the performance should be optimized
  for the network as in cases where there are some known issues with the network
  that may be causing adverse effects on applications.
  Currently it is ignored.")
  (set-timeout-exception-threshold! [clj-client-builder timeout]
    "Set the maximum timeout exception threshold.
  Default threshold is 998.
  Minimum threshold is 2 and is calculated by timeout - 2.
  For this reason, specify timeout equal to theshold + 2,
  ex: (set-timeout-exception-threshold client_builder_object 1000)")
  (set-transcoder! [clj-client-builder transcoder]
    "Set the default transcoder.
  transcoder is a transcoder object.
  Default transcoder is SerializingTranscoder.")
  (set-use-nagle-algorithm! [clj-client-builder b]
    "Set to true if you'd like to enable the Nagle algorithm."))

(deftype CouchbaseCljClientBuilder [^CouchbaseConnectionFactoryBuilder cfb]
  ICouchbaseCljClientBuilder
  (get-factory-builder [clj-client-builder] cfb)
  (set-auth-descriptor! [clj-client-builder dsc]
    (.setAuthDescriptor cfb dsc))
  (set-daemon! [clj-client-builder b]
    (.setDaemon cfb b))
  (set-failure-mode! [clj-client-builder k]
    (.setFailureMode cfb (failure-mode k)))
  (set-hash-alg! [clj-client-builder k]
    (.setHashAlg cfb (hash-alg k)))
  (set-max-reconnect-delay! [clj-client-builder delay]
    (.setMaxReconnectDelay cfb delay))
  ;(set-min-reconnect-interval! [clj-client-builder interval]
  ;  (.setReconnectThresholdTime cfb interval TimeUnit/MILLISECONDS))
  (set-obs-poll-interval! [clj-client-builder interval]
    (.setObsPollInterval cfb interval))
  (set-obs-poll-max! [clj-client-builder poll]
    (.setObsPollMax cfb poll))
  (set-op-queue-max-block-time! [clj-client-builder time]
    (.setOpQueueMaxBlockTime cfb time))
  (set-op-timeout! [clj-client-builder timeout]
    (.setOpTimeout cfb timeout))
  (set-read-buffer-size! [clj-client-builder size]
    (.setReadBufferSize cfb size))
  (set-should-optimize! [clj-client-builder b]
    (.setShouldOptimize cfb b))
  (set-timeout-exception-threshold! [clj-client-builder timeout]
    (.setTimeoutExceptionThreshold cfb timeout))
  (set-transcoder! [clj-client-builder transcoder]
    (.setTranscoder cfb transcoder))
  (set-use-nagle-algorithm! [clj-client-builder b]
    (.setUseNagleAlgorithm cfb b)))

(def
  ^{:doc "A key/value conversion map of client options
  to corresponding set functions."}
  method-map
  {:auth-descriptor set-auth-descriptor!
   :daemon set-daemon!
   :failure-mode set-failure-mode!
   :hash-alg set-hash-alg!
   ;; TODO: Observers disabled for now
   ;:initial-observers set-initial-observers!
   :max-reconnect-delay set-max-reconnect-delay!
   ;:min-reconnect-interval set-min-reconnect-interval!
   :obs-poll-interval set-obs-poll-interval!
   :obs-poll-max set-obs-poll-max!
   :op-queue-max-block-time set-op-queue-max-block-time!
   :op-timeout set-op-timeout!
   :read-buffer-size set-read-buffer-size!
   :should-optimize set-should-optimize!
   :timeout-exception-threshold set-timeout-exception-threshold!
   :transcoder set-transcoder!
   :use-nagle-algorithm set-use-nagle-algorithm!})

(defn- dispatch
  [builder kv]
  (let [k (key kv)
        v (val kv)]
    (if-let [method (k method-map)]
      (if (coll? v)
        (apply method builder v)
        (method builder v))
      (throw (java.lang.UnsupportedOperationException.
              (format "Wrong keyword %s specified for a client builder." k))))))

(defn create-client-builder
  "Create and return a CouchbaseCljClientBuilder object.
  This will create a CouchbaseConnectionFactoryBuilder object internally.
  opts is a map to specify options for CouchbaseConnectionFactoryBuilder object.
  Currenty :hash-alg and :failure-mode keywords must be specified.
  ex:
  (client-client-builder {:hash-alg :native-hash :failure-mode :redistribute})

  After creating, you can set options.
  ex: (set-op-timeout! clj-client-builder timeout)

  All options can be looked at method-map Var.
  You can get the internal CouchbaseConnectionFactoryBuilder
  and pass it to create-factory function to build client."
  [opts]
  (let [builder (->CouchbaseCljClientBuilder
                 (CouchbaseConnectionFactoryBuilder.))]
    (doseq [kv opts]
      (dispatch builder kv))
    builder))

(defn create-factory
  "Create a CouchbaseConnectionFactory object.
  You must specify all keyword arguments: factory-builder, bucket, username,
  password, and uris.
  factory-builder can be created from create-client-bulider
  with get-factory-builder function.
  :ex
  (get-factory-builder (create-client-builder opts)).

  uris must be a sequential collection of URI objects.
  :ex
  [(URI. \"http://127.0.0.1:8091/pools\")]"
  [{:keys [^CouchbaseConnectionFactoryBuilder factory-builder
           ^String bucket ^String username ^String password uris]}]
  (-> ^CouchbaseConnectionFactoryBuilder factory-builder
      (.buildCouchbaseConnection uris bucket username password)))

(defn build
  "Create CouchbaseConnectionFactory object from input.
  You can specify keywords arguments: bucket, username, password, uris,
  and other opts.
  bucket is the bucket name. Default value is defined
  as @default-bucket and is \"default\".
  username is the bucket username.
  Default value is definedas @default-username and is a empty string.
  Currently username is ignored.
  password is the bucket password.
  Default value is defined as @default-password and is a empty string.
  uris is a Collection of string uris, ex: [\"http://127.0.0.1:8091/pools\"]
  Other options can be specified for
  CouchbaseConnectionFactoryBuilder object creation.
  Internally, :failure-mode and :hash-alg must have a value
  and those default values are
  :redistribute and :native-hash respectively.
  All options can be looked at method-map Var."
  [{:keys [^String bucket ^String username ^String password
           ^Collection uris failure-mode hash-alg] :as opts}]
  (let [bkt (or bucket @cb-config/default-bucket)
        user (or username @cb-config/default-username)
        pass (or password @cb-config/default-password)
        ^Collection coll-uris  (if uris
                                 (map str->uri uris)
                                 (map str->uri @cb-config/default-uris))
        list-uris (ArrayList. coll-uris)
        failure-mode (or failure-mode :redistribute)
        hash-alg (or hash-alg :native-hash)
        opts (-> (dissoc opts :bucket :username :password :uris)
                 (assoc :failure-mode failure-mode
                        :hash-alg hash-alg))
        builder (create-client-builder opts)]
    (create-factory {:factory-builder (get-factory-builder builder)
                     :uris list-uris
                     :bucket bkt
                     :username user
                     :password pass})))
