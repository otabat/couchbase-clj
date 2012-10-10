(ns couchbase-clj.client
  (:import [java.net URI]
           [java.util Collection]
           [java.util.concurrent TimeUnit]
           [java.util.concurrent Future]
           [net.spy.memcached CASValue]
           [net.spy.memcached.internal GetFuture]
           [net.spy.memcached.internal BulkGetFuture]
           [net.spy.memcached.internal OperationFuture]
           [net.spy.memcached.transcoders Transcoder]
           [net.spy.memcached PersistTo]
           [net.spy.memcached ReplicateTo]
           [com.couchbase.client CouchbaseClient]
           [com.couchbase.client CouchbaseConnectionFactory]
           [com.couchbase.client.internal HttpFuture]
           [com.couchbase.client.protocol.views Query]
           [com.couchbase.client.protocol.views View]
           [com.couchbase.client.protocol.views ViewRow]
           [couchbase_clj.client_builder.CouchbaseCljClientBuilder]
           [couchbase_clj.query.CouchbaseCljQuery])
  (:refer-clojure :exclude [get set replace flush inc dec replicate
                            future-cancel future-cancelled? future-done?])
  (:require [couchbase-clj.query :as cbq])
  (:use [couchbase-clj.client-builder]
        [couchbase-clj.config]
        [couchbase-clj.future]
        [couchbase-clj.util]))

(def ^:private persist-to-map {:master PersistTo/MASTER
                               :one PersistTo/ONE
                               :two PersistTo/TWO
                               :three PersistTo/THREE
                               :four PersistTo/FOUR})
(def ^:private replicate-to-map {:zero ReplicateTo/ZERO
                                 :one ReplicateTo/ONE
                                 :two ReplicateTo/TWO
                                 :three ReplicateTo/THREE})

(defn persist-to
  "Get the PersistTo object by specifying a corresponding keyword argument.
  persist can be :master, :one, :two, :three, :four.
  If other value or no argument is specified, 
  @couchbase-clj.config/default-persist will be specified as the default value.

  MASTER or ONE requires Persist to the Master.
  TWO requires Persist to at least two nodes including the Master.
  THREE requires Persist to at least three nodes including the Master.
  FOUR requires Persist to at least four nodes including the Master."
  ([] (@default-persist persist-to-map))
  ([persist]
     (or (and persist (persist persist-to-map))
         (@default-persist persist-to-map))))

(defn replicate-to
  "Get the ReplicateTo object by specifying a corresponding keyword argument.
  replicate can be :zero, :one, :two, :three
  If other value or no argument is specified, 
  @couchbase-clj.config/default-replicate will be specified as the default value.

  ZERO implies no requirements for the data to be replicated to the replicas.
  ONE implies requirements for the data to be replicated with at least one replica.
  TWO implies requirements for the data to be replicated with at least two replicas.
  THREE implies requirements for the data to be replicated with at least three replicas."
  ([] (@default-replicate replicate-to-map))
  ([replicate]
     (or (and replicate (replicate replicate-to-map))
         (@default-replicate replicate-to-map))))

(defn cas-id
  "Get the cas ID from the CASValue object."
  [^CASValue c]
  (when c
    (.getCas c)))

(defn cas-val
  "Get the value from the CASValue object"
  [^CASValue c]
  (when c
    (.getValue c)))

(defn cas-val-json
  "Get the JSON string value converted to Clojure data from the CASValue object.
  nil is returned, if c is nil."
  [^CASValue c]
  (read-json (cas-val c)))

(defn view-id
  "Get the ID of query results from ViewRow object."
  [^ViewRow view]
  (.getId view))

(defn view-key
  "Get the key of query results from ViewRow object."
  [^ViewRow view]
  (.getKey view))

(defn view-val
  "Get the value of query results from ViewRow object."
  [^ViewRow view]
  (.getValue view))

(defn view-val-json
  "Get the JSON string value of query results from ViewRow object,
  Converted to Clojure data."
  [^ViewRow view]
  (read-json (view-val view)))

(defn view-doc
  "Get document of query results when include-docs is set to true."
  [^ViewRow view]
  (.getDocument view))

(defn view-doc-json
  "Get JSON string document of query results converted to Clojure data
  when include-docs is set to true."
  [^ViewRow view]
  (read-json (.getDocument view)))

(defprotocol ICouchbaseCljClient
  (get-client [clj-client] "Get the CouchbaseClient object.")
  (get-factory [clj-client] "Get the CouchbaseConnectionFactory object.")
  (get-available-servers [clj-client] "Get the addresses of available servers in a Vector.")
  (get-unavailable-servers [clj-client] "Get the addresses of unavailable servers in a Vector.")
  (get-node-locator [clj-client] "Get a read-only wrapper around the node locator wrapping this instance.")
  (get-versions [clj-client] "Get versions of all of the connected servers in a Map.")
  (get-sasl-mechanisms [clj-client] "Get the list of sasl mechanisms in a Set.")
  (get-client-status
    [clj-client]
    "Get all of the stats from all of the connections in a Map."
    ;; TODO: Seems not working
    ; [clj-client k]
    )
  (get-auth-descriptor [clj-client] "Get the auth descriptor.")
  (get-failure-mode [clj-client] "Get the failure mode.")
  (get-hash-alg [clj-client] "Get the hashing algorithm.")
  (get-max-reconnect-delay [clj-client] "Get the max reconnect delay.")
  (get-min-reconnect-interval [clj-client] "Get the min reconnect interval.")
  ;; TODO: APIs not provided?
  ;(get-obs-poll-interval [clj-client])
  ;(get-obs-poll-max [clj-client])
  (get-op-queue-max-block-time [clj-client] "Get the op queue max block time.")
  (get-op-timeout [clj-client]
    "Get the operation timeout.
  This is used as a default timeout value for various sync and async client operations.")
  (get-read-buffer-size [clj-client] "Get the read buffer size.")
  (get-timeout-exception-threshold [clj-client] "Get the timeout exception threshold.")
  (get-transcoder [clj-client] "Get the default transcoder.")
  (daemon? [clj-client]
    "Return true if IO thread should be a daemon thread, otherwise return false.")
  (should-optimize? [clj-client]
    "Return if the performance should be optimized for the network, otherwise return false.")
  (use-nagle-algorithm? [clj-client]
    "Return true if the Nagle algorithm is specified, otherwise return false.")
  (async-add
    [clj-client k v]
    [clj-client k v opts]
    "Asynchronously add a value with the specified key that does not already exist.
  Return value is a CouchbaseCljOperationFuture object.
  k is the key and can be a keyword, symbol or a string.
  v is the value to be stored.
  You can specify a optional key value map as the opts argument.
  Optional keywords are :expiry, :transcoder, and when :observe is set to true,
  persist, and :replicate can be set.  

  expiry is the integer expiry time for key in seconds.
  Values larger than 30*24*60*60 seconds (30 days) are interpreted as absolute times from the epoch.
  By specifying -1, expiry can be disabled.
  If expiry is not specified, 
  @couchbase-clj.config/default-data-expiry will be specified as the default value.

  transcoder is the Transcoder object to be used to serialize the value.
  If transcoder is not specified,
  SerializingTranscoder will be specified as the default transcoder.

  observe is the Boolean flag to enable persist and replicate options.

  persist is the keyword to specify Persist requirements to Master and more servers.
  Values can be :master, :one, :two, :three, :four.
  If persist is not specified, 
  @couchbase-clj.config/default-persist will be specified as the default value.

  replicate is the keyword to specify Replication requirements to zero or more replicas.
  Values can be :zero, :one, :two, :three.
  If other value or no argument is specified, 
  @couchbase-clj.config/default-replicate will be specified as a default value.")
  (add
    [clj-client k v]
    [clj-client k v opts]
    "Synchronously add a value with the specified key that does not already exist.
  If adding has succeeded then true is returned, otherwise false.
  k is the key and can be a keyword, symbol or a string.
  v is the value to be stored.
  You can specify a optional key value map as the opts argument.
  Optional keywords are :expiry, :transcoder, :timeout, and when :observe is set to true,
  :persist, and :replicate can be set.  

  expiry is the integer expiry time for key in seconds.
  Values larger than 30*24*60*60 seconds (30 days) are interpreted as absolute times from the epoch.
  By specifying -1, expiry can be disabled.
  If expiry is not specified, 
  @couchbase-clj.config/default-data-expiry will be specified as the default value.

  transcoder is the Transcoder object to be used to serialize the value.
  If transcoder is not specified,
  SerializingTranscoder will be specified as the default transcoder.

  timeout is the integer operation timeout value in milliseconds.
  If timeout is not specified, the default value will be the value
  set by create-client function.
  It can be retrived by get-op-timeout function.

  observe is the Boolean flag to enable persist and replicate options.

  persist is the keyword to specify Persist requirements to Master and more servers.
  Values can be :master, :one, :two, :three, :four.
  If persist is not specified, 
  @couchbase-clj.config/default-persist will be specified as the default value.

  replicate is the keyword to specify Replication requirements to zero or more replicas.
  Values can be :zero, :one, :two, :three.
  If other value or no argument is specified, 
  @couchbase-clj.config/default-replicate will be specified as a default value.")
  (async-add-json
    [clj-client k v]
    [clj-client k v opts]
    "Asynchronously add a value that will be converted to JSON string
  with the specified key that does not already exist.
  Return value will be a CouchbaseCljOperationFuture object.
  Arguments are the same as async-add.")
  (add-json
    [clj-client k v]
    [clj-client k v opts]
    "Synchronously add a value that will be converted to JSON string
  with the specified key that does not already exist.
  If adding has succeeded then true is returned, otherwise false.
  Arguments are the same as add.")
  (async-append
    [clj-client k v cas-id]
    [clj-client k v cas-id opts]
    "Asynchronously append a value to an existing key.
  Return value is a CouchbaseCljOperationFuture object.
  k is the key and can be a keyword, symbol or a string.
  v is the value to be stored.
  cas-id is the integer unique value to identify key/value combination.
  You can specify a optional transcoder keyword in a map.

  transcoder is the Transcoder object to be used to serialize the value.
  If transcoder is not specified,
  SerializingTranscoder will be specified as the default transcoder.")
  (append
    [clj-client k v cas-id]
    [clj-client k v cas-id opts]
     "Synchronously append a value to an existing key.
  If appending has succeeded then true is returned, otherwise false.
  k is the key and can be a keyword, symbol or a string.
  v is the value to be stored.
  cas-id is the integer unique value to identify key/value combination.
  You can specify a optional key value map as the opts argument.
  Optional keywords are :transcoder and :timeout.

  transcoder is the Transcoder object to be used to serialize the value.
  If transcoder is not specified,
  SerializingTranscoder will be specified as the default transcoder.

  timeout is the integer operation timeout value in milliseconds.
  If timeout is not specified, the default value will be the value
  set by create-client function.
  It can be retrived by get-op-timeout function.")
  (async-prepend
    [clj-client k v cas-id]
    [clj-client k v cas-id opts]
    "Asynchronously prepend a value to an existing key.
  Return value is a CouchbaseCljOperationFuture object.
  k is the key and can be a keyword, symbol or a string.
  v is the value to be stored.
  cas-id is the integer unique value to identify key/value combination.
  You can specify a optional transcoder keyword in a map.

  transcoder is the Transcoder object to be used to serialize the value.
  If transcoder is not specified,
  SerializingTranscoder will be specified as the default transcoder.")
  (prepend
    [clj-client k v cas-id]
    [clj-client k v cas-id opts]
    "Synchronously prepend a value to an existing key.
  If prepending has succeeded then true is returned, otherwise false.
  k is the key and can be a keyword, symbol or a string.
  v is the value to be stored.
  cas-id is the integer unique value to identify key/value combination.
  You can specify a optional key value map as the opts argument.
  Optional keywords are :transcoder and :timeout.

  transcoder is the Transcoder object to be used to serialize the value.
  If transcoder is not specified,
  SerializingTranscoder will be specified as the default transcoder.

  timeout is the integer operation timeout value in milliseconds.
  If timeout is not specified, the default value will be the value
  set by create-client function.
  It can be retrived by get-op-timeout function.")
  (async-delete
    [clj-client k]
    [clj-client k opts]
    "Asynchronously delete the specified key.
  k is the key and can be a keyword, symbol or a string.
  Currently no options can be specified.")
  (delete
    [clj-client k]
    [clj-client k opts]
    "Synchronously delete the specified key.
  If deletion has succeeded then true is returned, otherwise false.
  Return value is a CouchbaseCljOperationFuture object.
  k is the key and can be a keyword, symbol or a string.
  You can specify a optional key value map as the opts argument.
  Optional keyword is :timeout.

  timeout is the integer operation timeout value in milliseconds.
  If timeout is not specified, the default value will be the value
  set by create-client function.
  It can be retrived by get-op-timeout function.")
  (async-get
    [clj-client k]
    [clj-client k opts]
    "Asynchronously get the value of the specified key.
  Return value is a CouchbaseCljGetFuture object.
  You can specify a optional transcoder keyword in a map.

  transcoder is the Transcoder object to be used to serialize the value.
  If transcoder is not specified,
  SerializingTranscoder will be specified as the default transcoder.")
  (get
    [clj-client k]
    [clj-client k opts]
    "Synchronously get the value of the specified key.
  You can specify a optional transcoder keyword in a map.

  transcoder is the Transcoder object to be used to serialize the value.
  If transcoder is not specified,
  SerializingTranscoder will be specified as the default transcoder.")
  (get-json
    [clj-client k]
    [clj-client k opts]
    "Synchronously get the JSON string value converted to Clojure data of the specified key.
  You can specify a optional transcoder keyword in a map.
  Arguments are the same as get.")
  (async-get-touch
    [clj-client k]
    [clj-client k opts]
    "Asynchronously get a value and update the expiration time for a given key
  Return value is a CouchbaseCljOperationFuture object.
  You can specify a optional key value map as the opts argument.
  Optional keywords are :expiry and :transcoder.

  expiry is the integer expiry time for key in seconds.
  Values larger than 30*24*60*60 seconds (30 days) are interpreted as absolute times from the epoch.
  By specifying -1, expiry can be disabled.
  If expiry is not specified, 
  @couchbase-clj.config/default-data-expiry will be specified as the default value.

  transcoder is the Transcoder object to be used to serialize the value.
  If transcoder is not specified,
  SerializingTranscoder will be specified as the default transcoder.")
  (get-touch
    [clj-client k]
    [clj-client k opts]
    "Synchronously get a value and update the expiration time for a given key
  Return value is a CASValue object.
  You can specify a optional key value map as the opts argument.
  Optional keywords are :expiry and :transcoder.

  expiry is the integer expiry time for key in seconds.
  Values larger than 30*24*60*60 seconds (30 days) are interpreted as absolute times from the epoch.
  By specifying -1, expiry can be disabled.
  If expiry is not specified, 
  @couchbase-clj.config/default-data-expiry will be specified as the default value.

  transcoder is the Transcoder object to be used to serialize the value.
  If transcoder is not specified,
  SerializingTranscoder will be specified as the default transcoder.")
  (async-get-multi
    [clj-client ks]
    [clj-client ks opts]
    "Asynchronously get multiple keys.
  ks is a sequential collection containing keys.
  Key can be a keyword, symbol or a string.
  You can specify a optional transcoder keyword in a map.

  transcoder is the Transcoder object to be used to serialize the value.
  If transcoder is not specified,
  SerializingTranscoder will be specified as the default transcoder.")
  (async-get-multi-json
    [clj-client k]
    [clj-client k opts]
    "Asynchronously get multiple JSON string value converted to Clojure data of the specified key.
  You can specify a optional transcoder keyword in a map.
  Arguments are the same as get-multi.")
  (get-multi
    [clj-client ks]
    [clj-client ks opts]
    "Synchronously get multiple keys.
  ks is a sequential collection containing keys.
  Key can be a keyword, symbol or a string.
  You can specify a optional transcoder keyword in a map.

  transcoder is the Transcoder object to be used to serialize the value.
  If transcoder is not specified,
  SerializingTranscoder will be specified as the default transcoder.")
  (get-multi-json
    [clj-client k]
    [clj-client k opts]
    "Synchronously get multiple JSON string value converted to Clojure data of the specified key.
  You can specify a optional transcoder keyword in a map.
  Arguments are the same as get-multi.")
  (async-get-lock
    [clj-client k]
    [clj-client k opts]
    "Asynchronously get a lock.
  Return value is a CouchbaseCljOperationFuture object.
  k is the key and can be a keyword, symbol or a string.
  You can specify a optional key value map as the opts argument.
  Optional keywords are :expiry and :transcoder.

  expiry is the integer expiry time for key in seconds.
  If expiry is not specified, 
  @couchbase-clj.config/default-lock-expiry will be specified as the default value.

  transcoder is the Transcoder object to be used to serialize the value.
  If transcoder is not specified,
  SerializingTranscoder will be specified as the default transcoder.")
  (get-lock
    [clj-client k]
    [clj-client k opts]
    "Synchronously get a lock.
  Return value is a CASValue object.
  k is the key and can be a keyword, symbol or a string.
  You can specify a optional key value map as the opts argument.
  Optional keywords are :expiry and :transcoder.

  expiry is the integer expiry time for key in seconds.
  If expiry is not specified, 
  @couchbase-clj.config/default-lock-expiry will be specified as the default value.

  transcoder is the Transcoder object to be used to serialize the value.
  If transcoder is not specified,
  SerializingTranscoder will be specified as the default transcoder.")
  (locked? [clj-client k]
    "Retrun true if key is locked.
  k is the key and can be a keyword, symbol or a string.")
  (async-get-cas
    [clj-client k]
    [clj-client k opts]
    "Asynchronously get single key value with CAS value.
  Return value is a CouchbaseCljOperationFuture object.
  k is the key and can be a keyword, symbol or a string.
  You can specify a optional key value map as the opts argument.
  Optional keyword is :transcoder.

  transcoder is the Transcoder object to be used to serialize the value.
  If transcoder is not specified,
  SerializingTranscoder will be specified as the default transcoder.")
  (get-cas
    [clj-client k]
    [clj-client k opts]
    "Synchronously get single key value with CAS value.
  Return value is a CASValue object. 
  k is the key and can be a keyword, symbol or a string.
  You can specify a optional key value map as the opts argument.
  Optional keyword is :transcoder.

  transcoder is the Transcoder object to be used to serialize the value.
  If transcoder is not specified,
  SerializingTranscoder will be specified as the default transcoder.")
  (get-cas-id
    [clj-client k]
    [clj-client k opts]
    "Synchronously get a CAS ID.
  Integer CAS ID is returned.
  Arguments are the same as get-cas.")
  (async-inc
    [clj-client k]
    [clj-client k opts]
    "Asynchronously increment the value of an existing key.
  k is the key and can be a keyword, symbol or a string.
  You can specify a optional key value map as the opts argument.
  Optional keyword is :offset.

  offset is the integer offset value to increment.
  If offset is not specified, 
  @couchbase-clj.config/default-inc-offset will be specified as the default value.")
  (inc
    [clj-client k]
    [clj-client k opts]
    "Synchronously increment the value of an existing key.
  k is the key and can be a keyword, symbol or a string.
  You can specify a optional key value map as the opts argument.
  Optional keywords are :offset, :default and :expiry.

  offset is the integer offset value to increment.
  If offset is not specified, 
  @couchbase-clj.config/default-inc-offset will be specified as the default value.

  default is the default value to increment if key does not exist.
  If default is not specified,
  @couchbase-clj.config/default-inc-default will be specified as the default value.

  expiry is the integer expiry time for key in seconds.
  If expiry is not specified, 
  @couchbase-clj.config/default-lock-expiry will be specified as the default value.")
  (async-dec
    [clj-client k]
    [clj-client k opts]
    "Asynchronously decrement the value of an existing key.
  k is the key and can be a keyword, symbol or a string.
  You can specify a optional key value map as the opts argument.
  Optional keyword is :offset.

  offset is the integer offset value to decrement. 
  If offset is not specified, 
  @couchbase-clj.config/default-dec-offset will be specified as the default value.")
  (dec
    [clj-client k]
    [clj-client k opts]
    "Synchronously decrement the value of an existing key.
  k is the key and can be a keyword, symbol or a string.
  You can specify a optional key value map as the opts argument.
  Optional keywords are :offset, :default and :expiry.

  offset is the integer offset value to increment.
  If offset is not specified, 
  @couchbase-clj.config/default-inc-offset will be specified as the default value.

  default is the default value to increment if key does not exist.
  If default is not specified,
  @couchbase-clj.config/default-inc-default will be specified as the default value.

  expiry is the integer expiry time for key in seconds.
  If expiry is not specified, 
  @couchbase-clj.config/default-lock-expiry will be specified as the default value.")
  (async-replace
    [clj-client k v]
    [clj-client k v opts]
    "Asynchronously update an existing key with a new value.
  Return value is a CouchbaseCljOperationFuture object.
  k is the key and can be a keyword, symbol or a string.
  v is the value to be stored.
  You can specify a optional key value map as the opts argument.
  Optional keywords are :expiry, :transcoder, and when :observe is set to true,
  :persist, and :replicate can be set.

  expiry is the integer expiry time for key in seconds.
  Values larger than 30*24*60*60 seconds (30 days) are interpreted as absolute times from the epoch.
  By specifying -1, expiry can be disabled.
  If expiry is not specified, 
  @couchbase-clj.config/default-data-expiry will be specified as the default value.

  transcoder is the Transcoder object to be used to serialize the value.
  If transcoder is not specified,
  SerializingTranscoder will be specified as the default transcoder.

  observe is the Boolean flag to enable persist and replicate options.

  persist is the keyword to specify Persist requirements to Master and more servers.
  Values can be :master, :one, :two, :three, :four.
  If persist is not specified, 
  @couchbase-clj.config/default-persist will be specified as the default value.

  replicate is the keyword to specify Replication requirements to zero or more replicas.
  Values can be :zero, :one, :two, :three.
  If other value or no argument is specified, 
  @couchbase-clj.config/default-replicate will be specified as a default value.")
  (replace
    [clj-client k v]
    [clj-client k v opts]
    "Synchronously update an existing key with a new value.
  If replacing has succeeded then true is returned, otherwise false.
  k is the key and can be a keyword, symbol or a string.
  v is the value to be stored.
  You can specify a optional key value map as the opts argument.
  Optional keywords are :expiry, :transcoder, :timeout, and when :observe is set to true,
  :persist, and :replicate can be set.

  expiry is the integer expiry time for key in seconds.
  Values larger than 30*24*60*60 seconds (30 days) are interpreted as absolute times from the epoch.
  By specifying -1, expiry can be disabled.
  If expiry is not specified, 
  @couchbase-clj.config/default-data-expiry will be specified as the default value.

  transcoder is the Transcoder object to be used to serialize the value.
  If transcoder is not specified,
  SerializingTranscoder will be specified as the default transcoder.

  timeout is the integer operation timeout value in milliseconds.
  If timeout is not specified, the default value will be the value
  set by create-client function.
  It can be retrived by get-op-timeout function.

  observe is the Boolean flag to enable persist and replicate options.

  persist is the keyword to specify Persist requirements to Master and more servers.
  Values can be :master, :one, :two, :three, :four.
  If persist is not specified, 
  @couchbase-clj.config/default-persist will be specified as the default value.

  replicate is the keyword to specify Replication requirements to zero or more replicas.
  Values can be :zero, :one, :two, :three.
  If other value or no argument is specified, 
  @couchbase-clj.config/default-replicate will be specified as a default value.")
  (async-replace-json
    [clj-client k v]
    [clj-client k v opts]
    "Asynchronously update an existing key with a new value
  that will be converted to a JSON string value.
  Arguments are the same as async-replace.")
  (replace-json
    [clj-client k v]
    [clj-client k v opts]
    "Synchronously update an existing key with a new value
  that will be converted to a JSON string value.
  Arguments are the same as replace.")
  (async-set
    [clj-client k v]
    [clj-client k v opts]
    "Asynchronously store a value using the specified key.
  Return value is a CouchbaseCljOperationFuture object.
  k is the key and can be a keyword, symbol or a string.
  v is the value to be stored.
  You can specify a optional key value map as the opts argument.
  Optional keywords are :expiry, :transcoder, and when :observe is set to true,
  :persist, and :replicate can be set.  

  expiry is the integer expiry time for key in seconds.
  Values larger than 30*24*60*60 seconds (30 days) are interpreted as absolute times from the epoch.
  By specifying -1, expiry can be disabled.
  If expiry is not specified, 
  @couchbase-clj.config/default-data-expiry will be specified as the default value.

  transcoder is the Transcoder object to be used to serialize the value.
  If transcoder is not specified,
  SerializingTranscoder will be specified as the default transcoder.

  observe is the Boolean flag to enable persist and replicate options.

  persist is the keyword to specify Persist requirements to Master and more servers.
  Values can be :master, :one, :two, :three, :four.
  If persist is not specified, 
  @couchbase-clj.config/default-persist will be specified as the default value.

  replicate is the keyword to specify Replication requirements to zero or more replicas.
  Values can be :zero, :one, :two, :three.
  If other value or no argument is specified, 
  @couchbase-clj.config/default-replicate will be specified as a default value.")
  (set
    [clj-client k v]
    [clj-client k v opts]
    "Synchronously store a value using the specified key.
  If set has succeeded then true is returned, otherwise false.
  k is the key and can be a keyword, symbol or a string.
  v is the value to be stored.
  You can specify a optional key value map as the opts argument.
  Optional keywords are :expiry, :transcoder, :timeout, and when :observe is set to true,
  :persist, and :replicate can be set.  

  expiry is the integer expiry time for key in seconds.
  Values larger than 30*24*60*60 seconds (30 days) are interpreted as absolute times from the epoch.
  By specifying -1, expiry can be disabled.
  If expiry is not specified, 
  @couchbase-clj.config/default-data-expiry will be specified as the default value.

  transcoder is the Transcoder object to be used to serialize the value.
  If transcoder is not specified,
  SerializingTranscoder will be specified as the default transcoder.

  timeout is the integer operation timeout value in milliseconds.
  If timeout is not specified, the default value will be the value
  set by create-client function.
  It can be retrived by get-op-timeout function.

  observe is the Boolean flag to enable persist and replicate options.

  persist is the keyword to specify Persist requirements to Master and more servers.
  Values can be :master, :one, :two, :three, :four.
  If persist is not specified, 
  @couchbase-clj.config/default-persist will be specified as the default value.

  replicate is the keyword to specify Replication requirements to zero or more replicas.
  Values can be :zero, :one, :two, :three.
  If other value or no argument is specified, 
  @couchbase-clj.config/default-replicate will be specified as a default value.")
  (async-set-json
    [clj-client k v]
    [clj-client k v opts]
    "Asynchronously store a value that will be converted to a JSON String
  using the specified key.
  Return value is a CouchbaseCljOperationFuture object.
  Arguments are the same as async-set.")
  (set-json
    [clj-client k v]
    [clj-client k v opts]
    "Synchronously store a value that will be converted to a JSON String
  using the specified key.
  If set has succeeded then true is returned, otherwise false.
  Arguments are the same as set.")
  (async-set-cas
    [clj-client k v cas-id]
    [clj-client k v cas-id opts]
    "Asynchronously compare the CAS ID and store a value using the specified key.
  Return value is a CouchbaseCljOperationFuture object.
  k is the key and can be a keyword, symbol or a string.
  v is the value to be stored.
  cas-id is the integer unique value to identify key/value combination.
  You can specify a optional key value map as the opts argument.
  Optional keywords are :expiry and :transcoder.

  expiry is the integer expiry time for key in seconds.
  Values larger than 30*24*60*60 seconds (30 days) are interpreted as absolute times from the epoch.
  By specifying -1, expiry can be disabled.
  If expiry is not specified, 
  @couchbase-clj.config/default-data-expiry will be specified as the default value.

  transcoder is the Transcoder object to be used to serialize the value.
  If transcoder is not specified,
  SerializingTranscoder will be specified as the default transcoder.")
  (set-cas
    [clj-client k v cas-id]
    [clj-client k v cas-id opts]
    "Synchronously compare the CAS ID and store a value using the specified key.
  Keyword results that are originally defined in CASResponse
  and mapped by cas-response function will be returned.
  k is the key and can be a keyword, symbol or a string.
  v is the value to be stored.
  cas-id is the integer unique value to identify key/value combination.
  You can specify a optional key value map as the opts argument.
  Optional keywords are :expiry and :transcoder.

  expiry is the integer expiry time for key in seconds.
  Values larger than 30*24*60*60 seconds (30 days) are interpreted as absolute times from the epoch.
  By specifying -1, expiry can be disabled.
  If expiry is not specified, 
  @couchbase-clj.config/default-data-expiry will be specified as the default value.

  transcoder is the Transcoder object to be used to serialize the value.
  If transcoder is not specified,
  SerializingTranscoder will be specified as the default transcoder.")
  (async-set-cas-json
    [clj-client k v cas-id]
    [clj-client k v cas-id opts]
    "Asynchronously compare the CAS ID and store a value that is converted to a JSON string
  using the specified key.
  Return value is a CouchbaseCljOperationFuture object.
  Arguments are the same as async-set-cas.")
  (set-cas-json
    [clj-client k v cas-id]
    [clj-client k v cas-id opts]
    "Synchronously compare the CAS ID and store a value that is converted to a JSON string
  using the specified key.
  Keyword results that are originally defined in CASResponse
  and mapped by cas-response function will be returned.
  Arguments are the same as set-cas.")
  (async-touch
    [clj-client k]
    [clj-client k opts]
    "Asynchronously update the expiration time for a given key
  Return value is a CouchbaseCljOperationFuture object.
  You can specify a optional key value map as the opts argument.
  Optional keyword is :expiry.

  expiry is the integer expiry time for key in seconds.
  Values larger than 30*24*60*60 seconds (30 days) are interpreted as absolute times from the epoch.
  By specifying -1, expiry can be disabled.
  If expiry is not specified, 
  @couchbase-clj.config/default-data-expiry will be specified as the default value.")
  (touch
    [clj-client k]
    [clj-client k opts]
    "Synchronously update the expiration time for a given key
  If update has succeeded then true is returned, otherwise false.
  You can specify a optional key value map as the opts argument.
  Optional keywords are :expiry and :timeout.

  expiry is the integer expiry time for key in seconds.
  Values larger than 30*24*60*60 seconds (30 days) are interpreted as absolute times from the epoch.
  By specifying -1, expiry can be disabled.
  If expiry is not specified, 
  @couchbase-clj.config/default-data-expiry will be specified as the default value.

  timeout is the integer operation timeout value in milliseconds.
  If timeout is not specified, the default value will be the value
  set by create-client function.
  It can be retrived by get-op-timeout function.")
  (async-unlock
    [clj-client k cas-id]
    [clj-client k cas-id opts]
    "Asynchronously unlock.
  Return value is a CouchbaseCljOperationFuture object.
  k is the key and can be a keyword, symbol or a string.
  cas-id is the integer unique value to identify key/value combination.
  You can specify a optional key value map as the opts argument.
  Optional keyword is :transcoder.

  transcoder is the Transcoder object to be used to serialize the value.
  If transcoder is not specified,
  SerializingTranscoder will be specified as the default transcoder.")
  (unlock
    [clj-client k cas-id]
    [clj-client k cas-id opts]
    "Synchronously unlock.
  If unlocking has succeeded then true is returned, otherwise false.
  k is the key and can be a keyword, symbol or a string.
  cas-id is the integer unique value to identify key/value combination.
  You can specify a optional key value map as the opts argument.
  Optional keyword is :transcoder.

  transcoder is the Transcoder object to be used to serialize the value.
  If transcoder is not specified,
  SerializingTranscoder will be specified as the default transcoder.")
  (async-get-view [clj-client design-doc view-name]
    "Asynchronously get a new view.
  Return value is a CouchbaseCljHttpFuture object.
  design-doc is a design document name.
  view-name is a view name within a design document.")
  (get-view [clj-client design-doc view-name]
    "Synchronously get a new view.
  Return value is a View object.
  design-doc is a design document name.
  view-name is a view name within a design document.")
  (async-get-views [clj-client design-doc]
    "Asynchronously get a Vector of views.
  Return value is a CouchbaseCljHttpFuture object.
  design-doc is a design document name.")
  (get-views [clj-client design-doc]
    "Synchronously get a sequence of views.
  Return value is a Vector of views.
  design-doc is a design document name.")
  (async-query
    [clj-client view q]
    [clj-client design-doc view-name q]
    "Asynchronously query a view within a design doc.
  Return value is a CouchbaseCljHttpFuture object.
  view is a View object.
  q is a CouchbaseCljQuery object or query parameters.
  design-doc is a design document name.
  view-name is a view name within a design document.")
  (query
    [clj-client view q]
    [clj-client design-doc view-name q]
    "Synchronously query a view within a design doc.
  Return value is a sequence of ViewRows.
  view is a View object.
  q is a CouchbaseCljQuery object or query parameters.
  design-doc is a design document name.
  view-name is a view name within a design document.")
  (lazy-query
    [clj-client view q num]
    [clj-client design-doc view-name q num]
    "Lazily query a view within a design doc.
  Response is a lazy sequence of a ViewResponse.
  view is a View object.
  q is a CouchbaseCljQuery object or query parameters.
  design-doc is a design document name.
  view-name is a view name within a design document.
  num is an integer to specify the amount of documents to get in each iterations.

  lazy-query can be used to query a large data lazily
  that it allows you to only get the amount of documents specified per iteration. 

  ex:
  (doseq [res (lazy-query clj-client view q num)]
    (println (view-ids res)))
  => (:id1 :id2 :id3 :id4 :id5)

  (doseq [res (lazy-query clj-client view q num)]
    (println (map view-id res)))
  => (:id1 :id2 :id3 :id4 :id5)")
  (wait-queue
    [clj-client]
    [clj-client timeout]
    "Synchronously wait for the queues to die down.
  Return true if the queues have died down, otherwise false.
  You can specifiy a optional operation timeout value.
  timeout is the integer operation timeout value in milliseconds.
  If timeout is not specified, the default value will be the value
  set by create-client function.
  It can be retrived by get-op-timeout function.")

  ;; TODO: Add observer methods
  ;(observe [clj-client k cas-id])
  ;(add-observer [clj-client conn-obs])
  ;(remove-observer [clj-client conn-obs])
  (flush
    [clj-client]
    [clj-client delay]
    "Flush all cached and persisted data.
  If flushing has succeeded then true is returned, otherwise false.  
  delay is the period of time to delay, in seconds 
  To do flushing, you'll need to enable flush_all by using cbepctl command.
  ex: cbepctl localhost:11210 set flush_param flushall_enabled true
  Currently there is a bug in this command and it may not work as expected.")
  (shutdown
    [clj-client]
    [clj-client timeout]
    "Shut down the client.
  If no argument is specified, client will shutdown immediately.
  If you specify a optional integer operation timeout value (in milliseconds),
  shutdown will occur gracefully.
  timeout is the max waiting time."))

(deftype CouchbaseCljClient [^CouchbaseClient cc ^CouchbaseConnectionFactory cf]
  ICouchbaseCljClient
  (get-client [clj-client] cc)
  (get-factory [clj-client] cf)
  (get-available-servers [clj-client]
    (let [vc (into [] (.getAvailableServers cc))]
      (when-not (empty? vc)
        vc)))
  (get-unavailable-servers [clj-client]
    (let [vc (into [] (.getUnavailableServers cc))]
      (when-not (empty? vc)
        vc)))
  (get-node-locator [clj-client] (.getNodeLocator cc))
  (get-versions [clj-client] (into {} (.getVersions cc)))
  (get-sasl-mechanisms [clj-client] (into #{} (.listSaslMechanisms cc)))
  (get-client-status [clj-client] (into {} (.getStats cc )))

  ;; TODO: Seems not working?
  ;(get-client-status [clj-client k]
  ;  (let [^String nk (name k)]
  ;    (into {} (.getStats cc nk))))
  (get-auth-descriptor [clj-client] (.getAuthDescriptor cf))
  (get-failure-mode [clj-client] (.getFailureMode cf))
  (get-hash-alg [clj-client] (.getHashAlg cf))
  (get-max-reconnect-delay [clj-client] (.getMaxReconnectDelay cf))
  (get-min-reconnect-interval [clj-client] (.getMinReconnectInterval cf))

  ;; TODO: APIs not provided?
  ;(get-obs-poll-interval [clj-client] (.getObsPollInterval cf))
  ;(get-obs-poll-max [clj-client])
  (get-op-queue-max-block-time [clj-client] (.getOpQueueMaxBlockTime cf))
  (get-op-timeout [clj-client] (.getOperationTimeout cf))
  (get-read-buffer-size [clj-client] (.getReadBufSize cf))
  (get-timeout-exception-threshold [clj-client] (.getTimeoutExceptionThreshold cf))
  (get-transcoder [clj-client] (.getTranscoder cc))
  (daemon? [clj-client] (.isDaemon cf))
  (should-optimize? [clj-client] (.shouldOptimize cf))
  (use-nagle-algorithm? [clj-client] (.useNagleAlgorithm cf))
  (async-add [clj-client k v] (async-add clj-client k v {}))
  (async-add [clj-client  k v {:keys [^int expiry ^Transcoder transcoder
                             observe persist replicate]}]
    (let [^String nk (name k)
          ^String sv (str v)
          ^int exp (or expiry ^int @default-data-expiry)
          ^PersistTo p (persist-to persist)
          ^ReplicateTo r (replicate-to replicate)
          ^OperationFuture fut (if transcoder
                                 (.add cc nk exp v transcoder)
                                 (if (true? observe) 
                                   (.add cc nk exp sv p r)
                                   (.add cc nk exp v)))]
      (->CouchbaseCljOperationFuture cf fut)))
  (add [clj-client k v] (add clj-client k v {}))
  (add [clj-client  k v {:keys [^long timeout] :as opts}]
    (let [^long to (or timeout (.getOperationTimeout cf))
          ^OperationFuture fut (get-future (async-add clj-client k v opts))]
      (.get fut to TimeUnit/MILLISECONDS)))
  (async-add-json [clj-client k v] (async-add-json clj-client k v {}))
  (async-add-json [clj-client k v opts]
    (let [jv (json-str v)]
      (async-add clj-client k jv opts)))
  (add-json [clj-client k v] (add-json clj-client k v {}))
  (add-json [clj-client k v opts]
    (let [jv (json-str v)]
      (add clj-client k jv opts)))
  (async-append [clj-client k v cas-id] (async-append clj-client k v cas-id {}))
  (async-append [clj-client k v cas-id {:keys [^Transcoder transcoder]}]
    (let [^String nk (name k)
          ^OperationFuture fut (if transcoder
                                 (.append cc ^long cas-id nk v transcoder)
                                 (.append cc ^long cas-id nk v))]
      (->CouchbaseCljOperationFuture cf fut)))
  (append [clj-client k v cas-id] (append clj-client k v cas-id {}))
  (append [clj-client k v cas-id {:keys [^long timeout] :as opts}]
    (let [^long to (or timeout (.getOperationTimeout cf))
          ^OperationFuture fut (get-future (async-append clj-client k v cas-id opts))]
      (.get fut to TimeUnit/MILLISECONDS)))
  (async-prepend [clj-client k v cas-id] (async-prepend clj-client k v cas-id {}))
  (async-prepend [clj-client k v cas-id {:keys [^Transcoder transcoder]}]
    (let [^String nk (name k)
          ^OperationFuture fut (if transcoder
                                 (.prepend cc ^long cas-id nk v transcoder)
                                 (.prepend cc ^long cas-id nk v))]
      (->CouchbaseCljOperationFuture cf fut)))
  (prepend [clj-client k v cas-id] (prepend clj-client k v cas-id {}))
  (prepend [clj-client k v cas-id {:keys [^long timeout] :as opts}]
    (let [^long to (or timeout (.getOperationTimeout cf))
          ^OperationFuture fut (get-future (async-prepend clj-client k v cas-id opts))]
      (.get fut to TimeUnit/MILLISECONDS)))
  ;; TODO: Currently delete command through observe is unavailable
  ;;       due to a bug in the couchbase cilent sdk.
  (async-delete [clj-client k]
    (let [^String nk (name k)
          ^OperationFuture fut (.delete cc nk)]
      (->CouchbaseCljOperationFuture cf fut)))

  (delete [clj-client k] (delete clj-client k {}))
  ;; TODO: Currently delete command through observe is unavailable
  ;;       due to a bug in the couchbase-cilent.
  (delete [clj-client k {:keys [^long timeout]}]
    (let [^long to (or timeout (.getOperationTimeout cf))
          ^OperationFuture fut (get-future (async-delete clj-client k))]
      (.get fut to TimeUnit/MILLISECONDS)))
  (async-get [clj-client k]
    (async-get clj-client k {}))
  (async-get [clj-client k {:keys [^Transcoder transcoder]}]
    (let [^String nk (name k)
          ^GetFuture fut (if transcoder
                           (.asyncGet cc nk transcoder)
                           (.asyncGet cc nk))]
      (->CouchbaseCljGetFuture cf fut)))
  (get [clj-client k] (get clj-client k {}))
  (get [clj-client k {:keys [^Transcoder transcoder]}]
    (let [^String nk (name k)]
      (if transcoder
       (.get cc nk transcoder)
       (.get cc nk))))
  (get-json [clj-client k] (get-json clj-client k {}))
  (get-json [clj-client k opts] (read-json (get clj-client k opts)))
  (async-get-touch [clj-client k]
    (async-get-touch clj-client k {}))
  (async-get-touch [clj-client k {:keys [^int expiry ^Transcoder transcoder]}]
    (let [^String nk (name k)
          ^int exp (or expiry ^int @default-data-expiry)
          ^OperationFuture fut (if transcoder
                                (.asyncGetAndTouch cc nk exp transcoder)
                                (.asyncGetAndTouch cc nk exp))]
      (->CouchbaseCljOperationFuture cf fut)))
  (get-touch [clj-client k] (get-touch clj-client k {}))
  (get-touch [clj-client k {:keys [^int expiry ^Transcoder transcoder]}]
    (let [^String nk (name k)
          ^int exp (or expiry ^int @default-data-expiry)]
      (when-let [^CASValue c (if transcoder
                               (.getAndTouch cc nk exp transcoder)
                               (.getAndTouch cc nk exp))]
        c)))
  (async-get-multi [clj-client ks]
    (async-get-multi clj-client ks {}))
  (async-get-multi [clj-client ks {:keys [^Transcoder transcoder]}]
    (let [^Collection seq-ks (map name ks)
          ^BulkGetFuture fut (if transcoder
                               (.asyncGetBulk cc seq-ks transcoder)
                               (.asyncGetBulk cc seq-ks))]
      (->CouchbaseCljBulkGetFuture cf fut)))
  (get-multi [clj-client ks] (get-multi clj-client ks {}))
  (get-multi [clj-client ks {:keys [^Transcoder transcoder]}]
    (let [^Collection seq-ks (map name ks)
          m (into {} (if transcoder
                 (.getBulk cc seq-ks transcoder)
                 (.getBulk cc seq-ks)))]
      (when-not (empty? m)
        m)))
  (get-multi-json [clj-client k] (get-multi-json clj-client k {}))
  (get-multi-json [clj-client k opts]
    (reduce #(merge %1 {(key %2)
                        (read-json (val %2))})
            nil
            (get-multi clj-client k opts)))
  (async-get-lock [clj-client k]
    (async-get-lock clj-client k {}))
  (async-get-lock [clj-client k {:keys [^int expiry ^Transcoder transcoder]}]
    (let [^String nk (name k)
          ^int exp (or expiry ^int @default-lock-expiry)
          ^OperationFuture fut (if transcoder
                                 (.asyncGetAndLock cc nk exp transcoder)
                                 (.asyncGetAndLock cc nk exp))]
      (->CouchbaseCljOperationFuture cf fut)))
  (get-lock [clj-client k] (get-lock clj-client k {}))
  (get-lock [clj-client k {:keys [^int expiry ^Transcoder transcoder]}]
    (let [^String nk (name k)
          ^int exp (or expiry ^int @default-lock-expiry)
          ^CASValue c (if transcoder
                        (.getAndLock cc nk exp transcoder)
                        (.getAndLock cc nk exp))]
      c))
  (locked?
    [clj-client k]
    (let [cas-id (get-cas-id clj-client k)]
      (if (= cas-id -1)
        true
        false)))
  (async-get-cas [clj-client k]
    (async-get-cas clj-client k {}))
  (async-get-cas [clj-client k {:keys [^Transcoder transcoder]}]
    (let [^String nk (name k)
          ^OperationFuture fut (if transcoder
                                 (.asyncGets cc nk transcoder)
                                 (.asyncGets cc nk))]
      (->CouchbaseCljOperationFuture cf fut)))
  (get-cas [clj-client k] (get-cas clj-client k {})) 
  (get-cas [clj-client k {:keys [^Transcoder transcoder]}]
    (let [^String nk (name k)]
      (when-let [^CASValue c (if transcoder
                               (.gets cc nk transcoder)
                               (.gets cc nk))]
        c)))
  (get-cas-id [clj-client k] (get-cas-id clj-client k {}))
  (get-cas-id [clj-client k opts]
    (let [^CASValue c (get-cas clj-client k opts)]
      (when c
        (.getCas c))))
  (async-inc [clj-client k]
    (async-inc clj-client k {}))
  (async-inc [clj-client k {:keys [^long offset]}]
    (let [^String nk (name k)
          ^long ofst (or offset ^long @default-inc-offset)
          ^OperationFuture fut (.asyncIncr cc nk ofst)]
      (->CouchbaseCljOperationFuture cf fut)))
  (inc [clj-client k] (inc clj-client k {}))
  (inc [clj-client k {:keys [^long offset ^long default ^int expiry]}]
    (let [^String nk (name k)
          ^long ofst (or offset ^long @default-inc-offset)
          ^long dflt (or default ^long @default-inc-default)
          ^int exp (or expiry ^int @default-data-expiry)]
      (.incr cc nk ofst dflt exp)))
  (async-dec [clj-client k]
    (async-dec clj-client k {}))
  (async-dec [clj-client k {:keys [^long offset]}]
    (let [^String nk (name k)
          ^long ofst (or offset ^long @default-dec-offset)
          ^OperationFuture fut (.asyncDecr cc nk ofst)]
      (->CouchbaseCljOperationFuture cf fut)))
  (dec [clj-client k] (dec clj-client k {}))
  (dec [clj-client k {:keys [^long offset ^long default ^int expiry]}]
    (let [^String nk (name k)
          ^long ofst (or offset ^long @default-dec-offset)
          ^long dflt (or default ^long @default-dec-default)
          ^int exp (or expiry ^int @default-data-expiry)]
      (.decr cc nk ofst dflt exp)))
  (async-replace [clj-client k v]
    (async-replace clj-client k v {}))
  (async-replace [clj-client k v {:keys [^int expiry ^Transcoder transcoder
                                observe persist replicate]}]
    (let [^String nk (name k)
          ^String sv (str v)
          ^int exp (or expiry ^int @default-data-expiry)
          ^PersistTo p (persist-to persist)
          ^ReplicateTo r (replicate-to replicate)
          ^OperationFuture fut (if transcoder
                                 (.replace cc nk exp v transcoder)
                                 (if (true? observe)
                                  (.replace cc nk exp sv p r)
                                  (.replace cc nk exp v)))]
      (->CouchbaseCljOperationFuture cf fut)))
  (replace [clj-client k v] (replace clj-client k v {}))
  (replace [clj-client k v {:keys [^long timeout] :as opts}]
    (let [^long to (or timeout (.getOperationTimeout cf))
          ^OperationFuture fut (get-future (async-replace clj-client k v opts))]
      (.get fut to TimeUnit/MILLISECONDS)))
  (async-replace-json [clj-client k v] (async-replace-json clj-client k v {}))
  (async-replace-json [clj-client k v opts]
    (let [jv (json-str v)]
      (async-replace clj-client k jv opts)))
  (replace-json [clj-client k v] (replace-json clj-client k v {}))
  (replace-json [clj-client k v opts]
    (let [jv (json-str v)]
      (replace clj-client k jv opts)))
  (async-set [clj-client k v] (async-set clj-client k v {}))
  (async-set [clj-client k v {:keys [^int expiry ^Transcoder transcoder
                            observe persist replicate]}]
    (let [^String nk (name k)
          ^String sv (str v)
          ^int exp (or expiry ^int @default-data-expiry)
          ^PersistTo p (persist-to persist)
          ^ReplicateTo r (replicate-to replicate)
          ^OperationFuture fut (if transcoder
                                 (.set cc nk exp v transcoder)
                                 (if (true? observe)
                                   (.set cc nk exp sv p r)
                                   (.set cc nk exp v)))]
      (->CouchbaseCljOperationFuture cf fut)))
  (set [clj-client k v] (set clj-client k v {}))
  (set [clj-client k v {:keys [^long timeout] :as opts}]
    (let [^long to (or timeout (.getOperationTimeout cf))
          ^OperationFuture fut (get-future (async-set clj-client k v opts))]
      (.get fut to TimeUnit/MILLISECONDS)))
  (async-set-json [clj-client k v] (async-set-json clj-client k v {}))
  (async-set-json [clj-client k v opts]
    (let [jv (json-str v)]
      (async-set clj-client k jv opts)))
  (set-json [clj-client k v] (set-json clj-client k v {}))
  (set-json [clj-client k v opts]
    (let [jv (json-str v)]
      (set clj-client k jv opts)))
  (async-set-cas [clj-client k v cas-id]
    (async-set-cas clj-client k v cas-id {}))
  (async-set-cas [clj-client k v cas-id {:keys [^int expiry ^Transcoder transcoder]}]
    (let [^String nk (name k)
          ^int exp (or expiry ^int @default-data-expiry)
          ^Transcoder tc (or transcoder (get-transcoder clj-client))
          ^OperationFuture fut (.asyncCAS cc nk ^long cas-id exp v tc)]
      (->CouchbaseCljOperationFuture cf fut)))
  (set-cas [clj-client k v cas-id] (set-cas clj-client k v cas-id {}))
  (set-cas [clj-client k v cas-id {:keys [^int expiry ^Transcoder transcoder]}]
    (let [^String nk (name k)
          ^int exp (or expiry ^int @default-data-expiry)
          ^Transcoder tc (or transcoder (get-transcoder clj-client))]
      (cas-response (.cas cc nk ^long cas-id exp v tc))))
  (async-set-cas-json [clj-client k v cas-id]
    (async-set-cas-json clj-client k v cas-id {}))
  (async-set-cas-json [clj-client k v cas-id opts]
    (let [jv (json-str v)]
      (async-set-cas clj-client k jv cas-id opts)))
  (set-cas-json [clj-client k v cas-id]
    (set-cas-json clj-client k v cas-id {}))
  (set-cas-json [clj-client k v cas-id opts]
    (let [jv (json-str v)]
      (set-cas clj-client k jv cas-id opts)))
  (async-touch [clj-client k]
    (async-touch clj-client k {}))
  (async-touch [clj-client k {:keys [^int expiry]}]
    (let [^String nk (name k)
          ^int exp (or expiry ^int @default-data-expiry)
          ^OperationFuture fut (.touch cc nk exp)]
      (->CouchbaseCljOperationFuture cf fut)))
  (touch [clj-client k] (touch clj-client k {}))
  (touch [clj-client k {:keys [^long timeout] :as opts}]
    (let [^long to (or timeout (.getOperationTimeout cf))
          ^OperationFuture fut (get-future (async-touch clj-client k opts))]
      (.get fut to TimeUnit/MILLISECONDS)))
  (async-unlock [clj-client k cas-id]
    (async-unlock clj-client k cas-id {}))
  (async-unlock [clj-client k cas-id {:keys [^Transcoder transcoder]}]
    (let [^String nk (name k)
          ^OperationFuture fut (if transcoder
                                 (.asyncUnlock cc nk ^long cas-id transcoder)
                                 (.asyncUnlock cc nk ^long cas-id))]
      (->CouchbaseCljOperationFuture cf fut)))
  (unlock [clj-client k cas-id] (unlock clj-client k cas-id {}))
  (unlock [clj-client k cas-id {:keys [^Transcoder transcoder]}]
    (let [^String nk (name k)]
      (if transcoder
        (.unlock cc nk ^long cas-id transcoder)
        (.unlock cc nk ^long cas-id))))
  (async-get-view [clj-client design-doc view-name]
    (let [^HttpFuture fut (.asyncGetView cc design-doc view-name)]
      (->CouchbaseCljHttpFuture cf fut)))
  (get-view [clj-client design-doc view-name] (.getView cc design-doc view-name))
  (async-get-views [clj-client design-doc]
    (let [^Future fut (.asyncGetViews cc design-doc)]
      (->CouchbaseCljHttpFuture cf fut)))
  (get-views [clj-client design-doc]
    (when-let [rs (.getViews cc design-doc)]
      (seq rs)))
  (async-query [clj-client view q]
    (let [^couchbase_clj.query.CouchbaseCljQuery
          new-q (if (instance? couchbase_clj.query.CouchbaseCljQuery q)
                         q
                         (cbq/create-query q))
          ^HttpFuture fut (.asyncQuery cc view (cbq/get-query new-q))]
      (->CouchbaseCljHttpFuture cf fut)))
  (async-query [clj-client design-doc view-name q]
    (let [^View view (get-view clj-client design-doc view-name)]
      (async-query clj-client view q)))
  (query [clj-client view q]
    (let [^couchbase_clj.query.CouchbaseCljQuery
          new-q (if (instance? couchbase_clj.query.CouchbaseCljQuery q)
                  q
                  (cbq/create-query q))]
      (seq (.query cc view (cbq/get-query new-q)))))
  (query [clj-client design-doc view-name q]
    (let [^View view (get-view clj-client design-doc view-name)]
      (query clj-client view q)))
  (lazy-query [clj-client view q num]
    (let [^couchbase_clj.query.CouchbaseCljQuery
          new-q (if (instance? couchbase_clj.query.CouchbaseCljQuery q)
                  q
                  (cbq/create-query q))]
      (-> (.paginatedQuery cc view (cbq/get-query new-q) num)
          iterator-seq
          lazy-seq)))
  (lazy-query [clj-client design-doc view-name q num]
    (let [^View view (get-view clj-client design-doc view-name)]
      (lazy-query clj-client view q num)))
  (wait-queue [clj-client] (wait-queue clj-client (.getOperationTimeout cf)))
  (wait-queue [clj-client timeout] (.waitForQueues cc timeout TimeUnit/MILLISECONDS))

  ;; TODO: Not working
  ; (observe [clj-client k cas-id]
  ;   (let [^String nk (name k)]
  ;     (.observe cc nk ^long cas-id)))

  ;; TODO: Add observer methods
  ;(add-observer [clj-client conn-obs] (.addObserver cc conn-obs))
  ;(remove-observer [clj-client conn-obs] (.removeObserver cc conn-obs))
  (flush [clj-client] (flush clj-client -1))
  (flush [clj-client delay] (.isSuccess (.getStatus (.flush cc ^int delay))))
  (shutdown [clj-client] (shutdown clj-client -1))
  (shutdown [clj-client timeout] (.shutdown cc timeout TimeUnit/MILLISECONDS)))

(defn create-client
  "Create and return a Couchbase client.
  If no parameters are specified, client will be created from default values specified in
  couchbase-clj.config.

  You can specify keywords parameters: bucket, username, password, uris, client-builder, factory and other opts.
  bucket is the bucket name. Default value is defined as @default-bucket and is \"default\".
  username is the bucket username. Default value is defined as @default-username and is a empty string.
  Currently username is ignored.
  password is the bucket password. Default value is defined as @default-password and is a empty string.
  uris is a Collection of string uris, ex: [\"http://127.0.0.1:8091/pools\"]

  Other options can be specified for CouchbaseConnectionFactoryBuilder object creation.
  Internally, :failure-mode and :hash-alg must have a value and those default values are
  :redistribute and :native-hash respectively.
  All options for CouchbaseConnectionFactoryBuilder can be looked at couchbase-clj.client-builder/method-map Var.

  You can specify the client-builder keyword with the value of CouchbaseCljClientBuilder object
  which is created by couchbase-clj.client-builder/create-client-builder function.
  When doing this, bucket, username, password keywords should be specified.

  By using a factory keyword, you can pass a CouchbaseConnectionFactory object
  which is created by couchbase-clj.client-builder/create-factory function.

  ex:
  (create-client)

  (create-client {:bucket \"default\"
                  :username \"\"
                  :password \"\"
                  :uris [\"http://127.0.0.1:8091/pools\"]})

  (create-client {:auth-descriptor auth-descriptor-object
                  :daemon false
                  :failure-mode :redistribute
                  :hash-alg :native-hash
                  :max-reconnect-delay 30000
                  :min-reconnect-interval 1100
                  :obs-poll-interval 100
                  :obs-poll-max 400
                  :op-queue-max-block-time 10000
                  :op-timeout 10000
                  :read-buffer-size 16384
                  :should-optimize false
                  :timeout-exception-threshold 1000
                  :transcoder (SerializingTranscoder.)
                  :use-nagle-algorithm false})

  (create-client {:client-builder (create-client-builder {:hash-alg :native-hash
                                                          :failure-mode :redistribute
                                                          :max-reconnect-delay 30000})
                  :uris [(URI. \"http://127.0.0.1:8091/pools\")]
                  :bucket \"default\"
                  :username \"\"
                  :password \"\"})

  (create-client {:factory couchbase-connection-factory-object})"
  ([] (create-client {}))
  ([{:keys [client-builder factory] :as opts}]
     (let [cf (cond
               (and client-builder
                    (instance? couchbase_clj.client_builder.CouchbaseCljClientBuilder
                               client-builder))
               (create-factory (-> (assoc opts :factory-builder (get-factory-builder client-builder))
                                   (dissoc :client-builder)))
               (and factory (instance? CouchbaseConnectionFactory factory)) factory
               :else (build opts))]
       (->CouchbaseCljClient (CouchbaseClient. cf) cf))))

(defmacro defclient
  "A macro that defines a Var with Couchbase client specified by a name with or without options.
  See create-client function for detail."
  ([name]
     `(def ~name (create-client)))
  ([name opts]
     `(def ~name (create-client ~opts))))
