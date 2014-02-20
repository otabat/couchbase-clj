(ns couchbase-clj.query
  (:import [java.lang UnsupportedOperationException]
           [com.couchbase.client.protocol.views Query Stale OnError])
  (:refer-clojure :exclude [assoc assoc! get-method str])
  (:require [couchbase-clj.config :as cb-config]
            [couchbase-clj.util :as cb-util]))

(declare dispatch)
(declare create-query)

(defn- stale
  [k]
  (cond (= k :ok) Stale/OK
        (= k :false) Stale/FALSE
        (= k :update-after) Stale/UPDATE_AFTER
        (= k :true) Stale/OK
        (true? k) Stale/OK
        (false? k) Stale/FALSE
        :else Stale/UPDATE_AFTER))

(defn- on-error
  [k]
  (if (= k :stop) OnError/STOP OnError/CONTINUE))

(defprotocol ICouchbaseCljQuery
  (get-query [clj-query] "Get the Query object.")
  (reduce? [clj-query] "Returns true if the reduce function will be used.")
  (include-docs? [clj-query]
    "Returns true if the original JSON docuemnt will be included.")
  (set-include-docs [clj-query b]
    "Set true to include the original JSON document.")
  (set-desc [clj-query b]
    "Set true to retrieve the results in descending order.")
  (set-startkey-doc-id [clj-query doc-id]
    "Set the document ID to start at. doc-id can be a keyword or a string.")
  (set-endkey-doc-id [clj-query doc-id]
    "Set the document ID to end at. doc-id can be a keyword or a string.")
  (set-group [clj-query b] "Set true to reduce to a set of distinct keys.")
  (set-group-level [clj-query group-level]
    "Set the group-level as an integer to specify how many items of the keys
  are used in grouping.")
  (set-inclusive-end [clj-query b]
    "Set true to include the endkey in the result.")
  (set-key [clj-query x]
    "Set the key to fetch only the rows with the given keyword, which data
  will be converted to a JSON string value.

  ex1: (set-key 1000)
  ex2: (set-key :a)
  ex3: (set-key \"a\")
  ex4: (set-key [1 2])")
  (set-limit [clj-query limit] "Set the integer limit to specify
  the maximum number of rows to return.")
  (set-range [clj-query coll]
    "Set both startkey and endkey in a sequential collection.
  Both start-key and end-key will be converted to a JSON string.

  ex1: (set-range-start [1000 2000])
  ex2: (set-range-start [:a :b])
  ex3: (set-range-start [\"a\" \"b\"])
  ex4: (set-range-start [[1 2] [3 4])")
  (set-range-start [clj-query x]
    "Set the start key, which the data will be converted to a JSON string value.

  ex1: (set-range-start 1000)
  ex2: (set-range-start :a)
  ex3: (set-range-start \"a\")
  ex4: (set-range-start [1 2])")
  (set-range-end [clj-query x]
    "Set the end key, which the data will be converted to a JSON string value.

  ex1: (set-range-end 2000)
  ex2: (set-range-end :b)
  ex3: (set-range-end \"b\")
  ex4: (set-range-end [3 4])")
  (set-reduce [clj-query b] "Set true to use the reduce function of the view.")
  (set-skip [clj-query docs-to-skip] "Set the number of documents
  to skip as an integer.")
  (set-stale [clj-query stl]
    "Set the stale option as a keyword.
  When stale is equal to:
    :ok, :true, or boolean true then the stale will be ok.
    Index will not be updated.

    :false or boolean false then the stale will be false.
    Index will be updated before the query is executed.

    :update-after or other value then the stale will be update_after.
    Index will be updated after results are returned.")
  (set-on-error [clj-query oe]
    "Set the option to decide when an error occurs.
  When oe is equal to:
    :stop
    Stop processing the query when an error occurs and populate the
    errors response with details.

    :continue or other values (default value)
    Continue processing the query even if errors occur, populating the errors
    response at the end of the query response.")
  (assoc [clj-query m]
    "Return a updated copy of the query.")
  (assoc! [clj-query m]
    "Update the query, after created by create-query or defquery.

  ex:
  (defquery query {:include-docs true
                   :group true})

  (assoc! query {:on-error :stop
                 :group-level 2
                 :limit 100})")
  (str [clj-query]
    "Print the query created, in the form of url query parameters."))

(deftype CouchbaseCljQuery [^Query q opts]
  ICouchbaseCljQuery
  (get-query [clj-query] q)
  (reduce? [clj-query] (.willReduce q))
  (include-docs? [clj-query] (.willIncludeDocs q))
  (set-include-docs [clj-query b] (.setIncludeDocs q b))
  (set-desc [clj-query b] (.setDescending q b))
  (set-startkey-doc-id [clj-query doc-id] (.setStartkeyDocID q (name doc-id)))
  (set-endkey-doc-id [clj-query doc-id] (.setEndkeyDocID q (name doc-id)))
  (set-group [clj-query b] (.setGroup q b))
  (set-group-level [clj-query group-level] (.setGroupLevel q group-level))
  (set-inclusive-end [clj-query b] (.setInclusiveEnd q b))
  (set-key [clj-query x] (.setKey q ^String (cb-util/write-json x)))
  (set-limit [clj-query limit] (.setLimit q limit))
  (set-range [clj-query coll]
    (.setRange q
               ^String (cb-util/write-json (first coll))
               ^String (cb-util/write-json (second coll))))
  (set-range-start [clj-query x]
    (.setRangeStart q ^String (cb-util/write-json x)))
  (set-range-end [clj-query x]
    (.setRangeEnd q ^String (cb-util/write-json x)))
  (set-reduce [clj-query b] (.setReduce q b))
  (set-skip [clj-query docs-to-skip] (.setSkip q docs-to-skip))
  (set-stale [clj-query stl] (.setStale q (stale stl)))
  (set-on-error [clj-query oe] (.setOnError q (on-error oe)))
  (assoc [clj-query m]
    (create-query (merge opts m)))
  (assoc! [clj-query m]
    (doseq [kv m]
      (dispatch clj-query kv)))
  (str [clj-query] (.toString q)))

(def
  ^{:doc "A key/value conversion map of query options
  to corresponding set functions."}
  method-map
  {:desc set-desc
   :startkey-doc-id set-startkey-doc-id
   :endkey-doc-id set-endkey-doc-id
   :group set-group
   :group-level set-group-level
   :include-docs set-include-docs
   :inclusive-end set-inclusive-end
   :key set-key
   :limit set-limit
   :range set-range
   :range-start set-range-start
   :range-end set-range-end
   :reduce set-reduce
   :skip set-skip
   :stale set-stale
   :on-error set-on-error})

(defn- dispatch
  [query kv]
  (let [k (key kv)
        v (val kv)]
    (if-let [method (k method-map)]
      (method query v)
      (throw (java.lang.UnsupportedOperationException.
              (format "Wrong keyword %s specified for a query." k))))))

(defn create-query
  "Create and return a view query.
  Specify key/value arguments as a map.  

  ex:
  (create-query {:include-docs true
                 :desc false
                 :startkey-doc-id :doc1
                 :endkey-doc-id :doc2
                 :group true
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
                 :on-error :continue})"
  [m]
  (let [query (->CouchbaseCljQuery (Query.) m)]
    (doseq [kv m]
      (dispatch query kv))
    query))

(defmacro defquery
  "A macro that creates Var of a view query.
  Specify the name of the Var and key/value arguments as a map.  

  ex:
  (defquery query {:include-docs true
                   :desc false
                   :startkey-doc-id :doc1
                   :endkey-doc-id :doc2
                   :group true
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
                   :on-error :continue})"
  [name m]
  `(def ~name (create-query ~m)))
