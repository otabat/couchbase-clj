# couchbase-clj

A Clojure client for Couchbase Server 2.x, built on top of [couchbase-java-client](https://github.com/couchbase/couchbase-java-client).

---

This library provides a layer of Clojure that simplifies the compilicated Java interface.

Currently, couchbase-clj is based on couchbase-java-client 1.3.2.

Supports Clojure version >= 1.4.0.


## Installation
---
Add the following dependency to your Leiningen `project.clj` file:

    [couchbase-clj "0.2.0"]

## Documentation
---
[API docs](http://otabat.github.com/couchbase-clj)

## Basic Usage
---
Operations are synchronous and some have an alternative async version using the Future. Only basic usages are shown below. Check the API docs for more information.

### Create a connection 
To create a client connection, specify a bucket name and vector of server URIs.

```clojure
(ns sample
  (:require [couchbase-clj.client :as c]))
  
(c/defclient client {:bucket "default"
                     :uris ["http://127.0.0.1:8091/pools"]})
                     
;; By using the default values, below will be the same.
;; Default values are defined in couchbase-clj.config.
(c/defclient client)
  
;; You can specify options to the client
;; See API Docs for more information.
(c/defclient client {:bucket "default"
                     :uris ["http://127.0.0.1:8091/pools"]
                     :op-timeout 10000
                     :op-queue-max-block-time 20000})

;; Shutdown the client
;; To avoid the client creation overhead, you don't have to do this per operation.
;; In general, one client per application is enough.
(c/shutdown client)
```

### Write
#### Sync write

```clojure
(ns sample
  (:require [couchbase-clj.client :as c]
            [couchbase-clj.util :as u])) 
           
(c/defclient client)

;; Add a single key/value pair if the key does not already exist.
(c/add client :key 1)
;=> true

;; Key already exists.
(c/add client :key 1)
;=> false

;; Add a JSON string value.
(c/add client :key 1 (u/json-str {:a 1}))
;=> true

;; Same as above, but the value can be a Clojure collection.
(c/add-json client :key2 {:b 2})
;=> true

;; Replace a single key/value pair if the key exists.
(c/replace client :key 2)
;=> true

;; Replace a single key/value pair if the key exists. 
(c/replace-json client :key1 [1 2])
;=> true

;; Set a single key/value pair.
;; If the key does not already exist, it will add the value. 
(c/set client :key3 3)
;=> true

;; If the key exists, it will replace the value.
(c/set client :key4 4)
;=> true    

;; Set a Clojure collection data that will be converted to a JSON string value.
(c/set-json client :key1 {:a 1})
;=> true

;; Set a single key/value pair by using the CAS method (Check And Set)
;; First get the current CAS ID.
(def cas-id (c/get-cas-id client :key))
;=> #'sample/cas-id

;; Then set the value using CAS.
;; Results will be returned in a keyword.
;; See API Docs for more information.
(c/set-cas client :key "1" cas-id)
;=> :ok

;; Set a Clojure collection data that will be converted to a JSON string value using CAS.
(c/set-cas-json client :key1 {:a 1} (c/get-cas-id client :key1))
;=> :ok

;; Append a value to an existing key/value pair.
(c/append client :key ",2" (c/get-cas-id client :key))
;=> true

;; (c/get client :key)
;=> "1,2"

;; Prepend a value to an existing key/value pair
(c/prepend client :key "0," (c/get-cas-id client :key))
;=> true

;; (c/get client :key)
;=> "0,1,2"

;; Increment a value
(c/inc client :inc)
;=> 0

(c/inc client :inc)
;=> 1

;; Decrement a value
(c/dec client :inc)
;=> 0
```

#### Async write

```clojure
(ns sample
  (:require [couchbase-clj.client :as c]
            [couchbase-clj.util :as u])) 
           
(c/defclient client)

;; Add a single key/value pair if the key does not already exist.
(def fut (c/async-add client :key1 1))
;=> #'sample/fut

fut
;=> #<CouchbaseCljOperationFuture@28768bdb: :pending>

;; Get the result value of the Future.
;; This is a blocking operation.
@fut
;=> true

;; Add a Clojure collection data that will be converted to a JSON string value.
@(c/async-add-json client :key2 {:b 2})
;=> true

;; Replace a single key/value pair if the key exists.
@(c/async-replace client :key1 2)
;=> true

;; Replace a single key/value pair if the key exists. 
@(c/async-replace-json client :key1 [1 2])
;=> true

;; Set a single key/value pair.
;; If the key does not already exist, it will add the value. 
@(c/async-set client :key3 3)
;=> true

;; If the key exists, it will replace the value.
@(c/async-set client :key4 4)
;=> true	

;; Set a Clojure collection data that will be converted to a JSON string value.
@(c/async-set-json client :key1 {:a 1})
;=> true

;; Set a single key/value pair by using the CAS method (Check And Set)
;; First get the current CAS ID.
(def cas-id (c/get-cas-id client :key))
;=> #'sample/cas-id

;; Then set the value using CAS.
;; Results will be returned in a keyword.
;; See API Docs for more information.
@(c/async-set-cas client :key "1" cas-id)
;=> :ok

;; Set a Clojure collection data that will be converted to a JSON string value using CAS.
@(c/async-set-cas-json client :key1 {:a 1} (c/get-cas-id client :key1))
;=> :ok

;; Append a value to an existing key/value pair.
@(c/async-append client :key ",2" (c/get-cas-id client :key))
;=> true
    
;; Prepend a value to an existing key/value pair
@(c/async-prepend client :key "0," (c/get-cas-id client :key))
;=> true

;; Increment a value
;; First create the default value (it must be a string value).
@(c/async-add client :inc "1")
;=> true

;; Increment with a offset option
@(c/async-inc client :inc {:offset 5})
;=> 6
    
;; Decrement a value
@(c/async-dec client :inc)
;=> 5
```

### Read
#### Sync read

```clojure
(ns sample
  (:require [couchbase-clj.client :as c]))

(c/defclient client)

;; Get a single key/value pair.
;; All keys can be keywords, strings or symbols.
(c/get client :key1)
;=> "{\"a\":1}"

;; Specify a key and get a JSON string value that is converted to a Clojure data.
(c/get-json client :key1)
;=> {:a 1}

;; Get multiple key/value pairs.
(c/get-multi client [:key1 :key2])
;=> {"key2" "{\"b\":2}", "key1" "{\"a\":1}"} 
    
;; Specify keys and get the JSON string values (with keys) that are converted to Clojure data.
(c/get-multi-json client [:key1 :key2])
;=> {"key1" {:a 1}, "key2" {:b 2}}

;; Get a single key value with CAS value.
(c/get-cas client :key1)
;=> #<CASValue {CasValue 164987457502467/{"a":1}}>

;; Get the CAS ID.
(c/cas-id (c/get-cas client :key1))
;=> 164987457502467 

;; Get CAS ID in one command.
(c/get-cas-id client :key1)
;=> 164987457502467
```

#### Async read

```clojure
(ns sample
  (:require [couchbase-clj.client :as c]
            [couchbase-clj.future :as f]))

(c/defclient client)

(def fut (c/async-get client :key1))
;=> #'sample/fut

;; Get the result value of the Future.
@fut
;=>	"{\"a\":1}"

;; You can deref JSON string values to a Clojure data.
(f/deref-json fut)
;=> {:a 1}

@(c/async-get-multi client [:key1 :key2])
;=> {"key2" "{\"b\":2}", "key1" "{\"a\":1}"}

(f/deref-json (c/async-get-multi client [:key1 :key2]))
;=> {"key1" {:a 1}, "key2" {:b 2}}

@(c/async-get-cas client :key1)
;=> #<CASValue {CasValue 164987457502467/{"a":1}}>

;; Get the value 
(c/cas-val @(c/async-get-cas client :key1))
;=> "{\"a\":1}"
```

### Deletion
#### Sync deletion

```clojure
(ns sample
  (:require [couchbase-clj.client :as c]))
  
(c/defclient client)

(c/delete client :key1)
;=> true
```

#### Aync deletion

```clojure
(ns sample
  (:require [couchbase-clj.client :as c]))

(c/defclient client)

@(c/async-delete client :key1)
;=> true 
```

### Update expiry
#### Sync expiry update

```clojure
(ns sample
  (:require [couchbase-clj.client :as c]))

(c/defclient client)
  
;; Default expiry is defined in couchbase-clj.config/default-data-expiry as -1 (no expiry).
(c/touch client :key1)
;=> true

;; You can change the expiry (in seconds).
(c/touch client :key1 {:expiry 5})
;=> true
    
;; After 5 seconds 
(c/get client :key1)
;=> nil
```

#### Async expiry update

```clojure
(ns sample
  (:require [couchbase-clj.client :as c]))

(c/defclient client)
  
@(c/async-touch client :key1)
;=> true

@(c/async-touch client :key1 {:expiry 5})
;=> true
```

### Locking
#### Sync locking

```clojure
(ns sample
  (:require [couchbase-clj.client :as c]))

(c/defclient client)
  
;; Get a lock.
;; Default expiry is defined in couchbase-clj.config/default-lock-expiry as 15 seconds.
(def cas (c/get-lock client :key1))
;=> #'sample/cas

(c/locked? client :key1)
;=> true

;; Unlock
;; Specify the key and CAS ID.
(c/unlock client :key1 (c/cas-id cas))
;=> true

(c/locked? client :key1)
;=> false
```

#### Async locking

```clojure
(ns sample
  (:require [couchbase-clj.client :as c]))

(c/defclient client)
  
;;Get a lock.
(def cas @(c/async-get-lock client :key1))
;=> #'sample/cas

(c/locked? client :key1)
;=> true

;; Unlock
@(c/async-unlock client :key1 (c/cas-id cas))
;=> true

(c/locked? client :key1)
;=> false
```

### View querying
#### Sync query

```clojure
(ns sample
  (:require [couchbase-clj.client :as c]
            [couchbase-clj.query :as q]))

(c/defclient client)
  
;; Get the view object.
;; Specify the design document name and view name.
;; 
;; Perhaps view may look like this.
;; function (doc, meta) {
;;   if (meta.type == "json") {
;;     emit(meta.id, doc);
;;   }
;; }
;;
(def view (c/get-view client "dev_doc" "view"))
;=> #'sample/view

;; Create the query object.
;; You can specify options as a key/value map
;; or using a set-xxx corresponding function in couchbase-clj.query
;; See API Docs for more information.
(q/defquery query {:limit 1
                   :include-docs true})
;=> #'sample/query

;; Get the query results.
(c/query client view query)
;=> (#<ViewRowWithDocs com.couchbase.client.protocol.views.ViewRowWithDocs@1f0715ce>)

;; Get the ID, key, value, and doc (if include-docs is true) from query results.
(map #(hash-map :id (c/view-id %)
                :key (c/view-key %)
                :val (c/view-val %)
                :doc (c/view-doc %))
     (c/query client view query))
;=> ({:key "key1", :doc "{\"a\":1}", :val "{\"a\":1}", :id "key1"})
```
Or simply call

```clojure
(c/query client "dev_doc" "view" {:limit 2 :include-docs true})
;=> (#<ViewRowWithDocs com.couchbase.client.protocol.views.ViewRowWithDocs@1f0715ce>
;       #<ViewRowWithDocs com.couchbase.client.protocol.views.ViewRowWithDocs@a28974c>)

;; Get JSON string documents from query results that are converted to Clojure data.
(map c/view-doc-json (c/query client view query))
;=> ({:a 1} {:b 2})
```

#### Async query

```clojure
(ns sample
  (:require [couchbase-clj.client :as c]))

(c/defclient client)

(map #(hash-map :id (c/view-id %))
  @(c/async-query client "dev_doc" "view" {:limit 1}))
;=> ({:id "key1"})
```

#### Lazy query
Lazy query can be used to get the amount of documents specified per iteration. 
Communication between the client and Couchbase server will only occur per iteration.
This is typically used to get a large data lazily.


```clojure
(ns sample
  (:require [couchbase-clj.client :as c]))

(c/defclient client)

;; Get a lazy sequence of the query results.
;; Specify the design document name, view name, query options,
;; and the amount of documents to get in each iterations (minimum value is 15).
;; Be sure not to hold the head of the sequence.
    
(doseq [r (c/lazy-query client "dev_doc" "view" {:include-docs true} 15)]
  (println (map c/view-id r)))
;; (doc-id1 doc-id2 doc-id3 doc-id4 doc-id5 ...)
;; (doc-id16 doc-id17 doc-id18 doc-id19 doc-id20 ...)
;=> nil
```

## Change Log
* Release 0.2.0 on 2014-07-09
  * Fixes #6 to properly handle multiple nodes.
  * Renamed client-builder APIs which mutate.
  * Renamed query APIs which mutate.
  * Deleted assoc for cleaner API.
  * Added Clojure 1.6 dependency.
* Release 0.1.3 on 2014-02-11
  * Updated couchbase-java-client version to 1.3.2.
* Release 0.1.2 on 2013-04-07
  * Changed range query API, so that it can take an arbitrary Clojure data structure.
  * Added `view-key-json`. 
  * Renamed `json-str` to `write-json`.
* Release 0.1.1 on 2013-04-05
  * Updated couchbase-java-client version to 1.1.5.
* Release 0.1.0 on 2012-10-08
  * Initial release 

## License
---

Copyright © 2012-2014 Tatsuya Tsuda

Distributed under the Eclipse Public License, the same as Clojure.
