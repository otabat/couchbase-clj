(ns couchbase-clj.config)

(def
  ^{:doc "Default bucket name.
  You can change the value by using swap! or reset!.
  Default value is set to \"default\"."}
  default-bucket (atom "default"))
(def
  ^{:doc "Default bucket username.
  You can change the value by using swap! or reset!.
  Default value is set to \"\"."}
  default-username (atom ""))
(def
  ^{:doc "Default bucket password.
  You can change the value by using swap! or reset!.
  Default value is set to \"\"."}
  default-password (atom ""))
(def
  ^{:doc "Default URI sequence collection of one or more servers from the cluster.
  You can change the value by using swap! or reset!.
  Default value is set to [\"http://127.0.0.1:8091/pools\"]."}
  default-uris (atom ["http://127.0.0.1:8091/pools"]))
(def
  ^{:doc "Default expiry for the data (in seconds).
  You can change the value by using swap! or reset!.
  Default value is set to -1."}
  default-data-expiry (atom -1))
(def
  ^{:doc "Default expiry for locking the key (in seconds).
  You can change the value by using swap! or reset!.
  Default value is set to 15."}
  default-lock-expiry (atom 15))
(def
  ^{:doc "Default offset for incrementing the data.
  You can change the value by using swap! or reset!.
  Default value is set to 1."}
  default-inc-offset (atom 1))
(def default-inc-default
  ^{:doc "Default base value for incrementing the data.
  You can change the value by using swap! or reset!.
  Default value is set to 0."}
  (atom 0))
(def default-dec-offset
  ^{:doc "Default offset for Decrementing the data.
  You can change the value by using swap! or reset!.
  Default value is set to 1."}
  (atom 1))
(def default-dec-default
  ^{:doc "Default base value for Decrementing the data.
  You can change the value by using swap! or reset!.
  Default value is set to 0."}
  (atom 0))
(def default-persist
  ^{:doc "Default value for the persistent type.
  You can change the value by using swap! or reset!.
  Default value is set to :master."}
  (atom :master))
(def default-replicate
  ^{:doc "Default value for the replication type.
  You can change the value by using swap! or reset!.
  Default value is set to :zero."}
  (atom :zero))
