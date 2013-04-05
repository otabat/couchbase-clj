(ns couchbase-clj.test.fixture
  (:import [java.net URI])
  (:refer-clojure :exclude [flush])
  (:require [couchbase-clj.client :as cb-client]))

(def admin-username "Administrator")
(def admin-password "pwpwpw")
(def bucket "couchbase_clj_test")
(def bucket-type "couchbase")
(def bucket-ram-size 256)
(def bucket-replica 0)
(def host "127.0.0.1")
(def admin-port 8091)
(def op-port 11210)
(def uris ["http://127.0.0.1:8091/pools"])
(def bucket-username "")
(def bucket-password "")
(def design-doc "dev_doc")
(def view "view")
(def view2 "view2")
(declare ^:dynamic *client*)

(defn exec-cmd
  [cmd]
  (.waitFor (.. Runtime getRuntime (exec cmd))))

(defn get-client
  []
  *client*)

(defn- create-bucket-cmd
  [{:keys [host port username password bucket type ram-size replica]}]
  (let [cmd (format (str "couchbase-cli bucket-create -c %s:%d"
                         " --user=%s"
                         " --password=%s"
                         " --bucket=%s"
                         " --bucket-type=%s"
                         " --bucket-ramsize=%d"
                         " --bucket-replica=%d")
                    
                    host port username password bucket type ram-size replica)]
    cmd))

(defn delete-bucket-cmd
  [{:keys [host port username password bucket]}]
  (let [cmd (format (str "couchbase-cli bucket-delete -c %s:%d"
                         " --user=%s"
                         " --password=%s"
                         " --bucket=%s")
                    host port username password bucket)]
    cmd))

(defn create-bucket
  []
  (let [cmd (create-bucket-cmd {:host host
                                :port admin-port
                                :username admin-username
                                :password admin-password
                                :bucket bucket
                                :type bucket-type
                                :ram-size bucket-ram-size
                                :replica bucket-replica})]
    (exec-cmd cmd)))

(defn delete-bucket
 []
 (let [cmd (delete-bucket-cmd {:host host
                               :port admin-port
                               :username admin-username
                               :password admin-password
                               :bucket bucket})]
   (exec-cmd cmd)))

(defn create-client
  []
  (cb-client/create-client {:bucket bucket
                            :username bucket-username
                            :password bucket-password
                            :uris ["http://127.0.0.1:8091/pools"]}))

;(defn set-flush-all
;  [enabled]
;  (let [cmd (format "cbepctl %s:%s set flush_param flushall_enabled %s"
;                    host op-port enabled)]
;    (exec-cmd cmd)))
;
;(defn enable-flushing
;  []
;  (set-flush-all true))
;
;(defn disable-flushing
;  []
;  (set-flush-all false))
;
;(defn flush
;  []
;  (enable-flushing)
;  (cb-client/flush (get-client))
;  (disable-flushing))
;
;(defn flush-data
;  [f]
;  (flush)
;  (f))

(defn setup-bucket
  [f]
  (try
    (create-bucket)
    (Thread/sleep 3000)
    (f)
    (delete-bucket)
    (catch Exception e
      (println e)
      (println "Exception. Deleting bucket.")
      (delete-bucket))))

(defn setup-client
  [f]
  (binding [*client* (create-client)]
    (try
      (f)
      (cb-client/shutdown *client*)
      (catch Exception e
        (println e)
        (println "Exception. Shutdowning client.")
        (cb-client/shutdown *client*)))))
