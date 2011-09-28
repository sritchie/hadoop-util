(ns hadoop-util.core
  (:import [org.apache.hadoop.fs FileSystem Path]
           [org.apache.hadoop.conf Configuration]))

(defmulti conf-set (comp class :value))

(defmethod conf-set String
  [{key :key value :value conf :conf}]
  (.set conf key value))

(defmethod conf-set Integer
  [{key :key value :value conf :conf}]
  (.setInt conf key value))

(defmethod conf-set Float
  [{key :key value :value conf :conf}]
  (.setFloat conf key value))

(defn path [str]
  (Path. str))

(defn populate-hadoop-config
  [conf prop-map]
  (doseq [[k v] prop-map]
    (conf-set {:key k
               :value v
               :conf conf})))

(defn configuration [conf-map]
  (doto (Configuration.)
    (populate-hadoop-config conf-map)))

(defn filesystem
  ([] (filesystem {}))
  ([conf-map]
     (FileSystem/get (configuration conf-map))))

(defn mkdirs [fs base-path]
  (.mkdirs fs (path base-path)))

(defn delete
  ([fs path] (delete fs path false))
  ([fs path rec]
     (.delete fs (Path. path) rec)))

(defn local-filesystem []
  (FileSystem/getLocal (Configuration.)))
