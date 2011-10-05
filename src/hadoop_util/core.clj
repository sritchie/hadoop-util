(ns hadoop-util.core
  (:import [java.io File FileNotFoundException FileOutputStream BufferedOutputStream]
           [org.apache.hadoop.fs FileSystem Path]
           [org.apache.hadoop.conf Configuration]
           [org.apache.hadoop.mapred JobConf]))

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

(defn path
  ([str-or-path]
     (if (instance? Path str-or-path)
       str-or-path
       (Path. str-or-path)))
  ([parent child]
     (Path. parent child)))

(defn str-path
  ([part1] part1)
  ([part1 part2 & components]
     (apply str-path (str (path part1 (str part2))) components)))

(defn populate-hadoop-config
  [conf prop-map]
  (doseq [[k v] prop-map]
    (conf-set {:key k
               :value v
               :conf conf})))

(defn configuration [conf-map]
  (doto (Configuration.)
    (populate-hadoop-config conf-map)))

(defn job-conf [conf-map]
  (doto (JobConf.)
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

(defn clear-dir [fs path]
  (delete fs path true)
  (mkdirs fs path))

(defn local-filesystem []
  (FileSystem/getLocal (Configuration.)))

(defn mk-local-path [local-dir]
  (.pathToFile (local-filesystem)
               (path local-dir)))
