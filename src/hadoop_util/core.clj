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

(declare copy-local*)

(defn- copy-file-local
  [^FileSystem fs ^Path path ^String target-local-path ^bytes buffer]
  (with-open [is (.open fs path)
              os (BufferedOutputStream.
                  (FileOutputStream. target-local-path))]
    (loop []
      (let [amt (.read is buffer)]
        (when (> amt 0)
          (.write os buffer 0 amt)
          (recur))))))

(defn copy-dir-local
  [^FileSystem fs ^Path path ^String target-local-path ^bytes buffer]
  (.mkdir (File. target-local-path))
  (let [contents (seq (.listStatus fs path))]
    (doseq [c contents]
      (let [subpath (.getPath c)]
        (copy-local* fs subpath (str-path target-local-path (.getName subpath)) buffer)))))

(defn- copy-local*
  [^FileSystem fs ^Path path ^String target-local-path ^bytes buffer]
  (if (.isDir (.getFileStatus fs path))
    (copy-dir-local fs path target-local-path buffer)
    (copy-file-local fs path target-local-path buffer)))

(defn copy-local
  [^FileSystem fs ^String spath ^String local-path]
  (let [target-file (File. local-path)
        source-name (.getName (Path. spath))
        buffer (byte-array (* 1024 15))
        dest-path (cond
                   (not (.exists target-file)) local-path
                   (.isFile target-file) (throw
                                          (IllegalArgumentException.
                                           (str "File exists " local-path)))
                   (.isDirectory target-file) (str-path local-path source-name)
                   :else (throw
                          (IllegalArgumentException.
                           (str "Unknown error, local file is neither file nor dir "
                                local-path))))]
    (if-not (.exists fs (path spath))
      (throw
       (FileNotFoundException.
        (str "Could not find on remote " spath)))
      (copy-local* fs (path spath) dest-path buffer))))


