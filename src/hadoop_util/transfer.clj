(ns hadoop-util.transfer
  (:import [java.io File FileNotFoundException FileOutputStream BufferedOutputStream]
           [org.apache.hadoop.fs FileSystem Path]
           [org.apache.hadoop.conf Configuration]
           [org.apache.hadoop.mapred JobConf]))

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


