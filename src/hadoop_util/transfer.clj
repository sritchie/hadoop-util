(ns hadoop-util.transfer
  "Namespace responsible for recursively transferring directories from
   a distributed filestore (or another local filestore) to the local
   filesystem on the current machine."
  (:require [hadoop-util.core :as h]
            [clojure.java.io :as io])
  (:import [java.io File FileNotFoundException IOException
            FileOutputStream BufferedOutputStream]
           [org.apache.hadoop.fs FileSystem Path FileStatus FileChecksum]))

;; ## Throttling Agent

(defn check-in
  "Report the current downloaded number of kilobytes to the supplied
  agent."
  [throttle-agent kbs]
  (when throttle-agent
    (letfn [(bump-kbs
              [{:keys [sleep-ms last-check max-kbs kb-pool] :as m} kbs]
              (let [m (update-in m [:kb-pool] + kbs)
                    current-time (System/currentTimeMillis)
                    diff-ms      (if (= current-time last-check)
                                   1
                                   (- current-time last-check))
                    secs         (/ diff-ms 1000)
                    rate         (/ kb-pool secs)]
                (cond
                 (> rate max-kbs) (assoc m
                                    :sleep-ms (rand 1000)
                                    :last-check current-time
                                    :kb-pool 0)
                 (pos? sleep-ms)  (assoc m
                                    :sleep-ms 0)
                 :else m)))]
      (when (pos? (:max-kbs @throttle-agent))
        (send throttle-agent bump-kbs kbs)))))

(defn sleep-interval
  "Returns the current sleep interval specified by the supplied
  throttling agent."
  [throttle-agent]
  (if throttle-agent
    (:sleep-ms @throttle-agent)
    0))

(defn update-limit
  "Updates the throttling agent's rate limit (in kb/s)."
  [throttle-agent new-limit]
  {:pre [(pos? new-limit)]}
  (letfn [(bump [m new-limit]
            (assoc m :max-kbs new-limit))]
    (send throttle-agent bump new-limit)))

(defn throttle
  "Returns a throttling agent. Any positive kb-per-second rate will
  cause throttling; if the rate is zero or negative, downloads will
  proceed without a throttle."
  [kb-per-second]
  {:pre [(or (nil? kb-per-second)
             (not (neg? kb-per-second)))]}
  (agent {:last-check (System/currentTimeMillis)
          :max-kbs    (or kb-per-second 0)
          :kb-pool    0
          :sleep-ms   0}))

(defn try-times*
  "Executes thunk. If an exception is thrown, will retry. At most n retries
  are done. If still some exception is thrown it is bubbled upwards in
  the call chain."
  [n thunk]
  (loop [n n]
    (or (try (thunk)
             (catch Exception e
               (when (zero? n)
                 (throw e))))
        (recur (dec n)))))

(defmacro try-times
  "Executes body. If an exception is thrown, will retry. At most n retries
  are done. If still some exception is thrown it is bubbled upwards in
  the call chain."
  [[n] & body]
  `(try-times* ~n (fn [] ~@body)))

;; ## Recursive Transfer

(defn file-type
  "Accepts a hadoop filesystem object and some path and returns a
  namespace-qualified type keyword."
  [^FileSystem fs ^Path path]
  (cond (.isDirectory fs path) ::directory
        (.isFile fs path)      ::file))

;; Using a multimethod for the file transfer recursion removed the
;; need for a conditional check within the directory copy.

(defmulti copy
  (fn [& [fs path]]
    (file-type fs path)))

(defmethod copy ::file
  [^FileSystem fs ^Path remote-path local-path ^bytes buffer throttle]
  ;; Do we need to delete the partial file that might exist on retry from a read timeout?
  (let [remote-length (-> (.getFileStatus fs remote-path) (.getLen))]
    (with-open [is (.open fs remote-path)
               os (BufferedOutputStream. (FileOutputStream. local-path))]
     (loop [sleep-ms (sleep-interval throttle)]
       (when (pos? sleep-ms)
         (prn "Sleep: " sleep-ms)
         (Thread/sleep sleep-ms))
       (let [amt (.read is buffer)]
         (when (pos? amt)
           (.write os buffer 0 amt)
           (check-in throttle (/ amt 1024))
           (recur (sleep-interval throttle))))))
    (when-not (= remote-length (.length (io/as-file local-path)))
      (throw (IOException. "Local file size not equal to remote file size.")))))

(defmethod copy ::directory
  [^FileSystem fs ^Path remote-path local-path buffer throttle]
  (.mkdirs (io/as-file local-path))
  (doseq [status (.listStatus fs remote-path)]
    (let [remote-subpath (.getPath status)
          local-subpath (h/str-path (str local-path) (.getName remote-subpath))]
      ;; the first entry returned from `listStatus` is the current remote-path
      ;; skip over it to avoid looping infinitely
      (when-not (= remote-subpath remote-path)
        ;; try to copy up to 3 times
        (try-times [3] (copy fs remote-subpath local-subpath buffer throttle))))))

;; TODO: Support transfers between filesystems rather than assuming a
;; local target.

(defn rcopy
  "Copies information at the supplied remote-path over to the supplied
 local-path.

  Arguments are Filesystem, remote shard path, target local path, and
  an optional throttling agent."
  [remote-fs remote-path target-path & {:keys [throttle]}]
  (let [buffer      (byte-array (* 1024 128)) ;; 128k
        remote-path (h/path remote-path)
        source-name (.getName remote-path)
        target-path (io/as-file target-path)
        target-path (cond
                     (not (.exists target-path)) target-path
                     (.isFile target-path)
                     (throw (IllegalArgumentException.
                             (str "File exists: " target-path)))
                     (.isDirectory target-path)
                     (h/str-path (str target-path) source-name)
                     :else
                     (throw (IllegalArgumentException.
                             (format "Unknown error, %s is neither file nor dir."
                                     target-path))))]
    (if (.exists remote-fs remote-path)
      (copy remote-fs remote-path target-path buffer throttle)
      (throw (FileNotFoundException.
              (str "Couldn't find remote path: " remote-path))))))
