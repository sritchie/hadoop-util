(ns hadoop-util.test
  (:require [hadoop-util.core :as h])
  (:import [java.util UUID]
           [org.apache.hadoop.fs FileSystem]))

(defn uuid []
  (str (UUID/randomUUID)))

(defn delete-all [^FileSystem fs paths]
  (dorun
   (for [t paths]
     (.delete fs (h/path t) true))))

(defn with-fs-tmp* [n afn]
  (let [prefix    "/tmp/unittests"
        fs        (h/filesystem)
        tmp-paths (->> (repeatedly #(str prefix (uuid)))
                       (take n))]
    (.mkdirs fs (h/path prefix))
    (try (apply afn fs tmp-paths)
         (finally
          (delete-all fs tmp-paths)))))

(defmacro with-fs-tmp
  [[fs-sym & tmp-syms] & body]
  `(with-fs-tmp* ~(count tmp-syms)
     (fn [~fs-sym ~@tmp-syms]
       ~@body)))

(defn local-temp-path []
  (-> (System/getProperty "java.io.tmpdir")
      (str "/" (uuid))))

(defn with-local-tmp* [n afn]
  (let [tmp-paths (take n (repeatedly local-temp-path))
        fs        (h/local-filesystem)]
    (try (apply afn fs tmp-paths)
         (finally
          (delete-all fs tmp-paths)))))

(defmacro with-local-tmp
  [[fs-sym & tmp-syms] & body]
  `(with-local-tmp* ~(count tmp-syms)
     (fn [~fs-sym ~@tmp-syms]
       ~@body)))

(defmacro def-fs-test
  [name fs-args & body]
  `(deftest ~name
     (with-fs-tmp ~fs-args
       ~@body)))

(defmacro def-local-fs-test [name local-args & body]
  `(deftest ~name
     (with-local-tmp ~local-args
       ~@body)))
