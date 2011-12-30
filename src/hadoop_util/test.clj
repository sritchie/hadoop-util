(ns hadoop-util.test
  (:require [hadoop-util.core :as h])
  (:import [java.util UUID]))

(defn uuid []
  (str (UUID/randomUUID)))

(defmacro with-fs-tmp
  [[fs-sym & tmp-syms] & body]
  (let [tmp-paths (mapcat (fn [t]
                            [t `(str "/tmp/unittests/" (uuid))])
                          tmp-syms)]
    `(let [~fs-sym (h/filesystem)
           ~@tmp-paths]
       (.mkdirs ~fs-sym (h/path "/tmp/unittests"))
       (try ~@body
            (finally
             (delete-all ~fs-sym [~@tmp-syms]))))))

(defmacro def-fs-test
  [name fs-args & body]
  `(deftest ~name
     (with-fs-tmp ~fs-args
       ~@body)))

(defn local-temp-path []
  (str (System/getProperty "java.io.tmpdir") "/" (uuid)))

(defmacro with-local-tmp
  [[fs-sym & tmp-syms] & [kw & more :as body]]
  (let [[log-lev body] (if (keyword? kw)
                         [kw more]
                         [:warn body])
        tmp-paths (mapcat (fn [t]
                            [t `(local-temp-path)])
                          tmp-syms)]
    `(log/with-log-level ~log-lev
       (let [~fs-sym (h/local-filesystem)
             ~@tmp-paths]
         (try ~@body
              (finally
               (delete-all ~fs-sym [~@tmp-syms])))))))

(defmacro def-local-fs-test [name local-args & body]
  `(deftest ~name
     (with-local-tmp ~local-args
       ~@body)))
