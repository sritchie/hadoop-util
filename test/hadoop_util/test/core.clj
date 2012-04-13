(ns hadoop-util.test.core
  (:use [hadoop-util.core])
  (:use [clojure.test]))

(deftest test-mk-qualified-path-string
  "test that an unqualified string gets qualified properly"
  (is (= "file:/tmp/test" (.toString (mk-qualified-path "/tmp/test")))))

(deftest test-mk-qualified-path-path
  "test that an unqualified Path gets qualified"
  (is (= "file:/tmp/test" (.toString (mk-qualified-path (path "/tmp/test"))))))

(deftest test-mk-qualified-path-string-qualified
  "test that an qualified string passes through properly"
  (is (= "s3n://abucket/tmp/test" (.toString (mk-qualified-path "s3n://abucket/tmp/test")))))

(deftest test-mk-qualified-path-path-qualified
  "test that an qualified Path passes through properly"
  (is (= "s3n://abucket/tmp/test" (.toString (mk-qualified-path (path "s3n://abucket/tmp/test"))))))

