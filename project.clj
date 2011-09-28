(defproject hadoop-util "0.2.2-SNAPSHOT"
  :description "Hadoop utilities that we've found useful."
  :dependencies [[org.clojure/clojure "1.2.1"]]
  :dev-dependencies [[swank-clojure "1.4.0-SNAPSHOT"]
                     [clojure-source "1.2.0"]
                     [org.apache.hadoop/hadoop-core "0.20.2-dev"]
                     [lein-marginalia "0.6.0"]
                     [lein-midje "1.0.3"]
                     [midje "1.2.0"]])
