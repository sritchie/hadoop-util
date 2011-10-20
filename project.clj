(defproject hadoop-util "0.2.4-SNAPSHOT"
  :description "Hadoop utilities that we've found useful."
  :dependencies [[org.clojure/clojure "1.3.0"]]
  :dev-dependencies [[swank-clojure "1.4.0-SNAPSHOT"]
                     [org.apache.hadoop/hadoop-core "0.20.2-dev"]
                     [lein-marginalia "0.6.1"]
                     [lein-midje "1.0.4"]
                     [midje "1.3-alpha4"]])
