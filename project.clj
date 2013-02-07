(defproject hadoop-util "0.2.9"
  :description "Hadoop utilities that we've found useful."
  :min-lein-version "2.0.0"
  :dependencies [[org.clojure/clojure "1.4.0"]]
  :profiles {:dev
             {:dependencies
              [[org.apache.hadoop/hadoop-core "0.20.2-dev"]
               [midje "1.4.0"]]}})
