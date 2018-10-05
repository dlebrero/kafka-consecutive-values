(defproject our-service "0.1.0-SNAPSHOT"
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [org.apache.kafka/kafka-streams "2.0.0"]
                 [clj-kafka.franzy/core "2.0.7"]
                 [compojure "1.5.1"]
                 [ring/ring-jetty-adapter "1.5.0"]
                 [org.clojure/tools.nrepl "0.2.12"]
                 [org.clojure/tools.logging "0.3.1"]
                 [ch.qos.logback/logback-classic "1.1.7"]
                 [org.slf4j/jcl-over-slf4j "1.7.14"]
                 [org.slf4j/jul-to-slf4j "1.7.14"]
                 [org.slf4j/log4j-over-slf4j "1.7.14"]
                 [factual/geo "2.1.0"]]
  :main our-service.main
  :aot [franzy.serialization.serializers]
  )
