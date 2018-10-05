(ns our-service.fake-producer
  (:require
    [franzy.serialization.serializers :as serializers]
    [franzy.clients.producer.client :as client]
    [franzy.clients.producer.protocols :as producer]
    [compojure.core :refer [routes ANY GET POST]]
    [clojure.tools.logging :as log])
  (:use ring.middleware.params))

(defn for-ever
  [thunk]
  (loop []
    (if-let [result (try
                      [(thunk)]
                      (catch Exception e
                        (println e)
                        (Thread/sleep 100)))]
      (result 0)
      (recur))))

(def kafka-client (delay
                    (client/make-producer {:bootstrap.servers "kafka1:9092"
                                           :acks              "all"
                                           :retries           1
                                           :client.id         "example-producer"}
                                          (serializers/string-serializer)
                                          (serializers/edn-serializer))))

(defn produce-edn [m]
  (let [value (assoc-in m [:value :ts] (System/currentTimeMillis))]
    (log/info "Producing" value)
    (for-ever
      #(producer/send-async! @kafka-client value))))

(defn send-command [command-key command]
  (produce-edn {:topic "device-gsp-coords"
                :key   command-key
                :value command}))

(defn api []
  (routes
    (POST "/device-point" [device lat lon]
      (send-command device {:lat (Double/parseDouble lat)
                            :lon (Double/parseDouble lon)})
      {:status 200
       :body   (pr-str "done!")})))

(comment

  (dotimes [i 179]
    (send-command "device1" {:lat 23 :lon i :i i}))
  (send-command "device1" {:lat 23 :lon 1})
  (send-command "device1" {:lat 23 :lon 2})

  )
