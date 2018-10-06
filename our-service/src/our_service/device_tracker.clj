(ns our-service.device-tracker
  (:require
    [franzy.serialization.deserializers :as deserializers]
    [franzy.serialization.serializers :as serializers]
    [geo [spatial :as spatial]])
  (:gen-class)
  (:import (java.util Properties)
           (org.apache.kafka.streams StreamsConfig KafkaStreams)
           (org.apache.kafka.common.serialization Serde Serdes Serializer)
           (org.apache.kafka.clients.consumer ConsumerConfig)
           (org.apache.kafka.streams StreamsBuilder)
           (org.apache.kafka.streams.state Stores)
           (org.apache.kafka.streams.kstream ValueTransformerWithKeySupplier ValueTransformerWithKey Initializer Aggregator ValueMapper Predicate)))

(defn initializer [v]
  (reify Initializer
    (apply [_]
      v)))

(defmacro aggregator [kv & body]
  `(reify Aggregator
     (apply [_# ~(first kv) ~(second kv) ~(nth kv 2)]
       ~@body)))

(defmacro value-mapper [v & body]
  `(reify ValueMapper
     (apply [_# ~v]
       ~@body)))

(defmacro pred [kv & body]
  `(reify Predicate
     (test [_# ~(first kv) ~(second kv)]
       ~@body)))

(defn value-transformer-with-store [store-name f]
  (reify ValueTransformerWithKeySupplier
    (get [this]
      (let [state-store (volatile! nil)]
        (reify ValueTransformerWithKey
          (init [this context]
            (vreset! state-store (.getStateStore context store-name)))
          (transform [this key value]
            (f @state-store key value))
          (close [this]))))))

;;;
;;; Serialization stuff
;;;

(deftype NotSerializeNil [edn-serializer]
  Serializer
  (configure [_ configs isKey] (.configure edn-serializer configs isKey))
  (serialize [_ topic data]
    (when data (.serialize edn-serializer topic data)))
  (close [_] (.close edn-serializer)))

;; Can be global as they are thread-safe
(def serializer (NotSerializeNil. (serializers/edn-serializer)))
(def deserializer (deserializers/edn-deserializer))

(deftype EdnSerde []
  Serde
  (configure [this map b])
  (close [this])
  (serializer [this]
    serializer)
  (deserializer [this]
    deserializer))

;;;
;;; Application
;;;

(defn kafka-config []
  (doto
    (Properties.)
    (.put StreamsConfig/APPLICATION_ID_CONFIG "device-tracker-consumer")
    (.put StreamsConfig/BOOTSTRAP_SERVERS_CONFIG "kafka1:9092")
    (.put StreamsConfig/COMMIT_INTERVAL_MS_CONFIG 1000)
    (.put StreamsConfig/DEFAULT_KEY_SERDE_CLASS_CONFIG (class (Serdes/String)))
    (.put StreamsConfig/DEFAULT_VALUE_SERDE_CLASS_CONFIG EdnSerde)
    (.put ConsumerConfig/AUTO_OFFSET_RESET_CONFIG "earliest")))

(defn distance [current previous]
  (letfn [(point [{:keys [lat lon]}]
            (spatial/spatial4j-point lat lon))]
    (/ (spatial/distance (point current)
                         (point previous))
       1000)))

(def ^String device-data-topic "device-gsp-coords")

(defn create-kafka-stream-topology-kstream []
  (let [^StreamsBuilder builder (StreamsBuilder.)
        state-name "last-device-state"
        store (Stores/keyValueStoreBuilder
                (Stores/persistentKeyValueStore state-name)
                (EdnSerde.)
                (EdnSerde.))
        _ (-> builder
              (.addStateStore store)
              (.stream device-data-topic)
              (.transformValues (value-transformer-with-store
                                  state-name
                                  (fn [store key current]
                                    (let [previous (.get store key)]
                                      (.put store key current)
                                      (if-not previous
                                        (assoc current :dist 0)
                                        (assoc current :dist (distance current previous))))))
                                (into-array [state-name]))
              (.to "points-with-distance"))]
    builder))

(defn create-kafka-stream-topology-ktable []
  (let [^StreamsBuilder builder (StreamsBuilder.)
        _ (-> builder
              (.stream device-data-topic)
              (.groupByKey)
              (.aggregate (initializer [])
                          (aggregator [k new-point [previous _]]
                            [new-point previous]))
              (.toStream)
              (.mapValues (value-mapper [current previous]
                            (if previous
                              (assoc current :dist (distance current previous))
                              (assoc current :dist 0))))
              (.to "points-with-distance"))]
    builder))

(defn partition-partition
  ([^StreamsBuilder builder n stream-name]
   (partition-partition builder n n stream-name))
  ([^StreamsBuilder builder n step stream-name]
   (let [store-name (str "partition-by-" stream-name "-" n)
         store (Stores/keyValueStoreBuilder
                 (Stores/persistentKeyValueStore store-name)
                 (EdnSerde.)
                 (EdnSerde.))]
     (-> builder
         (.addStateStore store)
         (.stream stream-name)
         (.transformValues
           (value-transformer-with-store
             store-name
             (fn [store key value]
               (let [previous (or (.get store key) [])
                     chunk-so-far (conj previous value)]
                 (if (= n (count chunk-so-far))
                   (do
                     (.put store key (vec (drop step chunk-so-far)))
                     chunk-so-far)
                   (do
                     (.put store key chunk-so-far)
                     nil)))))
           (into-array [store-name]))
         (.filter (pred [k v]
                    (some? v)))))))

(defn create-kafka-stream-topology-using-partition []
  (let [^StreamsBuilder builder (StreamsBuilder.)]
    (-> builder
        (partition-partition 2 1 device-data-topic)
        (.mapValues (reify ValueMapper
                      (apply [this [current previous]]
                        (assoc current :dist (distance current previous)))))
        (.to "points-with-distance"))
    builder))

(defn start-kafka-streams []
  (let [builder (create-kafka-stream-topology-kstream)
        kafka-streams (KafkaStreams. (.build builder) (kafka-config))]
    (.start kafka-streams)
    kafka-streams))