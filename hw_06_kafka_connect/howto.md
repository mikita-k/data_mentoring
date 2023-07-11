# setup env (WSL)
- install JDK
```shell
java -version
sudo apt-get update
sudo apt install default-jdk
java -version
```
- grant permissions
```shell
sudo chmod a+rwx /home
```
- download **kafka** to /home directory.
- get latest version https://www.apache.org/dyn/closer.cgi?path=/kafka/3.5.0/kafka_2.13-3.5.0.tgz
- unzip
```shell
cd /home
wget <url>
tar -xzf kafka_2.13-3.5.0.tgz
```
- unzip
```commandline
cd kafka_2.13-3.5.0
```


---
# Example from kafka quickstart:

---
## start Kafka
- open 3 terminals
```shell
   bin/zookeeper-server-start.sh config/zookeeper.properties 
   bin/kafka-server-start.sh config/server.properties
   <working terminal>
```

## prepare topics
```shell
# input topic
bin/kafka-topics.sh --create \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 1 \
    --topic streams-plaintext-input

# prepare output topic
bin/kafka-topics.sh --create \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 1 \
    --topic streams-wordcount-output \
    --config cleanup.policy=compact

# describe topics
bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe
```

## run Kafka Stream
```shell
# start Wordcount Application
bin/kafka-run-class.sh org.apache.kafka.streams.examples.wordcount.WordCountDemo

# start the console producer to input some data into input topic
bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic streams-plaintext-input

# start consumer to read data from output topic
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic streams-wordcount-output \
    --from-beginning \
    --formatter kafka.tools.DefaultMessageFormatter \
    --property print.key=true \
    --property print.value=true \
    --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
    --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
```

---
# Run calculate square Calculator (using WSL)

---
## start Kafka
- open 3 terminals
```shell
   bin/zookeeper-server-start.sh config/zookeeper.properties 
   bin/kafka-server-start.sh config/server.properties
   <working terminal>
```

## prepare topics
```shell
# input-topic
bin/kafka-topics.sh --create \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 1 \
    --topic input-topic

# output-topic
bin/kafka-topics.sh --create \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 1 \
    --topic output-topic \
    --config cleanup.policy=compact

# verify the topics
bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe
```

## run Kafka Streams
```shell
# start kafka producer
bin/kafka-run-class.sh homework/kafkaStreams/SquareCalculator/test/Producer

# start kafka stream
bin/kafka-run-class.sh homework/kafkaStreams/SquareCalculator/test/StreamProcessor

# start kafka consumer
bin/kafka-run-class.sh homework/kafkaStreams/SquareCalculator/test/Consumer

```
