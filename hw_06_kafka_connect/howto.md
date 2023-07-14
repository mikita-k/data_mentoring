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
bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
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

# СТОП-ЛЕНЬ!

0. start Kafka
```shell
- open 3 terminals
   bin/zookeeper-server-start.sh config/zookeeper.properties 
   bin/kafka-server-start.sh config/server.properties
   <working terminal>
```


1. run connector: read JSON and send to _expedia_ topic
```shell
# create topic 'expedia'
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic expedia

# check topic content
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic expedia --from-beginning

# run connector
bin/connect-standalone.sh config/connect-standalone.properties /home/mentee/homework/hw_06/json-directory-source.properties
```

2. run kafka stream, calculate duration and send to _expedia-duration_ topic
```shell
# create topic 'expedia-duration'
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic expedia-duration

# check topic content
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic expedia-duration --from-beginning

# run kafka stream for calculate duration
bin/kafka-run-class.sh homework/kafkaStreams/KafkaStream/KafkaStreamDuration

# NB! Нужно закинуть в общий котёл кафки ещё и либу json-20230618 !
```

3. check result calculating!
```shell
# Show total amount of hotels (hotel_id) and number of distinct hotels (hotel_id) for each category.

# запустить расчёт всех отелей по категориям гостевания
bin/kafka-run-class.sh homework/kafkaStreams/Aggregation/KafkaConsumerCheckTotal

# запустить нормальный расчёт отелей по категориям гостевания
bin/kafka-run-class.sh homework/kafkaStreams/Aggregation/KafkaConsumerKTable
```

4. utils
```shell
# delete topic
bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic expedia

# check topics
bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# очистить ресурсы
rm -rf /tmp/kafka-logs /tmp/zookeeper /tmp/kraft-combined-logs
```
