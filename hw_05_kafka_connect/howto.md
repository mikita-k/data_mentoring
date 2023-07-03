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
# start Kafka
- open 3 terminals
```shell
   bin/zookeeper-server-start.sh config/zookeeper.properties 
   bin/kafka-server-start.sh config/server.properties
   <working terminal>
```
- check topics 
```commandline
bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

- TEST Kafks producer/consumer
```shell
# create topic:
bin/kafka-topics.sh --create --topic quickstart-events --bootstrap-server localhost:9092

# describe topic
bin/kafka-topics.sh --describe --topic quickstart-events --bootstrap-server localhost:9092

# start producer
bin/kafka-console-producer.sh --topic quickstart-events --bootstrap-server localhost:9092

# start consumer
bin/kafka-console-consumer.sh --topic quickstart-events --from-beginning --bootstrap-server localhost:9092
```

- TEST Kafka Connect
```shell
# create file
echo -e "foo\nbar" > test.txt

# run source connector
bin/connect-standalone.sh config/connect-standalone.properties config/connect-file-source.properties
#bin/connect-standalone.sh /home/config/json_connector.properties
#bin/connect-standalone.sh /home/config/avro_connector.properties

# check kafka topic content
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic connect-test --from-beginning
#bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic json_topic --from-beginning
#bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic avro_topic --from-beginning

# delete kafka topic
bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic connect-test

# stop connector/kafka/zookeeper
ctrl+c

# clear env
rm -rf /tmp/kafka-logs /tmp/zookeeper /tmp/kraft-combined-logs
```