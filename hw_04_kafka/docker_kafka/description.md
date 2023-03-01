# Run docker containers:
- open current folder in terminal
- add containers to docker
  - docker-compose -f docker-compose.yml up -d
- check if containers is running
  - docker ps
# Create Kafka topic
- open Kafka folder with shell-scripts
  - opt/kafka_2.13-2.8.1/bin
- create topic 
  - kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic first_kafka_topic
- check if topic created 
  - kafka-topics.sh --list --zookeeper zookeeper:2181
