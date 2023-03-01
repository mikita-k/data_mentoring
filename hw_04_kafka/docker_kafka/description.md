# Run docker containers:
- open current folder in terminal
- add containers to docker
  - docker-compose -f docker-compose.yml up -d
- check if containers is running
  - docker ps
# Create Kafka topic
- open kafka container:
  - docker exec -it kafka /bin/sh
- open Kafka folder with shell-scripts
  - cd opt/kafka_2.13-2.8.1/bin
- create topic 
  - kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic messages
- check if topic created 
  - kafka-topics.sh --list --zookeeper zookeeper:2181
- describe topic
  - kafka-topics.sh --describe --zookeeper zookeeper:2181 --topic messages
- delete topic
  - kafka-topics.sh --delete --zookeeper zookeeper:2181 --topic messages
# Add message to topic
- specify topic for which one to produce messages
  - kafka-console-producer.sh --broker-list kafka:9092 --topic messages
- add message as JSON:
  - {'user_id': 1, 'recipient_id': 2, 'message': 'Hi.'}
  - {'user_id': 2, 'recipient_id': 1, 'message': 'Hello there.'}
- press CTRL+C to close the producer shell
# HOW IT WORKS
  - in real time:
    - open two terminals.
      - in left open Kafka sh and run kafka-producer:
        - kafka-console-producer.sh --broker-list kafka:9092 --topic messages
      - in roght open Kafka sh and run kafka-consumer:
        - kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic messages
      - add a new message in the producer tab:
        - {'user_id': 1, 'recipient_id': 2, 'message': 'Hi again.'}
      - this message shown in the consumer tab
  - list all messages for a specific topic:
    - kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic messages --from-begin
ning
