package homework.kafkaStreams.SquareCalculator;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.*;

import java.time.Duration;
import java.util.*;

public class Consumer {
    public static void main(String[] args) {
        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "my-consumer-group");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);

        KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(consumerConfig);
        consumer.subscribe(Collections.singletonList("output-topic"));

        try {
            while (true) {
                ConsumerRecords<String, Integer> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, Integer> record : records) {
                    System.out.println("Key: " + record.key() + ", Value: " + record.value());
                }
            }
        } finally {
            consumer.close();
        }
    }
}
