package homework.kafkaStreams.Aggregation;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONObject;

import java.time.Duration;
import java.util.*;

public class KafkaConsumerCheckTotal {

    public static void main(String[] args) {
        // Конфигурация Kafka-консюмера
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-check-total-hotel-counts");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Создание Kafka-консюмера
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // Подписка на топик "expedia-duration"
        consumer.subscribe(Arrays.asList("expedia-duration"));

        // Создание HashMap для подсчета количества hotel_id для каждого duration
        Map<String, ArrayList<Long>> durationCounts = new HashMap<>();

        // Чтение сообщений из Kafka-топика
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            boolean show_data = false;

            for (ConsumerRecord<String, String> record : records) {
                String messageValue = record.value();

                // Извлечение значений hotel_id и duration из JSON-сообщения
                JSONObject json = new JSONObject(messageValue);
                long hotelId = json.getLong("hotel_id");
                String duration = json.getString("duration");

                // Добавление отеля для текущего duration
                if (durationCounts.get(duration) == null) {
                    durationCounts.put(duration, new ArrayList<>(Arrays.asList(hotelId)));
                } else {
                    durationCounts.get(duration).add(hotelId);
                }

                show_data = true;
            }

            if (!show_data) {
                continue;
            }

            // Вывод общего количества hotel_id для каждого duration
            System.out.println("\n\n\tTotal amount of hotels (hotel_id) for each category.");
            for (Map.Entry<String, ArrayList<Long>> entry : durationCounts.entrySet()) {
                System.out.println(entry.getKey() + ": " + entry.getValue().size());
            }
            System.out.println("\n\tNumber of distinct hotels (hotel_id) for each category.");
            for (Map.Entry<String, ArrayList<Long>> entry : durationCounts.entrySet()) {
                HashSet<Long> distinctSet = new HashSet<>(entry.getValue());
                System.out.println(entry.getKey() + ": " + distinctSet.size());
            }
        }
    }
}
