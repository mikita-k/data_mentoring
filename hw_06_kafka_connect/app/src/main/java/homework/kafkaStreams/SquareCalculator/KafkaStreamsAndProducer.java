package homework.kafkaStreams.SquareCalculator;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;
import java.util.Scanner;

public class KafkaStreamsAndProducer {
    public static void main(String[] args) {
        // Create the Kafka producer
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);

        Producer<String, Integer> producer = new KafkaProducer<>(producerConfig);

        // Create the Kafka Streams application
        Properties streamsConfig = new Properties();
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "calculator-application");
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, Integer> inputStream = builder.stream("input-topic", Consumed.with(Serdes.String(), Serdes.Integer()));

        KStream<String, Integer> squaredStream = inputStream.mapValues(num -> num * num);

        squaredStream.to("output-topic", Produced.with(Serdes.String(), Serdes.Integer()));

        KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfig);
        streams.start();

        // Read input numbers from the user and send them to Kafka topic
        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.print("Enter a number (or 'exit' to quit): ");
            String input = scanner.nextLine();
            if (input.equalsIgnoreCase("exit")) {
                break;
            }
            producer.send(new ProducerRecord<>("input-topic", "key", Integer.parseInt(input)));
        }

        producer.close();
        streams.close();
    }
}
