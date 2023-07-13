package homework.kafkaStreams.Aggregation;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.json.JSONObject;

import java.util.Properties;

public class KafkaConsumerKTable {

    public static void main(String[] args) {
        // Setup Kafka Streams
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "duration-aggregation");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // Create KStream
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> expediaStream = builder.stream("expedia-duration");

        // Extract and group by duration
        KTable<String, Long> durationCounts = expediaStream
                .groupBy((key, value) -> extractDuration(value))
                .count(Materialized.as("duration-counts"));

        durationCounts.toStream().foreach((key, value) -> {
            System.out.println("Duration: " + key + ", Count: " + value);
        });

        // Starting Kafka Streams app
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        // Stop Kafka Streams app
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static String extractDuration(String value) {
        JSONObject jsonObject = new JSONObject(value);
        return jsonObject.getString("duration");
    }
}
