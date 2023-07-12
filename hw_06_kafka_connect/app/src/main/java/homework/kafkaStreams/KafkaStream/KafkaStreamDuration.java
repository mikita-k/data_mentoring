package homework.kafkaStreams.KafkaStream;


import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.json.JSONObject;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.Properties;

public class KafkaStreamDuration {

    public static void main(String[] args) {
        // Настройки Kafka Streams
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-stream-duration");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // Построение графа обработки
        StreamsBuilder builder = new StreamsBuilder();

        // Чтение сообщений из топика "expedia"
        KStream<String, String> expediaStream = builder.stream("expedia");

        // Преобразование сообщений в JSON и вычисление разницы в днях
        expediaStream.mapValues(new ValueMapper<String, String>() {
            @Override
            public String apply(String value) {
                // Парсинг JSON
                JSONObject jsonMessage = new JSONObject(value);

                // Здесь нужно использовать библиотеку JSON парсера по вашему выбору
                JSONObject jsonObject = new JSONObject(jsonMessage.getString("payload"));
                String srchCi = jsonObject.getString("srch_ci");
                String srchCo = jsonObject.getString("srch_co");
                Long hotelId = jsonObject.getLong("hotel_id");

                // Преобразование в даты
                LocalDate checkInDate = LocalDate.parse(srchCi);
                LocalDate checkOutDate = LocalDate.parse(srchCo);

                // Вычисление разницы в днях
                long term = ChronoUnit.DAYS.between(checkInDate, checkOutDate);

                // Выбор категории длительности
                String duration;
                if (term <= 0) {
                    duration = "Erroneous data";
                } else if (term <= 4) {
                    duration = "Short stay";
                } else if (term <= 10) {
                    duration = "Standard stay";
                } else if (term <= 14) {
                    duration = "Standard extended stay";
                } else {
                    duration = "Long stay";
                }

                // Создание результирующего JSON-а
                JSONObject jsonResult = new JSONObject();
                jsonResult.put("hotel_id", hotelId);
                jsonResult.put("duration", duration);
                return jsonResult.toString();
            }
        }).to("expedia-duration"); // Отправка результатов в выходной топик "output-topic"

        // Построение и запуск Kafka Streams приложения
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        // Вывод результатов в консоль
        expediaStream.print(Printed.toSysOut());

        // Остановка приложения при получении сигнала завершения
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
