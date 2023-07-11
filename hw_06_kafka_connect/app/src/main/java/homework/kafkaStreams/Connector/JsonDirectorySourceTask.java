package homework.kafkaStreams.Connector;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

public class JsonDirectorySourceTask extends SourceTask {

    private String directory;
    private Set<String> processedFiles;
    private String topic = "expedia";

    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void start(Map<String, String> props) {
        directory = props.get("directory");
        processedFiles = new HashSet<>();
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        List<SourceRecord> records = new ArrayList<>();

        File dir = new File(directory);
        File[] files = dir.listFiles((dir1, name) -> name.toLowerCase().endsWith(".json"));

        if (files != null) {
            for (File file : files) {
                String filePath = file.getAbsolutePath();

                if (processedFiles.contains(filePath)) {
                    continue; // Пропускаем файлы, которые уже были обработаны
                }

                try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        records.add(new SourceRecord(null, null, topic, null, null, null, line));
                    }
                } catch (IOException e) {
                    // Обработка ошибок чтения файла
                    e.printStackTrace();
                }

                processedFiles.add(filePath);
            }
        }

        return records;
    }

    @Override
    public void stop() {
        // Очистка ресурсов и завершение работы коннектора
    }
}
