package homework.kafkaStreams.Connector;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceTask;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JsonDirectorySourceConnector extends SourceConnector {

    private String directory;

    @Override
    public void start(Map<String, String> props) {
        directory = props.get("directory");
        if (directory == null || directory.isEmpty()) {
            throw new ConfigException("Missing required configuration 'directory'");
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return JsonDirectorySourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> configs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> config = new HashMap<>();
            config.put("directory", directory);
            configs.add(config);
        }
        return configs;
    }

    @Override
    public void stop() {
        // Clean up any resources here if needed
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef()
                .define("directory", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Directory path");
    }

    @Override
    public String version() {
        return "1.0";
    }
}
