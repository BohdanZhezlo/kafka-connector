package com.vs.kafka.source;

import com.vs.kafka.VersionUtil;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MySourceConnector extends SourceConnector {

    private static Logger LOGGER = LoggerFactory.getLogger(MySourceConnector.class);

    private MySourceConnectorConfig config;

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> properties) {
        LOGGER.info("Starting connector...");
        try {
            config = new MySourceConnectorConfig(properties);
        } catch (ConfigException ce) {
            LOGGER.error("Couldn't start Source connector due to configuration error: {} ", ce.getMessage());
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return MySourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        LOGGER.info("Max tasks: {}", maxTasks);
        List<Map<String, String>> configs = new ArrayList<>(maxTasks);
        Map<String, String> taskConfig = new HashMap<>();
        taskConfig.putAll(config.toMap());
        configs.add(taskConfig);
        return configs;
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return MySourceConnectorConfig.conf();
    }
}
