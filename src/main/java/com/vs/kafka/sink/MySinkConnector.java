package com.vs.kafka.sink;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.vs.kafka.VersionUtil;
import com.vs.kafka.source.MySourceConnectorConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySinkConnector extends SinkConnector {

    private static Logger LOGGER = LoggerFactory.getLogger(MySinkConnector.class);

    private MySinkConnectorConfig config;

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> map) {
        LOGGER.info("Starting Sink connector...");
        try {
            config = new MySinkConnectorConfig(map);
        } catch (ConfigException ce) {
            LOGGER.error("Couldn't start Sink connector due to configuration error: {} ", ce.getMessage());
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return MySinkTask.class;
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
        //TODO: Do things that are necessary to stop your connector.
    }

    @Override
    public ConfigDef config() {
        return MySinkConnectorConfig.conf();
    }
}
