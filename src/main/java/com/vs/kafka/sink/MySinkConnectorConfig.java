package com.vs.kafka.sink;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.HashMap;
import java.util.Map;

public class MySinkConnectorConfig extends AbstractConfig {

    public static final String FILE_NAME_CONFIG = "my-connector.file-name";
    private static final String FILE_NAME_DOC = "File where to put results";

    public MySinkConnectorConfig(ConfigDef config, Map<String, String> parsedConfig) {
        super(config, parsedConfig);
    }

    public MySinkConnectorConfig(Map<String, String> parsedConfig) {
        this(conf(), parsedConfig);
    }

    public static ConfigDef conf() {
        ConfigDef configDef = new ConfigDef()
                .define(FILE_NAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, FILE_NAME_DOC);
        return configDef;
    }

    public Map<String, String> toMap() {
        Map<String, ?> uncastProperties = this.values();
        Map<String, String> config = new HashMap<>(uncastProperties.size());
        uncastProperties.forEach((key, valueToBeCast) -> config.put(key, valueToBeCast.toString()));
        return config;
    }

}
