package com.vs.kafka.source;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.HashMap;
import java.util.Map;


public class MySourceConnectorConfig extends AbstractConfig {


    public static final String INT_RANGE_CONFIG = "my-source-connector.int-field-range";
    public static final String INT_RANGE_CONFIG_DOC = "A range for generating random integer values";

    public static final String POLL_INTERVAL_CONFIG = "my-source-connector.poll-interval";
    public static final String POLL_INTERVAL_CONFIG_DOC = "AN interval (in millis) between polls";

    public static final String TOPIC_CONFIG = "my-source-connector.topic";
    public static final String TOPIC_DEFAULT = "bzhezlo_hw_3";
    private static final String TOPIC_DOC = "Topic to publish data to";

    public MySourceConnectorConfig(ConfigDef config, Map<String, String> parsedConfig) {
        super(config, parsedConfig);
    }

    public MySourceConnectorConfig(Map<String, String> parsedConfig) {
        this(conf(), parsedConfig);
    }

    public static ConfigDef conf() {
        ConfigDef configDef = new ConfigDef()
                .define(INT_RANGE_CONFIG, Type.INT, Importance.HIGH, INT_RANGE_CONFIG_DOC)
                .define(POLL_INTERVAL_CONFIG, Type.LONG, Importance.HIGH, POLL_INTERVAL_CONFIG_DOC)
                .define(TOPIC_CONFIG, ConfigDef.Type.STRING, TOPIC_DEFAULT, new ConfigDef.NonEmptyStringWithoutControlChars(), ConfigDef.Importance.HIGH, TOPIC_DOC);
        return configDef;
    }

    public Map<String, String> toMap() {
        Map<String, ?> uncastProperties = this.values();
        Map<String, String> config = new HashMap<>(uncastProperties.size());
        uncastProperties.forEach((key, valueToBeCast) -> config.put(key, valueToBeCast.toString()));
        return config;
    }

}
