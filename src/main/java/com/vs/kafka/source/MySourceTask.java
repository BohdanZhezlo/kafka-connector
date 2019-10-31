package com.vs.kafka.source;

import com.vs.kafka.VersionUtil;
import com.vs.kafka.model.MyEntity;
import com.vs.kafka.model.MyEntitySchema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.vs.kafka.source.MySourceConnectorConfig.*;

public class MySourceTask extends SourceTask {

    static final Logger LOGGER = LoggerFactory.getLogger(MySourceTask.class);

    private Integer intRange;
    private String topic;
    private Long interval;
    private Random random;

    private Long lastExecution = 0L;

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> map) {
        setupTaskConfig(map);
        random = new Random();
    }

    @Override
    public List<SourceRecord> poll() {
        if (System.currentTimeMillis() > (lastExecution + interval)) {
            lastExecution = System.currentTimeMillis();
            return toSourceRecords(getEntities());
        }
        return Collections.emptyList();
    }

    private List<SourceRecord> toSourceRecords(List<MyEntity> entities) {
        LOGGER.info("GENERATED {} ENTITIES", entities.size());
        return entities.stream().map(entity ->
                new SourceRecord(
                        new HashMap<>(),
                        new HashMap<>(),
                        topic,
                        MyEntitySchema.myEntityVectorSchema(),
                        MyEntitySchema.toStruct(entity))
        ).collect(Collectors.toList());
    }

    @Override
    public void stop() {
        LOGGER.info("Stopping Source connector!");
    }

    private List<MyEntity> getEntities() {
        int randomNumberOfEntities = randomIntInRange(50, 100);
        List<MyEntity> entities = new ArrayList<>(randomNumberOfEntities);
        for (int i = 0; i < randomNumberOfEntities; i++) {
            Integer randValue = randomIntInRange(0, intRange);
            String randString = UUID.randomUUID().toString();
            entities.add(new MyEntity(randValue, randString));
        }
        return entities;
    }

    private void setupTaskConfig(Map<String, String> props) {
        this.intRange = Integer.parseInt(props.get(INT_RANGE_CONFIG));
        this.topic = props.get(TOPIC_CONFIG);
        this.interval = Long.parseLong(props.get(POLL_INTERVAL_CONFIG));
    }

    private int randomIntInRange(int start, int end) {
        return start + random.nextInt((end - start) + 1);
    }
}
