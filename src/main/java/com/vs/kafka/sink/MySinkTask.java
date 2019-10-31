package com.vs.kafka.sink;

import com.vs.kafka.VersionUtil;
import com.vs.kafka.model.MyEntity;
import com.vs.kafka.model.MyEntitySchema;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.Map;

import static com.vs.kafka.sink.MySinkConnectorConfig.FILE_NAME_CONFIG;

public class MySinkTask extends SinkTask {

    private static Logger LOGGER = LoggerFactory.getLogger(MySinkTask.class);

    private PrintWriter writer;

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        setupTaskConfig(props);
        writer.println("\"IntVal\",\"StringVal\"");
    }

    @Override
    public void put(Collection<SinkRecord> collection) {
        for (SinkRecord sinkRecord : collection) {
            MyEntity entity = MyEntitySchema.fromStruct((Struct) (sinkRecord.value()));
            writer.println("\"" + entity.intValue + "\",\"" + entity.stringValue + "\"");
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
        writer.flush();
    }

    @Override
    public void stop() {
        writer.close();
    }

    private void setupTaskConfig(Map<String, String> props) {
        try {
            File file = new File(props.get(FILE_NAME_CONFIG));
            file.getParentFile().mkdirs();
            this.writer = new PrintWriter(file, "UTF-8");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
