package com.infotrends.in.debesync.mysql.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.Map;

public class ExtractTopicName<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final String TABLE_LOCATION_CONFIG_NAME = "table.location";
    private int tableNameLocation;
    static final int DEFAULT_TABLE_LOCATION = 2;

    private static final String TABLE_SEPARATOR_CONFIG_NAME = "table.separator";
    private String topicNameSeparator;
    static final String DEFAULT_TOPIC_NAME_SEPARATOR = "\\.";

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(TABLE_LOCATION_CONFIG_NAME, ConfigDef.Type.INT, DEFAULT_TABLE_LOCATION, ConfigDef.Importance.HIGH, "The location of the table name from topic")
            .define(TABLE_SEPARATOR_CONFIG_NAME, ConfigDef.Type.STRING, DEFAULT_TOPIC_NAME_SEPARATOR, ConfigDef.Importance.HIGH, "The separator used for separating the topic");

    @Override
    public R apply(R record) {
        String topicName = record.topic();
        String[] topicArr = topicName.split(this.topicNameSeparator);
        String newTopicName = topicArr[this.tableNameLocation];
        return record.newRecord(
                newTopicName,
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                record.valueSchema(),
                record.value(),
                record.timestamp()
        );
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        this.tableNameLocation = config.getInt(TABLE_LOCATION_CONFIG_NAME);
        this.topicNameSeparator = config.getString(TABLE_SEPARATOR_CONFIG_NAME);
    }
}
