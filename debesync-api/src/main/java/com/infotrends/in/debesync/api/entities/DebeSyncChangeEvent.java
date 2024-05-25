package com.infotrends.in.debesync.api.entities;

import io.debezium.engine.ChangeEvent;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;

public interface DebeSyncChangeEvent<K,V> extends ChangeEvent<K, V> {

    String source();
    String destination(String destination);
    Schema keySchema();
    Schema valueSchema();
    SinkRecord sinkRecord();
    Headers connectHeaders();

}
