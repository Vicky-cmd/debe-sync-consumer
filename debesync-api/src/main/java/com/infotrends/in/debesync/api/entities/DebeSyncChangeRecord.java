package com.infotrends.in.debesync.api.entities;

import io.debezium.engine.Header;
import io.debezium.engine.RecordChangeEvent;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.List;

public class DebeSyncChangeRecord<K,V,H> implements RecordChangeEvent<V>, DebeSyncChangeEvent<K, V> {
    private final K key;
    private final Schema keySchema;
    private final V value;
    private final Schema valueSchema;
    private final List<Header<H>> headers;

    private final Headers connectHeaders;
    private final SourceRecord record;

    private String destination;

    @SuppressWarnings("unchecked")
    public DebeSyncChangeRecord(SchemaAndValue key, SchemaAndValue value, List<Header<H>> headers, Headers connectHeaders, SourceRecord record) {
        this(key!=null?(K)key.value():null, key!=null?key.schema():null, value!=null?(V)value.value():null, value!=null?value.schema():null, headers, connectHeaders, record);
    }

    public DebeSyncChangeRecord(K key, Schema keySchema, V value, Schema valueSchema, List<Header<H>> headers, Headers connectHeaders, SourceRecord record) {
        this.key = key;
        this.keySchema = keySchema;
        this.value = value;
        this.valueSchema = valueSchema;
        this.headers = headers;
        this.connectHeaders = connectHeaders;
        this.record = record;
    }

    @Override
    public K key() {
        return this.key;
    }

    @Override
    public V value() {
        return this.value;
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<Header<H>> headers() {
        return this.headers;
    }

    @Override
    public Headers connectHeaders() {
        return this.connectHeaders;
    }

    @Override
    public String destination() {
        return this.destination;
    }

    @Override
    public String destination(String destination) {
        return this.destination=destination;
    }

    @Override
    public Integer partition() {
        return this.record.partition();
    }

    @Override
    public V record() {
        return this.value;
    }

    @Override
    public String source() {
        return this.record.topic();
    }

    @Override
    public Schema keySchema() {
        return this.keySchema;
    }

    @Override
    public Schema valueSchema() {
        return this.valueSchema;
    }

    public SourceRecord sourceRecord() {
        return this.record;
    }

    @Override
    public SinkRecord sinkRecord() {
        return new SinkRecord(
                this.source(),
                this.record.partition(),
                this.keySchema(),
                this.key(),
                this.valueSchema(),
                this.value(),
                this.record.offset(),
                this.record.timestamp(),
                this.record.timestampType(),
                this.connectHeaders
        );
    }
}
