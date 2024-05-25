package com.infotrends.in.debesync.api.entities;

import io.debezium.engine.Header;
import lombok.AllArgsConstructor;
import org.apache.kafka.common.record.TimestampType;

import java.nio.charset.StandardCharsets;
import java.util.List;

@AllArgsConstructor
public class SourceRecord {
    private final String topic;
    private final int partition;
    private final long offset;
    private final long timestamp;
    private final TimestampType timestampType;
    private final byte[] key;
    private final byte[] value;
    private final List<Header<byte[]>> headers;

    public String topic() {
        return topic;
    }

    public int partition() {
        return partition;
    }

    public long offset() {
        return offset;
    }

    public long timestamp() {
        return timestamp;
    }

    public TimestampType timestampType() {
        return timestampType;
    }

    public byte[] key() {
        return key;
    }

    public byte[] value() {
        return value;
    }

    public List<Header<byte[]>> headers() {
        return headers;
    }

    @Override
    public String toString() {
        return "SourceRecord(topic = " + topic
                + ", partition = " + partition
                + ", offset = " + offset
                + ", " + timestampType + " = " + timestamp
                + ", headers = " + headers
                + ", key = " + new String(key, StandardCharsets.UTF_8)
                + ", value = " + new String(value, StandardCharsets.UTF_8) + ")";
    }
}
