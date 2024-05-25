package com.infotrends.in.debesync.api.errors.dlq;

import com.infotrends.in.debesync.api.errors.DebeSyncException;
import com.infotrends.in.debesync.api.errors.ErrorReporter;
import com.infotrends.in.debesync.api.converters.utils.ConverterUtils;
import com.infotrends.in.debesync.api.entities.EmbeddedHeader;
import io.debezium.config.Configuration;
import io.debezium.engine.Header;
import io.debezium.engine.format.Json;
import io.debezium.engine.format.KeyValueHeaderChangeEventFormat;
import io.debezium.engine.format.SerializationFormat;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.util.ConnectorTaskId;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public abstract class DeadLetterQueueReporter implements ErrorReporter {

    protected Configuration config;
    protected SinkConnectorConfig connectorConfig;

    protected final ConnectorTaskId connectorTaskId;

    private static final String KEY_CONVERTER_PREFIX = "converters.key";
    private static final String VALUE_CONVERTER_PREFIX = "converters.value";
    private static final String HEADER_CONVERTER_PREFIX = "converters.header";

    protected final Converter keyConverter;
    protected final Converter valueConverter;
    protected final HeaderConverter headerConverter;

    private final Class<? extends SerializationFormat<?>> headerFormat;

    public <K extends SerializationFormat<?>, V extends SerializationFormat<?>, H extends SerializationFormat<?>>
        DeadLetterQueueReporter(ConnectorTaskId connectorTaskId,
                                Configuration config,
                                SinkConnectorConfig connectorConfig,
                                KeyValueHeaderChangeEventFormat<K, V, H> format) {
        this.connectorTaskId=connectorTaskId;
        this.config=config;
        this.connectorConfig=connectorConfig;

        Class<? extends SerializationFormat<?>> keyFormat = format.getKeyFormat();
        Class<? extends SerializationFormat<?>> valueFormat = format.getValueFormat();
        this.headerFormat = format.getHeaderFormat();
        this.keyConverter = ConverterUtils.createConverter(config, KEY_CONVERTER_PREFIX, keyFormat, true);
        this.valueConverter = ConverterUtils.createConverter(config, VALUE_CONVERTER_PREFIX, valueFormat, false);
        this.headerConverter = ConverterUtils.createHeaderConverter(config, HEADER_CONVERTER_PREFIX,headerFormat);
    }

    protected boolean shouldConvertHeadersToString() {
        return ConverterUtils.isFormat(this.headerFormat, Json.class);
    }

    protected byte[] toBytes(String value) {
        if (value != null) {
            return value.getBytes(StandardCharsets.UTF_8);
        } else {
            return null;
        }
    }
    protected byte[] toBytes(int value) {
        return toBytes(String.valueOf(value));
    }
    protected byte[] toBytes(long value) {
        return toBytes(String.valueOf(value));
    }

    protected byte[] stacktrace(Throwable error) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            PrintStream stream = new PrintStream(bos, true, StandardCharsets.UTF_8.name());
            error.printStackTrace(stream);
            bos.close();
            return bos.toByteArray();
        } catch (IOException e) {
            log.error("Could not serialize stacktrace.", e);
        }
        return null;
    }

    protected byte[] getBytes(Object object) {
        if (object instanceof byte[]) {
            return (byte[]) object;
        }
        else if (object instanceof String) {
            return ((String) object).getBytes(StandardCharsets.UTF_8);
        }
        throw new DebeSyncException(String.format("Unsupported format of the object. Unable to convert the objects to bytes. Unsupported type - %s", object.getClass().getSimpleName()));
    }

    protected List<Header<byte[]>> convertHeaders(SinkRecord record) {
        List<Header<byte[]>> headers = new ArrayList<>();
        for (org.apache.kafka.connect.header.Header header : record.headers()) {
            String headerKey = header.key();
            byte[] rawHeader = headerConverter.fromConnectHeader(record.topic(), headerKey, header.schema(), header.value());
            headers.add(new EmbeddedHeader<>(headerKey, rawHeader));
        }
        return headers;
    }

    protected org.apache.kafka.common.header.Headers convertKafkaHeaders(List<Header<?>> headers) {
        org.apache.kafka.common.header.Headers kafkaHeaders = new RecordHeaders();
        for (Header<?> header : headers) {
            kafkaHeaders.add(header.getKey(), getBytes(header.getValue()));
        }
        return kafkaHeaders;
    }
}
