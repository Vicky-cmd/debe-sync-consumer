package com.infotrends.in.debesync.embedded;

import com.infotrends.in.debesync.api.ConsumerSyncEngine;
import com.infotrends.in.debesync.api.converters.utils.ConverterUtils;
import com.infotrends.in.debesync.api.entities.DebeSyncChangeRecord;
import com.infotrends.in.debesync.api.entities.SourceRecord;
import com.infotrends.in.debesync.api.errors.RetryWithToleranceOperator;
import com.infotrends.in.debesync.api.sink.contexts.storage.ConsumerTaskContext;
import io.debezium.config.Configuration;
import io.debezium.engine.Header;
import io.debezium.engine.format.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.runtime.errors.Stage;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;

import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;


@Slf4j
public class ConsumerSyncEngineBuilder<R> implements ConsumerSyncEngine.Builder<R> {

    private static final String KEY_CONVERTER_PREFIX = "sink.converters.key";
    private static final String VALUE_CONVERTER_PREFIX = "sink.converters.value";
    private static final String HEADER_CONVERTER_PREFIX = "sink.converters.header";
    private Configuration config;

    private Converter keyConverter;
    private Converter valueConverter;
    private HeaderConverter headerConverter;

    private final ConsumerSyncEngine.Builder<DebeSyncChangeRecord<?, ?, ?>> engineBuilder;

    protected Function<R, DebeSyncChangeRecord<?, ?, ?>> fromConverter;
    protected Function<DebeSyncChangeRecord<?, ?, ?>, R> toConverter;

    private RetryWithToleranceOperator retryWithToleranceOperator;
    private final Class<? extends SerializationFormat<?>> keyFormat;
    private final Class<? extends SerializationFormat<?>> valueFormat;
    private final Class<? extends SerializationFormat<?>> headerFormat;

    public ConsumerSyncEngineBuilder() {
        this(KeyValueHeaderChangeEventFormat.of(null, null, null));
    }

    public <V extends SerializationFormat<?>> ConsumerSyncEngineBuilder(ChangeEventFormat<V> format) {
        this(KeyValueHeaderChangeEventFormat.of(null, format.getValueFormat(), null));
    }

    public <K extends SerializationFormat<?>, V extends SerializationFormat<?>> ConsumerSyncEngineBuilder(KeyValueChangeEventFormat<K,V> format) {
        this(format instanceof KeyValueHeaderChangeEventFormat? (KeyValueHeaderChangeEventFormat<K, V, ?>) format:
                KeyValueHeaderChangeEventFormat.of(format.getKeyFormat(), format.getValueFormat(), null));
    }

    public <K extends SerializationFormat<?>, V extends SerializationFormat<?>, H extends SerializationFormat<?>> ConsumerSyncEngineBuilder(KeyValueHeaderChangeEventFormat<K, V, H> format) {
        log.info("Creating instance for the builder with 3 vars");
        this.keyFormat = format.getKeyFormat();
        this.valueFormat = format.getValueFormat();
        this.headerFormat = format.getHeaderFormat();
        this.engineBuilder = EmbeddedSyncEngine.EmbeddedSyncEngineBuilder.builder();
    }

    @Override
    public ConsumerSyncEngine.Builder<R> using(Properties props) {
        this.config = Configuration.from(props);
        Configuration sinkConfig = this.config.subset("sink", true);
        this.engineBuilder.using(sinkConfig.asProperties());
        return this;
    }

    @Override
    public ConsumerSyncEngine.Builder<R> using(ConsumerSyncEngine.ChangeEmitter<R> emitter) {
        this.engineBuilder.using(new EmbeddedChangeEmitter<>(emitter) {
            @Override
            public void run(Consumer<List<DebeSyncChangeRecord<?, ?, ?>>> consumer) {
                emitter.run((records) -> consumer.accept(records.stream()
                        .map(fromConverter).collect(Collectors.toList())));
            }

            @Override
            public List<? extends DebeSyncChangeRecord<?, ?, ?>> poll() {
                return emitter.poll().stream()
                        .map(fromConverter::apply)
                        .collect(Collectors.toList());
            }
        });
        return this;
    }

    @Override
    public ConsumerSyncEngine.Builder<R> using(ConsumerTaskContext consumerTaskContext) {
        this.retryWithToleranceOperator = consumerTaskContext.retryWithToleranceOperator();
        this.engineBuilder.using(consumerTaskContext);
        return this;
    }

    @Override
    public ConsumerSyncEngine.Builder<R> using(ConsumerSyncEngine.CallbackSubscription subscription) {
        this.engineBuilder.using(subscription);
        return this;
    }

    @Override
    public ConsumerSyncEngine.Builder<R> using(ConsumerSyncEngine.CompletionCallback callback) {
        this.engineBuilder.using(callback);
        return this;
    }

    @Override
    public ConsumerSyncEngine.Builder<R> using(ClassLoader classLoader) {
        this.engineBuilder.using(classLoader);
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ConsumerSyncEngine<R> build() {
        ConsumerSyncEngine<DebeSyncChangeRecord<?, ?, ?>> consumerSyncEngine = engineBuilder.build();
        this.keyConverter = ConverterUtils.createConverter(config, KEY_CONVERTER_PREFIX, keyFormat, true);
        this.valueConverter = ConverterUtils.createConverter(config, VALUE_CONVERTER_PREFIX, valueFormat, false);
        this.headerConverter = ConverterUtils.createHeaderConverter(config, HEADER_CONVERTER_PREFIX,headerFormat);
        this.fromConverter = (record) -> {
            SourceRecord sourceRecord = (SourceRecord) record;
            String topicName = sourceRecord.topic();
            this.retryWithToleranceOperator.sourceRecord(sourceRecord);
            final SchemaAndValue key = this.retryWithToleranceOperator.execute(() -> keyConverter.toConnectData(topicName, sourceRecord.key()), Stage.KEY_CONVERTER, this.getClass());
            final SchemaAndValue value = this.retryWithToleranceOperator.execute(() -> valueConverter.toConnectData(topicName, sourceRecord.value()), Stage.VALUE_CONVERTER, this.getClass());
            final Headers headers = this.retryWithToleranceOperator.execute(() -> this.convertHeaderDetails(sourceRecord), Stage.HEADER_CONVERTER, this.getClass());
            if (this.retryWithToleranceOperator.failed()) {
                return null;
            }
            return new DebeSyncChangeRecord<>(
                    key,
                    value,
                    sourceRecord.headers(),
                    headers,
                    sourceRecord
            );
        };
        this.toConverter = (record) -> (R) record.sourceRecord();
        return new ConsumerSyncEngine<>() {
            @Override
            public boolean isRunning() {
                return consumerSyncEngine.isRunning();
            }

            @Override
            public void stop() {
                consumerSyncEngine.stop();
            }

            @Override
            public void run() {
                consumerSyncEngine.run();
            }
        };
    }

    private Headers convertHeaderDetails(SourceRecord record) {
        Headers headers = new ConnectHeaders();
        for (Header<byte[]> header: record.headers()) {
            SchemaAndValue headerValue = this.headerConverter.toConnectHeader(record.topic(), header.getKey(), header.getValue());
            headers.add(header.getKey(), headerValue);
        }
        return headers;
    }
}
