package com.infotrends.in.debesync.embedded;

import com.infotrends.in.debesync.api.ConsumerSyncEngine;
import io.debezium.engine.format.ChangeEventFormat;
import io.debezium.engine.format.KeyValueChangeEventFormat;
import io.debezium.engine.format.KeyValueHeaderChangeEventFormat;
import io.debezium.engine.format.SerializationFormat;

public class EmbeddedBuilderFactory implements ConsumerSyncEngine.BuilderFactory {

    @Override
    public <T> ConsumerSyncEngine.Builder<T> builder() {
        return new ConsumerSyncEngineBuilder<>();
    }

    @Override
    public <T, V extends SerializationFormat<?>> ConsumerSyncEngine.Builder<T> builder(ChangeEventFormat<V> format) {
        return new ConsumerSyncEngineBuilder<>(format);
    }

    @Override
    public <T, K extends SerializationFormat<?>, V extends SerializationFormat<?>> ConsumerSyncEngine.Builder<T> builder(KeyValueChangeEventFormat<K, V> format) {
        return new ConsumerSyncEngineBuilder<>(format);
    }

    @Override
    public <T, K extends SerializationFormat<?>, V extends SerializationFormat<?>, H extends SerializationFormat<?>> ConsumerSyncEngine.Builder<T> builder(KeyValueHeaderChangeEventFormat<K, V, H> format) {
        return new ConsumerSyncEngineBuilder<>(format);
    }
}
