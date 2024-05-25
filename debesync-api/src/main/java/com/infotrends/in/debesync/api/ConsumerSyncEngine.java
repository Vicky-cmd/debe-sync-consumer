package com.infotrends.in.debesync.api;

import com.infotrends.in.debesync.api.entities.SourceRecord;
import com.infotrends.in.debesync.api.errors.DebeSyncException;
import com.infotrends.in.debesync.api.errors.dlq.DeadLetterQueueReporter;
import com.infotrends.in.debesync.api.health.TaskMetrics;
import com.infotrends.in.debesync.api.sink.contexts.storage.ConsumerTaskContext;
import io.debezium.engine.format.ChangeEventFormat;
import io.debezium.engine.format.KeyValueChangeEventFormat;
import io.debezium.engine.format.KeyValueHeaderChangeEventFormat;
import io.debezium.engine.format.SerializationFormat;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.eclipse.microprofile.config.Config;

import java.io.Closeable;
import java.io.Serializable;
import java.util.*;
import java.util.function.Consumer;
import java.util.regex.Pattern;

public interface ConsumerSyncEngine<R> extends Runnable, Serializable {

    boolean isRunning();
    void stop();
    Pattern SHELL_PROPERTY_NAME_PATTERN = Pattern.compile("^[a-zA-Z0-9_]+_+[a-zA-Z0-9_]+$");
    interface ChangeConnector<R> extends Runnable, Closeable {
        <K extends SerializationFormat<?>, V extends SerializationFormat<?>, H extends SerializationFormat<?>> void configure(Builder<SourceRecord> builder, KeyValueHeaderChangeEventFormat<K, V, H> format, ChangeEmitter<SourceRecord> emitter, Properties props) throws DebeSyncException;
    }

    interface ChangeEmitter<R> {

        void setup(Properties config);

        Set<TopicPartition> assignment();

        void subscribe(String ...topicName);

        void subscribe(String topicName);

        void subscribe(Pattern topicNameRegex);

        List<PartitionInfo> partitions(String topicName);

        Map<TopicPartition, Long> offsets(String topicName);

        void pause(TopicPartition... partitions);

        void resume(TopicPartition... partitions);

        void run(Consumer<List<R>> consumer);

        List<? extends R> poll();

        void commit();
        void close();

        Class<? extends DeadLetterQueueReporter> deadLetterQueueReporter();

        default EmitterType type() {
            return EmitterType.POLL_MODE;
        }
        enum EmitterType {
            POLL_MODE, RUNNER_MODE
        }
    }

    interface Builder<R> {

        static Builder<?> builder() {
            final BuilderFactory builderFactory = determineBuilder();
            return builderFactory.builder();
        }

        static <T extends SourceRecord, V extends SerializationFormat<?>> Builder<T> builder(ChangeEventFormat<V> format) {
            final BuilderFactory builderFactory = determineBuilder();
            return builderFactory.builder(format);
        }


        static <T extends SourceRecord, K extends SerializationFormat<?>, V extends SerializationFormat<?>> Builder<T> builder(KeyValueChangeEventFormat<K, V> format) {
            final BuilderFactory builderFactory = determineBuilder();
            return builderFactory.builder(format);
        }

        static <T extends SourceRecord, K extends SerializationFormat<?>, V extends SerializationFormat<?>, H  extends SerializationFormat<?>> Builder<T> builder(KeyValueHeaderChangeEventFormat<K, V, H> format) {
            final BuilderFactory builderFactory = determineBuilder();
            return builderFactory.builder(format);
        }

        static <T extends SourceRecord> Builder<T> builder(Class<? extends SerializationFormat<?>> keyFormat, Class<? extends SerializationFormat<?>> valueFormat, Class<? extends SerializationFormat<?>> headerFormat) {
            return builder(KeyValueHeaderChangeEventFormat.of(keyFormat, valueFormat, headerFormat));
        }

        private static BuilderFactory determineBuilder() {
            final ServiceLoader<BuilderFactory> serviceLoader = ServiceLoader.load(BuilderFactory.class);
            final Iterator<BuilderFactory> iterator = serviceLoader.iterator();
            if (!iterator.hasNext()) {
                throw new DebeSyncException("No valid implementation found for the Engine Builder. Please check the configuration for the ServiceLoader");
            }
            return iterator.next();
        }
        Builder<R> using(Properties config);

        Builder<R> using(ChangeEmitter<R> emitter);

        Builder<R> using(ConsumerTaskContext sinkTaskContext);

        Builder<R> using(CallbackSubscription subscription);
        Builder<R> using(CompletionCallback callback);

        Builder<R> using(ClassLoader classLoader);

        ConsumerSyncEngine<R> build();

    }

    interface BuilderFactory {
        <T> Builder<T> builder();
        <T, V extends SerializationFormat<?>> Builder<T> builder(ChangeEventFormat<V> format);
        <T, K extends SerializationFormat<?>, V extends SerializationFormat<?>> Builder<T> builder(KeyValueChangeEventFormat<K, V> format);
        <T, K extends SerializationFormat<?>, V extends SerializationFormat<?>, H  extends SerializationFormat<?>> Builder<T> builder(KeyValueHeaderChangeEventFormat<K, V, H> format);
    }

    interface CallbackSubscription {
        void connectorStarted();
        void connectorStopped();
        void taskStarted();
        void taskStatus(TaskMetrics metrics);
        void taskStopped();
    }

    interface CompletionCallback {
        void completed(String status, String message, Throwable error);
    }

    static void copyConfigToProps(Config config, Properties props, String oldPrefix, String newPrefix, boolean overwrite) {
        for (String name : config.getPropertyNames()) {
            String updatedPropertyName = name;
            if (SHELL_PROPERTY_NAME_PATTERN.matcher(name).matches()) {
                updatedPropertyName = name.replace("_", ".").toLowerCase();
            }
            if (!updatedPropertyName.startsWith(oldPrefix)) {
                continue;
            }
            updatedPropertyName = newPrefix + updatedPropertyName.substring(oldPrefix.length());
            if (props.contains(updatedPropertyName) && !overwrite)
                continue;

            props.put(updatedPropertyName, config.getConfigValue(name).getValue());
        }
    }
}
