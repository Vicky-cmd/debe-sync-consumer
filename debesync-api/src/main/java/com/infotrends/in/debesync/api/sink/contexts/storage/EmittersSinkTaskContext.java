package com.infotrends.in.debesync.api.sink.contexts.storage;

import com.infotrends.in.debesync.api.errors.DebeSyncException;
import com.infotrends.in.debesync.api.ConsumerSyncEngine;
import com.infotrends.in.debesync.api.errors.ErrorRecordsReporter;
import com.infotrends.in.debesync.api.errors.RetryWithToleranceOperator;
import com.infotrends.in.debesync.api.errors.dlq.DeadLetterQueueReporter;
import io.debezium.config.Configuration;
import io.debezium.config.Instantiator;
import io.debezium.engine.format.KeyValueHeaderChangeEventFormat;
import io.debezium.engine.format.SerializationFormat;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.util.ConnectorTaskId;

import java.lang.reflect.InvocationTargetException;
import java.util.*;

@Slf4j
public class EmittersSinkTaskContext<K extends SerializationFormat<K>,
        V extends SerializationFormat<V>,
        H extends SerializationFormat<H>> implements ConsumerTaskContext {

    private final Configuration config;
    private final ConsumerSyncEngine.ChangeEmitter<?> emitter;
    final Map<TopicPartition, Long> offsets;
    final Set<TopicPartition> pausedPartitions;
    final Set<TopicPartition> resumedPartitions;
    long timeoutMs;
    boolean requestedCommit;

    private final RetryWithToleranceOperator retryWithToleranceOperator;

    private final SinkConnectorConfig connectorConfig;

    private final ConnectorTaskId connectorTaskId;

    private final KeyValueHeaderChangeEventFormat<K, V, H> format;

    public EmittersSinkTaskContext(Map<String, String> config,
                                   SinkConnectorConfig connectorConfig,
                                   ConnectorTaskId connectorTaskId,
                                   KeyValueHeaderChangeEventFormat<K, V, H> format,
                                   ConsumerSyncEngine.ChangeEmitter<?> emitter) {
        this.config = Configuration.from(config);
        this.emitter = emitter;
        this.offsets = new HashMap<>();
        this.timeoutMs = -1L;
        this.pausedPartitions = new HashSet<>();
        this.resumedPartitions = new HashSet<>();
        this.requestedCommit = false;
        this.connectorTaskId = connectorTaskId;
        this.format = format;
        this.connectorConfig = connectorConfig;
//        this.retryWithToleranceOperator = retryWithToleranceOperator;
        this.retryWithToleranceOperator = new RetryWithToleranceOperator(this.connectorConfig.errorRetryTimeout(),
                this.connectorConfig.errorMaxDelayInMillis(), this.connectorConfig.errorToleranceType(), Time.SYSTEM);
        this.configureRecordReporters();
    }


    @Override
    public Map<String, String> configs() {
        log.info("Getting the config");
        return this.config.asMap();
    }

    @Override
    public void offset(Map<TopicPartition, Long> offsets) {
        log.info("Updating the offsets...");
        this.offsets.putAll(offsets);
    }

    @Override
    public void offset(TopicPartition tp, long offset) {
        log.info("Updating the offsets for " + tp.partition());
        this.offsets.put(tp, offset);
    }

    @Override
    public void timeout(long timeoutMs) {
        log.info("Setting the timeout " + timeoutMs);
        this.timeoutMs = timeoutMs;
    }

    @Override
    public Set<TopicPartition> assignment() {
        log.info("Fetching the partitions");
        return this.emitter.assignment();
    }

    @Override
    public void pause(TopicPartition... partitions) {
        log.info("Pausing the partitions");
        this.pausedPartitions.addAll(List.of(partitions));
        this.emitter.pause(partitions);
    }

    @Override
    public void resume(TopicPartition... partitions) {
        log.info("Resuming the partitions");
        this.resumedPartitions.addAll(List.of(partitions));
        this.emitter.resume(partitions);
    }

    @Override
    public void requestCommit() {
        log.info("Requesting commit");
        this.requestedCommit = true;
    }

    @Override
    public ErrantRecordReporter errantRecordReporter() {
        return new ErrorRecordsReporter(this.config, this.connectorConfig,
                this.retryWithToleranceOperator);
    }

    private void configureRecordReporters() {
        Class<? extends DeadLetterQueueReporter> deadLetterQueueReporterClazz = this.emitter.deadLetterQueueReporter();
        try {
//            Instantiator.getInstance(deadLetterQueueReporterClazz.getCanonicalName(), )
            DeadLetterQueueReporter deadLetterQueueReporter = deadLetterQueueReporterClazz.getDeclaredConstructor(ConnectorTaskId.class, Configuration.class,
                    SinkConnectorConfig.class, KeyValueHeaderChangeEventFormat.class)
                    .newInstance(this.connectorTaskId, this.config, this.connectorConfig, format);
            this.retryWithToleranceOperator.reporters(List.of(deadLetterQueueReporter));
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new DebeSyncException(String.format("Error occurred while initializing Dead Letter Reporter. Error - %s. Message: %s", e.getClass().getSimpleName(), e.getMessage()), e);
        }
    }

    @Override
    public RetryWithToleranceOperator retryWithToleranceOperator() {
        return this.retryWithToleranceOperator;
    }
}
