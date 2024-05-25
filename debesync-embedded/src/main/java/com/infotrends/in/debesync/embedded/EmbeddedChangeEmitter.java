package com.infotrends.in.debesync.embedded;

import com.infotrends.in.debesync.api.ConsumerSyncEngine;
import com.infotrends.in.debesync.api.errors.dlq.DeadLetterQueueReporter;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

public abstract class EmbeddedChangeEmitter<R, T> implements ConsumerSyncEngine.ChangeEmitter<R> {

    private final ConsumerSyncEngine.ChangeEmitter<T> emitter;

    public EmbeddedChangeEmitter(ConsumerSyncEngine.ChangeEmitter<T> emitter) {
        this.emitter = emitter;
    }
    @Override
    public void setup(Properties config) {
        this.emitter.setup(config);
    }

    @Override
    public void subscribe(String... topicName) {
        this.emitter.subscribe(topicName);
    }

    @Override
    public void subscribe(String topicName) {
        this.emitter.subscribe(topicName);
    }

    @Override
    public void subscribe(Pattern topicNameRegex) {
        this.emitter.subscribe(topicNameRegex);
    }

    @Override
    public List<PartitionInfo> partitions(String topicName) {
        return this.emitter.partitions(topicName);
    }

    @Override
    public Map<TopicPartition, Long> offsets(String topicName) {
        return this.emitter.offsets(topicName);
    }

    @Override
    public void commit() {
        this.emitter.commit();
    }

    @Override
    public void close() {
        this.emitter.close();
    }

    @Override
    public Class<? extends DeadLetterQueueReporter> deadLetterQueueReporter() {
        return this.emitter.deadLetterQueueReporter();
    }

    @Override
    public Set<TopicPartition> assignment() {
        return this.emitter.assignment();
    }

    @Override
    public void pause(TopicPartition... partitions) {
        this.emitter.pause(partitions);
    }

    @Override
    public void resume(TopicPartition... partitions) {
        this.emitter.resume(partitions);
    }
}
