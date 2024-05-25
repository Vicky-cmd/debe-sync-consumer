package com.infotrends.in.debesync.api.errors;

import io.debezium.config.Configuration;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.errors.Stage;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

@Slf4j
public class ErrorRecordsReporter implements ErrantRecordReporter {

    protected Configuration config;

    protected ConnectorConfig connectorConfig;

    protected RetryWithToleranceOperator retryWithToleranceOperator;

    protected final ConcurrentMap<TopicPartition, List<Future<Void>>> futures;

    public ErrorRecordsReporter(Configuration config,
                                ConnectorConfig connectorConfig,
                                RetryWithToleranceOperator retryWithToleranceOperator) {
        this.config = config;
        this.connectorConfig = connectorConfig;
        this.retryWithToleranceOperator = retryWithToleranceOperator;
        futures = new ConcurrentHashMap<>();
    }

    @Override
    public Future<Void> report(SinkRecord record, Throwable error) {
        Future<Void> future = this.retryWithToleranceOperator.executeFailed(Stage.TASK_PUT, SinkTask.class, record, error);
        if (!future.isDone()) {
            TopicPartition partition = new TopicPartition(record.topic(), record.kafkaPartition());
            this.futures.computeIfAbsent(partition, p -> new ArrayList<>()).add(future);
        }

        return future;
    }

    public void awaitFutures(Collection<TopicPartition> topicPartitions) {
        futuresFor(topicPartitions).forEach(future -> {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                log.error("Encountered an error while awaiting an errant record future's completion.", e);
                throw new ConnectException(e);
            }
        });
    }

    public void cancelFutures(Collection<TopicPartition> topicPartitions) {
        futuresFor(topicPartitions).forEach(future -> {
            try {
                future.cancel(true);
            } catch (Exception e) {
                log.error("Encountered an error while cancelling an errant record future", e);
                // No need to throw the exception here; it's enough to log an error message
            }
        });
    }

    private Collection<Future<Void>> futuresFor(Collection<TopicPartition> topicPartitions) {
        return topicPartitions.stream()
                .map(futures::remove)
                .filter(Objects::nonNull)
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }
}
