package com.infotrends.in.debesync.api.errors;

import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.Future;

public interface ErrorReporter extends AutoCloseable {
    Future<RecordMetadata> report(ProcessingContext context);

    @Override
    default void close() { }
}
