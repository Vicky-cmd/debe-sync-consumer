package com.infotrends.in.debesync.api.errors;

import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ErrantRecordFuture implements Future<Void> {

    private final List<Future<RecordMetadata>> futures;

    public ErrantRecordFuture(List<Future<RecordMetadata>> producerFutures) {
        futures = producerFutures;
    }

    public boolean cancel(boolean mayInterruptIfRunning) {
        throw new UnsupportedOperationException("Reporting an errant record cannot be cancelled.");
    }

    public boolean isCancelled() {
        return false;
    }

    public boolean isDone() {
        return futures.stream().allMatch(Future::isDone);
    }

    public Void get() throws InterruptedException, ExecutionException {
        for (Future<RecordMetadata> future: futures) {
            future.get();
        }
        return null;
    }

    public Void get(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        for (Future<RecordMetadata> future: futures) {
            future.get(timeout, unit);
        }
        return null;
    }
}
