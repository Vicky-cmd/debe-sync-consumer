package com.infotrends.in.debesync.api.errors;

import com.infotrends.in.debesync.api.entities.SourceRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.runtime.errors.Operation;
import org.apache.kafka.connect.runtime.errors.Stage;
import org.apache.kafka.connect.runtime.errors.ToleranceType;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@Slf4j
public class RetryWithToleranceOperator implements AutoCloseable {

    private static final Map<Stage, Class<? extends Exception>> TOLERABLE_EXCEPTIONS = new HashMap<>();
    static {
        TOLERABLE_EXCEPTIONS.put(Stage.TRANSFORMATION, Exception.class);
        TOLERABLE_EXCEPTIONS.put(Stage.HEADER_CONVERTER, Exception.class);
        TOLERABLE_EXCEPTIONS.put(Stage.KEY_CONVERTER, Exception.class);
        TOLERABLE_EXCEPTIONS.put(Stage.VALUE_CONVERTER, Exception.class);
    }

    public static final long RETRIES_DELAY_MIN_MS = 300;
    private final long errorRetryTimeout;
    private final long errorMaxDelayInMillis;
    private final ToleranceType errorToleranceType;
    protected final ProcessingContext context;
    private final CountDownLatch stopRequestedLatch;
    private final Time time;
    private volatile boolean stopping;

    private long totalFailures = 0;

    public RetryWithToleranceOperator(long errorRetryTimeout, long errorMaxDelayInMillis,
                                      ToleranceType toleranceType, Time time) {
        this(errorRetryTimeout, errorMaxDelayInMillis, toleranceType, time, new ProcessingContext(), new CountDownLatch(1));
    }

    public RetryWithToleranceOperator(long errorRetryTimeout, long errorMaxDelayInMillis,
                                      ToleranceType toleranceType, Time time,
                                      ProcessingContext context, CountDownLatch stopRequestedLatch) {
        this.errorRetryTimeout = errorRetryTimeout;
        this.errorMaxDelayInMillis = errorMaxDelayInMillis;
        this.errorToleranceType = toleranceType;
        this.context = context;
        this.time = time;
        this.stopRequestedLatch = stopRequestedLatch;
        this.stopping = false;
    }

    public synchronized Future<Void> executeFailed(Stage stage, Class<?> executingClass,
                                                   ConsumerRecord<byte[], byte[]> consumerRecord,
                                                   Throwable error) {
        markAsFailed();
        context.consumerRecord(consumerRecord);
        context.currentContext(stage, executingClass);
        context.error(error);
        Future<Void> errantRecordFuture = context.report();
        if (!withinToleranceLimits()) {
            throw new ConnectException("Tolerance exceeded in error handler", error);
        }
        return errantRecordFuture;
    }

    public synchronized Future<Void> executeFailed(Stage stage, Class<?> executingClass,
                                                   SinkRecord sinkRecord,
                                                   Throwable error) {

        markAsFailed();
        context.sinkRecord(sinkRecord);
        context.currentContext(stage, executingClass);
        context.error(error);
        Future<Void> errantRecordFuture = context.report();
        if (!withinToleranceLimits()) {
            throw new ConnectException("Tolerance exceeded in Source Worker error handler", error);
        }
        return errantRecordFuture;
    }

    public synchronized <V> V execute(Operation<V> operation, Stage stage, Class<?> executingClass) {
        context.currentContext(stage, executingClass);

        if (context.failed()) {
            log.debug("ProcessingContext is already in failed state. Ignoring requested operation.");
            return null;
        }

        try {
            Class<? extends Exception> ex = TOLERABLE_EXCEPTIONS.getOrDefault(context.stage(), RetriableException.class);
            return execAndHandleError(operation, ex);
        } finally {
            if (context.failed()) {
                context.report();
            }
        }
    }

    protected <V> V execAndHandleError(Operation<V> operation, Class<? extends Exception> tolerated) {
        try {
            V result = execAndRetry(operation);
            if (context.failed()) {
                markAsFailed();
            }
            return result;
        } catch (Exception e) {
            markAsFailed();
            context.error(e);

            if (!tolerated.isAssignableFrom(e.getClass())) {
                throw new ConnectException("Unhandled exception in error handler", e);
            }

            if (!withinToleranceLimits()) {
                throw new ConnectException("Tolerance exceeded in error handler", e);
            }

            return null;
        }
    }

    protected <V> V execAndRetry(Operation<V> operation) throws Exception {
        int attempt = 0;
        long startTime = time.milliseconds();
        long deadline = (errorRetryTimeout >= 0) ? startTime + errorRetryTimeout : Long.MAX_VALUE;
        do {
            try {
                attempt++;
                return operation.call();
            } catch (RetriableException e) {
                log.trace("Caught a retriable exception while executing {} operation with {}", context.stage(), context.executingClass());
                if (time.milliseconds() < deadline) {
                    backoff(attempt, deadline);
                } else {
                    log.trace("Can't retry. start={}, attempt={}, deadline={}", startTime, attempt, deadline);
                    context.error(e);
                    return null;
                }
                if (stopping) {
                    log.trace("Shutdown has been scheduled. Marking operation as failed.");
                    context.error(e);
                    return null;
                }
            } finally {
                context.attempt(attempt);
            }
        } while (true);
    }

    void backoff(int attempt, long deadline) {
        int numRetry = attempt - 1;
        long delay = RETRIES_DELAY_MIN_MS << numRetry;
        if (delay > errorMaxDelayInMillis) {
            delay = ThreadLocalRandom.current().nextLong(errorMaxDelayInMillis);
        }
        long currentTime = time.milliseconds();
        if (delay + currentTime > deadline) {
            delay = Math.max(0, deadline - currentTime);
        }
        log.debug("Sleeping for up to {} millis", delay);
        try {
            stopRequestedLatch.await(delay, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ignored) {
        }
    }

    @SuppressWarnings("fallthrough")
    public synchronized boolean withinToleranceLimits() {
        switch (errorToleranceType) {
            case NONE:
                if (totalFailures > 0) return false;
            case ALL:
                return true;
            default:
                throw new ConfigException("Unknown tolerance type: {}", errorToleranceType);
        }
    }

    void markAsFailed() {
        totalFailures++;
    }

    @Override
    public String toString() {
        return "RetryWithToleranceOperator{" +
                "errorRetryTimeout=" + errorRetryTimeout +
                ", errorMaxDelayInMillis=" + errorMaxDelayInMillis +
                ", errorToleranceType=" + errorToleranceType +
                ", totalFailures=" + totalFailures +
                ", time=" + time +
                ", context=" + context +
                '}';
    }

    public synchronized void reporters(List<ErrorReporter> reporters) {
        this.context.reporters(reporters);
    }

    @SuppressWarnings("unused")
    public synchronized void sinkRecord(SinkRecord preTransformRecord) {
        this.context.sinkRecord(preTransformRecord);
    }

    @SuppressWarnings("unused")
    public synchronized void consumerRecord(ConsumerRecord<byte[], byte[]> consumedMessage) {
        this.context.consumerRecord(consumedMessage);
    }

    public synchronized void sourceRecord(SourceRecord record) {
        this.context.sourceRecord(record);
    }

    public synchronized boolean failed() {
        return this.context.failed();
    }

    public synchronized Throwable error() {
        return this.context.error();
    }

    @SuppressWarnings("unused")
    public void triggerStop() {
        stopping = true;
        stopRequestedLatch.countDown();
    }

    @Override
    public synchronized void close() {
        this.context.close();
    }
}
