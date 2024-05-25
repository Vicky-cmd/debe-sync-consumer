package com.infotrends.in.debesync.api.errors;

import com.infotrends.in.debesync.api.entities.SourceRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.errors.Stage;
import org.apache.kafka.connect.runtime.errors.WorkerErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class ProcessingContext implements AutoCloseable{
    private Collection<ErrorReporter> reporters = Collections.emptyList();

    private ConsumerRecord<byte[], byte[]> consumedMessage;
    private SinkRecord sinkRecord;
    private SourceRecord sourceRecord;

    private Stage position;
    private Class<?> klass;
    private int attempt;
    private Throwable error;

    private void reset() {
        attempt = 0;
        position = null;
        klass = null;
        error = null;
    }

    public void consumerRecord(ConsumerRecord<byte[], byte[]> consumedMessage) {
        this.consumedMessage = consumedMessage;
        reset();
    }

    public void sourceRecord(SourceRecord sourceRecord) {
        this.sourceRecord = sourceRecord;
        reset();
    }

    public ConsumerRecord<byte[], byte[]> consumerRecord() {
        return consumedMessage;
    }

    public SinkRecord sinkRecord() {
        return sinkRecord;
    }

    public SourceRecord sourceRecord() {
        return sourceRecord;
    }

    public void sinkRecord(SinkRecord record) {
        this.sinkRecord = record;
        reset();
    }

    public void position(Stage position) {
        this.position = position;
    }

    public Stage stage() {
        return position;
    }

    public Class<?> executingClass() {
        return klass;
    }

    public void executingClass(Class<?> klass) {
        this.klass = klass;
    }

    public void currentContext(Stage stage, Class<?> klass) {
        position(stage);
        executingClass(klass);
    }

    public Future<Void> report() {
        if (reporters.size() == 1) {
            return new WorkerErrantRecordReporter.ErrantRecordFuture(Collections.singletonList(reporters.iterator().next().report(this)));
        }

        List<Future<RecordMetadata>> futures = reporters.stream()
                .map(r -> r.report(this))
                .filter(f -> !f.isDone())
                .collect(Collectors.toList());
        if (futures.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        return new WorkerErrantRecordReporter.ErrantRecordFuture(futures);
    }

    @Override
    public String toString() {
        return toString(false);
    }

    public String toString(boolean includeMessage) {
        StringBuilder builder = new StringBuilder();
        builder.append("Executing stage '");
        builder.append(stage().name());
        builder.append("' with class '");
        builder.append(executingClass() == null ? "null" : executingClass().getName());
        builder.append('\'');
        if (includeMessage && sinkRecord() != null) {
            builder.append(", where source record is = ");
            builder.append(sinkRecord());
        } else if (includeMessage && consumerRecord() != null) {
            ConsumerRecord<byte[], byte[]> msg = consumerRecord();
            builder.append(", where consumed record is ");
            builder.append("{topic='").append(msg.topic()).append('\'');
            builder.append(", partition=").append(msg.partition());
            builder.append(", offset=").append(msg.offset());
            if (msg.timestampType() == TimestampType.CREATE_TIME || msg.timestampType() == TimestampType.LOG_APPEND_TIME) {
                builder.append(", timestamp=").append(msg.timestamp());
                builder.append(", timestampType=").append(msg.timestampType());
            }
            builder.append("}");
        }
        builder.append('.');
        return builder.toString();
    }

    public void attempt(int attempt) {
        this.attempt = attempt;
    }

    public int attempt() {
        return attempt;
    }

    public Throwable error() {
        return error;
    }

    public void error(Throwable error) {
        this.error = error;
    }

    public boolean failed() {
        return error() != null;
    }

    public void reporters(Collection<ErrorReporter> reporters) {
        Objects.requireNonNull(reporters);
        this.reporters = reporters;
    }

    @Override
    public void close() {
        ConnectException e = null;
        for (ErrorReporter reporter : reporters) {
            try {
                reporter.close();
            } catch (Throwable t) {
                e = e != null ? e : new ConnectException("Failed to close all reporters");
                e.addSuppressed(t);
            }
        }
        if (e != null) {
            throw e;
        }
    }
}
