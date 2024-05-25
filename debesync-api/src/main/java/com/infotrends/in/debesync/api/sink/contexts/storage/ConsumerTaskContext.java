package com.infotrends.in.debesync.api.sink.contexts.storage;

import com.infotrends.in.debesync.api.errors.RetryWithToleranceOperator;
import org.apache.kafka.connect.sink.SinkTaskContext;

public interface ConsumerTaskContext extends SinkTaskContext {
    RetryWithToleranceOperator retryWithToleranceOperator();
}
