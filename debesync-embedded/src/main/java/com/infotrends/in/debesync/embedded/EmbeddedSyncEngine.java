package com.infotrends.in.debesync.embedded;

import com.infotrends.in.debesync.api.ConsumerSyncEngine;
import com.infotrends.in.debesync.api.entities.DebeSyncChangeRecord;
import com.infotrends.in.debesync.api.errors.DebeSyncException;
import com.infotrends.in.debesync.api.errors.RetryWithToleranceOperator;
import com.infotrends.in.debesync.api.health.TaskMetrics;
import com.infotrends.in.debesync.api.sink.contexts.storage.ConsumerTaskContext;
import com.infotrends.in.debesync.embedded.transforms.Transformations;
import io.debezium.config.Configuration;
import io.debezium.config.Instantiator;
import io.debezium.engine.StopEngineException;
import io.debezium.util.DelayStrategy;
import io.vavr.control.Try;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.errors.Stage;
import org.apache.kafka.connect.runtime.errors.ToleranceType;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
@NoArgsConstructor
public class EmbeddedSyncEngine<R> implements ConsumerSyncEngine<R>, EmbeddedEngineConfig {

    public static class EmbeddedSyncEngineBuilder<R> implements Builder<R> {
        private Properties props;
        private ChangeEmitter<R> emitter;
        private ConsumerTaskContext consumerTaskContext;
        private CompletionCallback callback = (status, message, error) -> {};
        private CallbackSubscription subscription;
        private ClassLoader classLoader = Instantiator.getClassLoader();

        private EmbeddedSyncEngineBuilder() {}

        public static <T> EmbeddedSyncEngineBuilder<T> builder() {
            return new EmbeddedSyncEngineBuilder<>();
        }

        @Override
        public Builder<R> using(Properties props) {
            this.props = props;
            return this;
        }

        @Override
        public Builder<R> using(ChangeEmitter<R> emitter) {
            this.emitter=emitter;
            return this;
        }

        @Override
        public Builder<R> using(ConsumerTaskContext consumerTaskContext) {
            this.consumerTaskContext = consumerTaskContext;
            return this;
        }

        @Override
        public Builder<R> using(CallbackSubscription subscription) {
            this.subscription = subscription;
            return this;
        }

        @Override
        public Builder<R> using(CompletionCallback callback) {
            this.callback = callback;
            return this;
        }

        @Override
        public Builder<R> using(ClassLoader classLoader) {
            if (classLoader!=null)
                this.classLoader = classLoader;
            return this;
        }

        @Override
        public ConsumerSyncEngine<R> build() {
            return new EmbeddedSyncEngine<>(this.props, this.emitter, this.callback,
                    this.subscription, this.classLoader, this.consumerTaskContext);
        }
    }

    private  ChangeEmitter<R> emitter;
    private Configuration config;
    private SinkConnector sinkConnector;
    private SinkTask sinkTask;
    private ConsumerTaskContext consumerTaskContext;

    private List<String> sourceTopics;

    private final AtomicReference<Thread> runningThread = new AtomicReference<>();

    private Optional<CompletionCallback> callback;

    private Optional<CallbackSubscription> subscription;
    private ClassLoader classLoader;

    private Transformations transformations;

    private ToleranceType errorTolerance;

    private RetryWithToleranceOperator retryWithToleranceOperator;

    final long DEFAULT_CONSUMER_SLEEP_INTERVAL = 100;

    private Long nullRecordsRefreshInterval;


    public EmbeddedSyncEngine(Properties props, ChangeEmitter<R> emitter, CompletionCallback callback,
                              CallbackSubscription subscription, ClassLoader classLoader,
                              ConsumerTaskContext consumerTaskContext) {
        this.config = Configuration.from(props);
        this.emitter = emitter;
        this.callback = Optional.ofNullable(callback);
        this.subscription = Optional.ofNullable(subscription);
        this.consumerTaskContext = consumerTaskContext;
        this.sourceTopics = new ArrayList<>();
        this.classLoader = classLoader;

        this.retryWithToleranceOperator = this.consumerTaskContext.retryWithToleranceOperator();
        this.transformations = new Transformations(this.config);

        this.errorTolerance = this.errorToleranceType(this.config.getString(ConnectorConfig.ERRORS_TOLERANCE_CONFIG));
        this.nullRecordsRefreshInterval = this.config.getLong("null-records.refresh.interval", DEFAULT_CONSUMER_SLEEP_INTERVAL);
    }

    @Override
    public boolean isRunning() {
        return this.runningThread.get() != null;
    }

    @Override
    public void stop() {
        try {
            this.stopConnectorAndTask();
        } finally {
            if (this.runningThread.get() == Thread.currentThread()) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void run() {
        if (this.runningThread.compareAndSet(null,
                Thread.currentThread())) {
            try {
                String sinkType = this.config.getString("type");
                log.debug(String.format("Configured Sink Type as %s", sinkType));
                this.subscribeToTopics(sinkType);

                String sinkConnectorName = this.config.getString(CONNECTOR_CLASS);
                log.debug("Configured Sink Connector: " + sinkConnectorName);
                try {
                    this.initiateSinkConnector(sinkConnectorName);
                    log.debug("Completed initializing the sink connector");
                    this.subscription.ifPresent(CallbackSubscription::connectorStarted);
                    log.info("Found instance for the sink Connector - " + sinkConnectorName);
                    log.debug("Initializing the sink Tasks");
                    this.startSinkTask((Class<? extends SinkTask>) this.sinkConnector.taskClass(), this.config.subset(sinkType, true).asMap());
                    this.subscription.ifPresent(CallbackSubscription::taskStarted);
                    log.debug("Successfully started and configured the sink Connector and the tasks");
                } catch (RetriableException e) {
                    log.error("Error occurred while starting up the sink connector/task. Executing the handleRetryableExceptions to restart the tasks");
                    Exception err = this.handleRetryableExceptions(e, sinkConnectorName, sinkType);
                    if (err!=null) {
                        throw new DebeSyncException(String.format("Unable to restart the connector connections... Error - %s. Message: %s",
                                err.getClass().getSimpleName(), err.getMessage()), err);
                    }
                    log.info("Successfully started connector and tasks after retries...");
                }
                log.info("Starting processing the records.");
                this.pollRecords();
            } finally {
                this.callback.ifPresent(callback -> callback.completed("completed", "The data load has completed", null));
                this.stop();
            }
        }
    }

    private void startSinkTask(Class<? extends SinkTask> sinkTaskClazz, Map<String, String> taskConfigMap) {

        this.sinkTask = Try.of(() -> sinkTaskClazz.getDeclaredConstructor().newInstance())
                .onSuccess((task) -> {
                    log.debug("Successfully created an instance of the Sink Task - " + task.getClass().getCanonicalName());
                    task.initialize(this.consumerTaskContext);
                    task.start(taskConfigMap);
                    log.debug("Successfully initialized the Sink Task.");
                }).onFailure(throwable -> log.error(String.format("Error Occurred while initiating the Sink Connector. Error - %s. Message: %s",
                        throwable.getClass().getName(), throwable.getMessage())))
                .getOrElseThrow((throwable) -> {
                    throw new DebeSyncException(String.format("Error Occurred while initiating the Sink Connector. Error - %s. Message: %s",
                            throwable.getClass().getName(), throwable.getMessage()), throwable);
                });
    }

    @SuppressWarnings("unchecked")
    private Exception handleRetryableExceptions(RetriableException e, String sinkConnectorName, String sinkType) {
        int maxRetryCount = this.getErrorsMaxRetries();
        log.info(String.format("Retryable exception thrown, connector will be restarted; errors.max.retries=%s", maxRetryCount));
        if (maxRetryCount == 0) {
            log.debug("Retry is disabled. Throwing the generated error");
            return e;
        } else if (maxRetryCount <= EmbeddedEngineConfig.DEFAULT_ERROR_MAX_RETRIES) {
            log.warn(String.format("Setting %s=%d is deprecated. To disable retries on connection errors, set %s=0", ERRORS_MAX_RETRIES.name(), maxRetryCount,
                    ERRORS_MAX_RETRIES.name()));
            return e;
        }
        DelayStrategy delayStrategy = this.delayStrategy();
        int retryCount = 0;
        boolean connectorStarted = false;
        while (!connectorStarted) {
            try {
                retryCount++;
                log.info(String.format("Attempting to start connector. Attempt - %s", retryCount));
                this.stopConnectorAndTask();

                this.initiateSinkConnector(sinkConnectorName);
                this.subscription.ifPresent(CallbackSubscription::connectorStarted);
                log.debug("Successfully initialized sink connector");
                this.startSinkTask((Class<? extends SinkTask>) this.sinkConnector.taskClass(),
                        this.config.subset(sinkType, true).asMap());
                this.subscription.ifPresent(CallbackSubscription::taskStarted);
                connectorStarted = true;
                log.debug("Successfully started and configured the sink Connector and the tasks");
            } catch (Exception err) {
                log.error(String.format("Error occurred while attempting to restart connector. Error - %s. Message: %s",
                        e.getClass().getSimpleName(), e.getMessage()));
                if (retryCount == maxRetryCount) {
                    log.error("Max Retry count for restarting the connectors exceeded. Exiting...");
                    return err;
                }
            }
            log.debug("Exponential Backoff strategy configured. Will retry initializing and starting the sink connector and tasks.");
            delayStrategy.sleepWhen(!connectorStarted);
        }

        return null;
    }

    private void stopConnectorAndTask() {
        if (this.sinkTask!=null)
            this.sinkTask.stop();
        if (this.sinkConnector!=null)
            this.sinkConnector.stop();
        this.subscription.ifPresent(CallbackSubscription::taskStopped);
        this.subscription.ifPresent(CallbackSubscription::connectorStopped);
    }


    private DelayStrategy delayStrategy() {
        return DelayStrategy.exponential(Duration.ofMillis(this.config.getInteger(ERRORS_RETRY_DELAY_INITIAL_MS)),
                Duration.ofMillis(this.config.getInteger(ERRORS_RETRY_DELAY_MAX_MS)));
    }

    private int getErrorsMaxRetries() {
        return config.getInteger(EmbeddedEngineConfig.ERRORS_MAX_RETRIES);
    }

    private void pollRecords() {
        log.debug("Emitter type/execution mode - " + this.emitter.type().name());
        if (this.emitter.type().equals(ChangeEmitter.EmitterType.POLL_MODE)) {
            this.emitter.run(this::pollRecords);
        } else {
            pollEmitterForRecords();
        }
    }

    private void pollEmitterForRecords() {
        while (this.runningThread.get() != null) {
            try {
                log.debug("Polling the emitter for new records...");
                List<? extends R> records = this.emitter.poll();
                if (records==null || records.isEmpty()) {
                    log.debug("No records available in the emitter. Will retry...");
                    this.emitter.commit();
                    sleep();
                    continue;
                }
                log.debug(String.format("Total Number of records fetched - %s", records.size()));
                this.pollRecords(records);
                log.debug("Completed processing the records. Committing the offset.");
                this.emitter.commit();
            } catch (Exception e) {
                log.error("Error occurred while polling the emitter. Error - " + e.getClass().getSimpleName() + " Message: " + e.getMessage());
                if (this.errorTolerance.equals(ToleranceType.NONE))
                    throw e;
            }
        }
    }

    private void pollRecords(List<? extends R> records) {

        if (records == null) return;
        log.debug(String.format("Total Number of records before applying transformations: %s", records.size()));
        List<SinkRecord> sinkRecords = this.transformRecords(records);
        if (sinkRecords == null) {
            log.debug(String.format("No records found after applying the transformations: %s", records.size()));
            return;
        }
        log.debug(String.format("Total Number of records after applying the transformations: %s", records.size()));

        try {
            log.debug("Invoking the put operation of the Sink Task to publish the records to target");
            this.sinkTask.put(sinkRecords);
            this.subscription.ifPresent(subscription -> subscription.taskStatus(TaskMetrics.builder()
                    .successCount(sinkRecords.size())
                    .build()));
        } catch (StopEngineException e) {
            sinkRecords.forEach(record ->
                    this.retryWithToleranceOperator.executeFailed(Stage.TASK_PUT,
                            EmbeddedSyncEngine.class, record, e));
            throw e;
        } catch (Exception e) {
            sinkRecords.forEach(record ->
                    this.retryWithToleranceOperator.executeFailed(Stage.TASK_PUT,
                            EmbeddedSyncEngine.class, record, e));
            log.error("Error occurred while putting the messages to the sink task. Error - " + e.getClass().getSimpleName() + " Message: " + e.getMessage());
            if (this.errorTolerance.equals(ToleranceType.NONE))
                throw e;
        }
    }

    private List<SinkRecord> transformRecords(List<? extends R> records) {
        try {
            log.debug("Applying the transformations for the records.");
            return records.stream()
                    .filter(Objects::nonNull)
                    .map(record -> ((DebeSyncChangeRecord<?, ?, ?>) record).sinkRecord())
                    .map(record ->
                            this.retryWithToleranceOperator.execute(
                                () -> this.transformations.apply(record),
                                Stage.TRANSFORMATION,
                                this.getClass()
                            )
                    )
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
        } catch (Exception e) {
            log.error("Error occurred while polling the emitter. Error - " + e.getClass().getSimpleName() + " Message: " + e.getMessage());
            this.subscription.ifPresent(subscription -> subscription.taskStatus(TaskMetrics.builder()
                    .erroredCount(records.size())
                    .build()));
            if (this.errorTolerance.equals(ToleranceType.NONE))
                throw e;
            else
                return null;
        }
    }

    private void sleep() {
        try {
            log.debug(String.format("Sleeping for %s - ", this.nullRecordsRefreshInterval));
            Thread.sleep(this.nullRecordsRefreshInterval);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    private void subscribeToTopics(String sinkType) {
        Optional<String> topicsStr = Optional.ofNullable(this.config.getString(sinkType + TOPIC_NAMES_CONFIG));
        Optional<String> topicNameRegex = Optional.ofNullable(this.config.getString(sinkType + TOPIC_NAMES_REGEX_CONFIG));
        if (topicsStr.isPresent() && topicNameRegex.isPresent()) {
            throw new ConfigException(String.format("%s and %s are mutually exclusive properties, but both are set.",
                    TOPIC_NAMES_CONFIG, TOPIC_NAMES_REGEX_CONFIG));
        } else if (topicsStr.isEmpty() && topicNameRegex.isEmpty()) {
            throw new ConfigException(String.format("Please configure at least one of the %s and %s properties.",
                    TOPIC_NAMES_CONFIG, TOPIC_NAMES_REGEX_CONFIG));
        } else if (StringUtils.isNotEmpty(topicsStr.get())) {
            this.sourceTopics = List.of(topicsStr.get().split(","));
            log.debug(String.format("Subscribing to the topics: %s", this.sourceTopics));
            this.emitter.subscribe(this.sourceTopics.toArray(new String[0]));
        } else if (StringUtils.isNotEmpty(topicNameRegex.get())) {
            Pattern sourceTopicsRegex = Pattern.compile(topicNameRegex.get());
            log.debug(String.format("Subscribing to the topics (regex): %s", topicNameRegex.get()));
            this.emitter.subscribe(sourceTopicsRegex);
        } else {
            throw new ConfigException(String.format("Invalid configuration for the %s and %s properties.",
                    TOPIC_NAMES_CONFIG, TOPIC_NAMES_REGEX_CONFIG));
        }

    }

    @SuppressWarnings("unchecked")
    private void initiateSinkConnector(String sinkConnectorName) {
        log.info(sinkConnectorName);
        this.sinkConnector = Try.of(() -> {
            Class<? extends SinkConnector> sinkConnectorClazz = (Class<? extends SinkConnector>)
                    this.classLoader.loadClass(sinkConnectorName);
            return sinkConnectorClazz
                    .getDeclaredConstructor().newInstance();
        }).onFailure(e -> log.error(String.format("Error Occurred while initiating the Sink Connector. Error - %s. Message - %s",
                e.getClass().getName(), e.getMessage()))).getOrElseThrow(throwable ->
            new DebeSyncException(String.format("Error Occurred while initiating the Sink Connector. Error - %s. Message: %s",
                throwable.getClass().getName(), throwable.getMessage()), throwable));
    }

    public ToleranceType errorToleranceType(String tolerance) {
        for (ToleranceType type: ToleranceType.values()) {
            if (type.name().equalsIgnoreCase(tolerance)) {
                return type;
            }
        }
        return ConnectorConfig.ERRORS_TOLERANCE_DEFAULT;
    }
}
