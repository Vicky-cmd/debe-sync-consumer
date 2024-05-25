package com.infotrends.in.debesync.core;

import com.infotrends.in.debesync.api.ConsumerSyncEngine;
import com.infotrends.in.debesync.api.entities.SourceRecord;
import com.infotrends.in.debesync.core.events.ConnectorStartedEvent;
import com.infotrends.in.debesync.core.events.ShutdownEvent;
import com.infotrends.in.debesync.health.ConsumerSyncHealth;
import com.infotrends.in.debesync.api.errors.DebeSyncException;
import io.debezium.DebeziumException;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.format.*;
import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.Startup;
import io.quarkus.runtime.StartupEvent;
import io.vavr.control.Try;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.control.ActivateRequestContext;
import jakarta.enterprise.context.spi.CreationalContext;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.spi.Bean;
import jakarta.enterprise.inject.spi.BeanManager;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.ConfigException;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.health.Liveness;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
@Startup
@ApplicationScoped
public class ConsumerSyncServer {

    private static final String PROP_PREFIX = "debezium.";
    public static final String PROP_SOURCE_PREFIX = PROP_PREFIX + "source.";
    private static final String PROP_SINK_PREFIX = PROP_PREFIX + "sink.";
    private static final String PROP_CONNECTOR_PREFIX = PROP_PREFIX + "connector.";

    final String DEFAULT_EMITTER_IMPLEMENTATION = "com.infotrends.in.debesync.core.consumers.kafka.KafkaChangeEmitter";

    private static final String KEY_FORMAT = PROP_PREFIX + "format.key";
    private static final String VALUE_FORMAT = PROP_PREFIX + "format.value";
    private static final String HEADER_FORMAT = PROP_PREFIX + "format.header";
    private static final String FORMAT_JSON = Json.class.getSimpleName().toLowerCase();
    private static final String FORMAT_JSON_BYTE_ARRAY = JsonByteArray.class.getSimpleName().toLowerCase();
    private static final String FORMAT_AVRO = Avro.class.getSimpleName().toLowerCase();
    private static final String FORMAT_CLOUD_EVENT = CloudEvents.class.getSimpleName().toLowerCase();

    final ExecutorService executor = Executors.newFixedThreadPool(1);

    private final Properties props = new Properties();
    private final Properties debeziumProps = new Properties();
    private ConsumerSyncEngine.ChangeEmitter<SourceRecord> sourceEmitter;
    private ConsumerSyncEngine.ChangeConnector<ChangeEvent<Object, Object>> connector;

    @Inject
    @Liveness
    protected ConsumerSyncHealth health;

    @Inject
    BeanManager beanManager;

    public ConsumerSyncServer() {}

    @ActivateRequestContext
    public void startUp(@Observes StartupEvent startupEvent) {
        try {
            startConsumer();
        } catch (DebeSyncException | ConfigException | DebeziumException e) {
            if (e.getCause()!=null)
                log.error(String.format("Quarkus application failed with the %s with the error - %s. Caused by - %s",
                    e.getClass().getSimpleName(), e.getMessage(), e.getCause()));
            else
                log.error(String.format("Quarkus application failed with the %s with the error - %s",
                        e.getClass().getSimpleName(), e.getMessage()));

            Try.run(this::destroy);
            Quarkus.asyncExit();
            Quarkus.waitForExit();
        }
    }

    private void startConsumer() {
        Config config = this.loadConfigOrDie();
        log.info("Starting the Debesync Consumer Application");
        String sinkType = config.getValue("debezium.sink.type", String.class);
        log.info("Detected Sink Type: " + sinkType);
        String connectorType = config.getOptionalValue("debezium.connector.type", String.class)
                .orElse("default-connector");
        String sourceEmitterName = config.getOptionalValue("debezium.source.connector.class", String.class)
                .orElse(DEFAULT_EMITTER_IMPLEMENTATION);
        log.info(String.format("Utilizing the emitter implementation: %s", sourceEmitterName));
        initializeSinkConnector(connectorType);
        this.sourceEmitter = initializeSourceConsumer(sourceEmitterName);

        ConsumerSyncEngine.copyConfigToProps(config, this.debeziumProps, PROP_PREFIX, "", true);
        ConsumerSyncEngine.copyConfigToProps(config, this.props, PROP_PREFIX, "", true);
        ConsumerSyncEngine.copyConfigToProps(config, this.props, PROP_SOURCE_PREFIX, "", true);
        ConsumerSyncEngine.copyConfigToProps(config, this.props, PROP_SINK_PREFIX, "", true);
        ConsumerSyncEngine.copyConfigToProps(config, this.props, PROP_CONNECTOR_PREFIX, "", true);

        Class<? extends SerializationFormat<?>> keyFormat = this.getFormat(config, KEY_FORMAT);
        Class<? extends SerializationFormat<?>> valueFormat = this.getFormat(config, VALUE_FORMAT);
        Class<? extends SerializationFormat<?>> headerFormat = this.getHeaderFormat(config);
        log.debug(String.format("Serialization Formats used - Key: %s. Value: %s. Header: %s.",
                keyFormat.getCanonicalName(), valueFormat.getCanonicalName(), headerFormat.getCanonicalName()));

        this.sourceEmitter.setup(debeziumProps);
        log.info("Debesync Application Thread: " + Thread.currentThread().getContextClassLoader().toString());
        ConsumerSyncEngine.Builder<SourceRecord> builder = ConsumerSyncEngine.Builder.builder(keyFormat, valueFormat, headerFormat)
                .using(this.debeziumProps).using(this.sourceEmitter)
                .using(Thread.currentThread().getContextClassLoader())
                .using((ConsumerSyncEngine.CallbackSubscription) health)
                .using((ConsumerSyncEngine.CompletionCallback) health);

        this.connector.configure(builder, KeyValueHeaderChangeEventFormat.of(keyFormat, valueFormat, headerFormat), this.sourceEmitter, this.props);

        log.info("Successfully Configured the Debesync Server. Running the Consumer Engine.");
        this.executor.execute(this.connector);

    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private ConsumerSyncEngine.ChangeEmitter<SourceRecord> initializeSourceConsumer(String sourceClassName) {

        try {
            Class<?> sourceClazz = Class.forName(sourceClassName);
            Constructor<?> sourceConstructor = sourceClazz.getConstructor();
            Object sourceInstance = sourceConstructor.newInstance();

            if (sourceInstance instanceof ConsumerSyncEngine.ChangeEmitter) {
                return (ConsumerSyncEngine.ChangeEmitter) sourceInstance;
            } else {
                throw new DebeSyncException(String.format("Unsupported implementation %s for the property - debezium.source.class", sourceClassName));
            }
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            throw new DebeSyncException(String.format("Unable to find valid implementation class %s for the property - debezium.source.class .Error - %s. Message: %s", sourceClassName, e.getClass().getSimpleName(), e.getMessage()), e);
        } catch (InvocationTargetException | InstantiationException | IllegalAccessException e) {
            throw new DebeSyncException(String.format("Unable to initialize the implementation class %s for the property - debezium.source.class .Error - %s. Message: %s", sourceClassName, e.getClass().getSimpleName(), e.getMessage()), e);
        }
    }

    public void handleConnectorStartedEvent(@Observes ConnectorStartedEvent event) {
        log.info("Connector StartUp Completed. " + event.getMessage());
    }

    public void handleShutdownEvent(@Observes ShutdownEvent event) throws InterruptedException {
        log.info("Shutting down the Debesync Consumer - Status: " + event.getStatus() + " Message: " + event.getMessage());
        if (event.getError()!=null) {
            log.error("Debesync Error triggering the shutdown event - " + event.getError() + " Error: " + event.getErrorMessage());
        }
        destroy();
    }

    @PreDestroy
    @SuppressWarnings("unused")
    public void destroy() throws InterruptedException {
        if (this.sourceEmitter != null)
            this.sourceEmitter.close();
        executor.shutdown();
        boolean isTerminated = executor.awaitTermination(1000, TimeUnit.MILLISECONDS);
        if (isTerminated)
            log.info("Successfully terminated the Debesync Application");
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void initializeSinkConnector(String sinkType) {
        Set<Bean<?>> connectorBeans = beanManager.getBeans(sinkType).stream()
                .filter(x -> ConsumerSyncEngine.ChangeConnector.class.isAssignableFrom(x.getBeanClass()))
                .collect(Collectors.toSet());

        if (connectorBeans.size() == 0) {
            throw new DebeziumException(String.format("No Consumers named - %s available", sinkType));
        } else if (connectorBeans.size() > 1) {
            throw new DebeziumException(String.format("Multiple Consumers named - %s were found", sinkType));
        } else {
            Bean<ConsumerSyncEngine.ChangeConnector<ChangeEvent<Object, Object>>> connectorBean = (Bean) connectorBeans.iterator().next();
            CreationalContext<ConsumerSyncEngine.ChangeConnector<ChangeEvent<Object, Object>>> connectorBeanContext = this.beanManager.createCreationalContext(connectorBean);
            this.connector = connectorBean.create(connectorBeanContext);
        }
    }

    private Config loadConfigOrDie() {
        Config config = ConfigProvider.getConfig();

        try {
            config.getValue("debezium.sink.type", String.class);
        } catch (NoSuchElementException e) {
            String configFile = Paths.get(System.getProperty("user.dir"), "conf", "application.properties").toString();
            log.info(String.format("Failed to load mandatory config value 'debezium.sink.type'. Please check you have a correct Debezium server config in %s or required properties are defined via system or environment variables.", configFile));
            Quarkus.asyncExit();
        }
        return config;
    }

    private Class<? extends SerializationFormat<?>> getFormat(Config config, String property) {
        String format = config.getOptionalValue(property, String.class).orElse(FORMAT_JSON);
        if (format.equalsIgnoreCase(FORMAT_JSON))
            return Json.class;
        else if (format.equalsIgnoreCase(FORMAT_JSON_BYTE_ARRAY))
            return JsonByteArray.class;
        else if (format.equalsIgnoreCase(FORMAT_AVRO))
            return Avro.class;
        else if (format.equalsIgnoreCase(FORMAT_CLOUD_EVENT))
            return CloudEvents.class;
        else
            throw new ConfigException(property, format, String.format("Unsupported format - %s for field - %s", format, property));
    }

    private Class<? extends SerializationFormat<?>> getHeaderFormat(Config config) {
        String format = config.getOptionalValue(ConsumerSyncServer.HEADER_FORMAT, String.class).orElse(FORMAT_JSON);
        if (format.equalsIgnoreCase(FORMAT_JSON))
            return Json.class;
        else if (format.equalsIgnoreCase(FORMAT_JSON_BYTE_ARRAY))
            return JsonByteArray.class;
        else
            throw new ConfigException(ConsumerSyncServer.HEADER_FORMAT, format, String.format("Unsupported format - %s for field - %s", format, ConsumerSyncServer.HEADER_FORMAT));
    }
}
