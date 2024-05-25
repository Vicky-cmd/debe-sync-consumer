package com.infotrends.in.debesync.api.connectors;

import com.infotrends.in.debesync.api.ConsumerSyncEngine;
import com.infotrends.in.debesync.api.entities.SourceRecord;
import com.infotrends.in.debesync.api.errors.DebeSyncException;
import com.infotrends.in.debesync.api.sink.contexts.storage.ConsumerTaskContext;
import com.infotrends.in.debesync.api.sink.contexts.storage.EmittersSinkTaskContext;
import io.debezium.config.Configuration;
import io.debezium.engine.format.KeyValueHeaderChangeEventFormat;
import io.debezium.engine.format.SerializationFormat;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

@Slf4j
@SuppressWarnings("raw")
public abstract class AbstractConnector<R> extends ChangeConnectorConfig implements ConsumerSyncEngine.ChangeConnector<R> {

    protected Configuration config;
    protected ConsumerSyncEngine.Builder<SourceRecord> builder;
    protected ConsumerSyncEngine<SourceRecord> engine;
    protected Properties props;

    protected ConsumerSyncEngine.ChangeEmitter<SourceRecord> emitter;

    final String defaultConnectorName = "DebeSync-Consumer";

    protected ConsumerTaskContext sinkTaskContext;

    @PostConstruct
    public void setup() {
        Config config = ConfigProvider.getConfig();
    }

    @Override
    public <K extends SerializationFormat<?>, V extends SerializationFormat<?>, H extends SerializationFormat<?>>
        void configure(ConsumerSyncEngine.Builder<SourceRecord> builder,
                          KeyValueHeaderChangeEventFormat<K, V, H> format,
                          ConsumerSyncEngine.ChangeEmitter<SourceRecord> emitter,
                          Properties props) throws DebeSyncException {
        log.info("Running Kafka Connector");
        this.builder = builder;
        this.emitter = emitter;
        this.props = props;
        this.config = Configuration.from(this.props);
        SinkConnectorConfig connectorConfig = new SinkConnectorConfig(new Plugins(Collections.emptyMap()),
                this.config.subset("sink.", true).asMap());
        this.sinkTaskContext = new EmittersSinkTaskContext(
                this.config.asMap(),
                connectorConfig,
                new ConnectorTaskId(defaultConnectorName, 0),
                format,
                this.emitter
        );
    }

    @Override
    public void close() throws IOException {
        if (this.engine!=null) {
            this.engine.stop();
        }
        if (this.emitter!=null) {
            this.emitter.close();
        }
    }
}
