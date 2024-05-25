package com.infotrends.in.debesync.mysql.sink.connectors;

import com.infotrends.in.debesync.api.ConsumerSyncEngine;
import com.infotrends.in.debesync.api.connectors.AbstractConnector;
import com.infotrends.in.debesync.api.entities.SourceRecord;
import com.infotrends.in.debesync.api.errors.DebeSyncException;
import com.infotrends.in.debesync.mysql.sink.connectors.MysqlConnectorConfig.ConnectorOpMode;
import com.infotrends.in.debesync.jdbc.storage.SchemaHistoryStorageReader;
import com.infotrends.in.debesync.mysql.schemahistory.generator.MysqlSchemaGenerator;
import io.debezium.engine.format.KeyValueHeaderChangeEventFormat;
import io.debezium.engine.format.SerializationFormat;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Named;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Properties;

import static com.infotrends.in.debesync.jdbc.storage.SchemaHistoryStorageReader.READER_TYPE;

@Slf4j
@Dependent
@Named("mysql")
public class EmbeddedMysqlConnector extends AbstractConnector<SourceRecord> {

    private MysqlSchemaGenerator schemaGenerator;
    private SchemaHistoryStorageReader reader;

    @Override
    public <K extends SerializationFormat<?>, V extends SerializationFormat<?>, H extends SerializationFormat<?>>
        void configure(ConsumerSyncEngine.Builder<SourceRecord> builder,
                          KeyValueHeaderChangeEventFormat<K, V, H> format,
                          ConsumerSyncEngine.ChangeEmitter<SourceRecord> emitter,
                          Properties props) throws DebeSyncException {
        super.configure(builder, format, emitter, props);
        log.debug("Configuring an instance of MYSQL Schema Generator...");
        schemaGenerator = new MysqlSchemaGenerator(this.config);
        log.debug("Completed configuring the schema generator");
    }

    @Override
    public void run() {

        ConnectorOpMode opMode = ConnectorOpMode.parseString(this.config.getString(CONNECTOR_OP_MODE, "never"));
        log.info(String.format("Starting the MYSQL Connector in '%s' mode", opMode.name()));
        if (opMode.loadSchema()) {
            log.info("Loading the schema from the schema history data created by debezium");
            loadSchema();
        }

        if (!opMode.loadData()) {
            log.info("Skipping data load as the operation mode is set as - " + opMode.name().toLowerCase());
            return;
        }
        log.info("Starting the the consumer for loading the data.");
        this.engine = this.builder.using(this.sinkTaskContext)
                .build();
        this.engine.run();
    }

    @Override
    public void close() throws IOException {
        super.close();
        if (this.reader!=null) {
            this.reader.close();
        }
    }
    
    private void loadSchema() {
        reader = this.config.getInstance(READER_TYPE, SchemaHistoryStorageReader.class, this.config);
        log.debug(String.format("Starting the %s reader for reading the schema changes from debezium source connector",
                reader.getClass().getCanonicalName()));
        reader.startConsumer(schemaGenerator);
    }
}
