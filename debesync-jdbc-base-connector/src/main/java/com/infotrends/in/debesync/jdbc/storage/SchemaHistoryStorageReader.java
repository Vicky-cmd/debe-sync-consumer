package com.infotrends.in.debesync.jdbc.storage;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.relational.history.HistoryRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.ConfigDef;

import java.util.function.Consumer;

@Slf4j
public abstract class SchemaHistoryStorageReader {

    protected static String CONFIGURATION_FIELD_PREFIX_STRING = "schema.history.internal.";

    public static Field READER_TYPE = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "type")
            .withDisplayName("reader implementation Class Name")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("The implementation class for the reading the schema history.")
            .withValidation(Field::isOptional);

    Field NAME = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "name")
            .withDisplayName("Logical name for the database schema history")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("The name used for the database schema history, perhaps differently by each implementation.")
            .withValidation(Field::isOptional);

    protected Configuration config;


    protected SchemaHistoryStorageReader(Configuration config) {
        this.config = config;
    }

    public abstract void startConsumer(Consumer<HistoryRecord> consumer);

    public abstract void close();
}
