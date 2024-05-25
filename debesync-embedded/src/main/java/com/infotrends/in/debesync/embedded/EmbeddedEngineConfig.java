package com.infotrends.in.debesync.embedded;

import io.debezium.config.Field;
import org.apache.kafka.common.config.ConfigDef;

public interface EmbeddedEngineConfig {
    Field ENGINE_NAME = Field.create("name")
            .withDescription("Unique name for this connector instance.").required();
    Field CONNECTOR_CLASS = Field.create("connector.class")
            .withDescription("The Java class for the connector").required();
    Field PREDICATES = Field.create("predicates")
           .withDisplayName("List of prefixes defining predicates.").withType(ConfigDef.Type.STRING)
           .withWidth(ConfigDef.Width.MEDIUM).withImportance(ConfigDef.Importance.LOW)
           .withDescription("Optional list of predicates that can be assigned to transformations. " +
                   "The predicates are defined using '<predicate.prefix>.type' config option and " +
                   "configured using options '<predicate.prefix>.<option>'");
    Field TRANSFORMS = Field.create("transforms")
            .withDisplayName("List of prefixes defining transformations.").withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM).withImportance(ConfigDef.Importance.LOW)
            .withDescription("Optional list of single message transformations applied on the messages. " +
                    "The transforms are defined using '<transform.prefix>.type' config option and configured using " +
                    "options '<transform.prefix>.<option>'");
    Field ERRORS_RETRY_DELAY_INITIAL_MS = Field.create("errors.retry.delay.initial.ms")
            .withDisplayName("Initial delay for retries").withType(ConfigDef.Type.INT)
            .withWidth(ConfigDef.Width.SHORT).withImportance(ConfigDef.Importance.MEDIUM)
            .withDefault(300).withValidation(Field::isPositiveInteger)
            .withDescription("Initial delay (in ms) for retries when encountering connection errors. " +
                    "This value will be doubled upon every retry but won't exceed 'errors.retry.delay.max.ms'.");
    Field ERRORS_RETRY_DELAY_MAX_MS = Field.create("errors.retry.delay.max.ms")
            .withDisplayName("Max delay between retries").withType(ConfigDef.Type.INT).withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM).withDefault(10000)
            .withValidation(Field::isPositiveInteger)
            .withDescription("Max delay (in ms) between retries when encountering connection errors.");

    int DEFAULT_ERROR_MAX_RETRIES = -1;

    String TOPIC_NAMES_CONFIG = ".topics";
    String TOPIC_NAMES_REGEX_CONFIG = ".topics.regex";
    Field ERRORS_MAX_RETRIES = Field.create("errors.max.retries")
            .withDisplayName("The maximum number of retries").withType(ConfigDef.Type.INT)
            .withWidth(ConfigDef.Width.SHORT).withImportance(ConfigDef.Importance.MEDIUM)
            .withDefault(-1).withValidation(Field::isInteger)
            .withDescription("The maximum number of retries on connection errors before failing " +
                    "(-1 = no limit, 0 = disabled, > 0 = num of retries).");
}
