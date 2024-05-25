package com.infotrends.in.debesync.kafka.emitters;

import io.debezium.config.Field;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.errors.ToleranceType;

import java.util.Set;

public interface KafkaChangeEmitterConfig {

    Field ERRORS_TOLERANCE_CONFIG = Field.create(ConnectorConfig.ERRORS_TOLERANCE_CONFIG)
                    .withType(ConfigDef.Type.STRING)
            .withDefault(ConnectorConfig.ERRORS_TOLERANCE_DEFAULT.value())
            .withAllowedValues(Set.of(ToleranceType.NONE.value(), ToleranceType.ALL.value()))
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withWidth(ConfigDef.Width.SHORT)
            .withDisplayName(ConnectorConfig.ERRORS_TOLERANCE_DISPLAY)
            .withDescription(ConnectorConfig.ERRORS_TOLERANCE_DOC);
    public static String KAFKA_CONFIG_PREFIX = "kafka.consumer.";

    Field BOOTSTRAP_SERVERS = Field.create(KAFKA_CONFIG_PREFIX + "bootstrap.servers")
            .withDisplayName("Bootstrap Servers")
            .withDescription("The bootstrap servers for the kafka consumer configuration")
            .withImportance(ConfigDef.Importance.HIGH)
            .withWidth(ConfigDef.Width.LONG)
            .withDefault("localhost:9092")
            .required();
}
