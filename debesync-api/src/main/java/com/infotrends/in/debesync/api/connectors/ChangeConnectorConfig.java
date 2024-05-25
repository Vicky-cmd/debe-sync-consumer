package com.infotrends.in.debesync.api.connectors;

import io.debezium.config.Field;
import org.apache.kafka.common.config.ConfigDef;

public class ChangeConnectorConfig {

    public Field CONNECTOR_OP_MODE = Field.create("operation.mode")
            .withDisplayName("The operation mode for the connector")
            .withDescription("The operation mode in which the connector runs. To either run the data to load schema or the capture data events")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .required();

}
