package com.infotrends.in.debesync.mysql.sink.connectors;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class MysqlConnectorConfig {

    public enum ConnectorOpMode {
        SCHEMA_ONLY(true, false),
        INITIAL(true, true),
        NEVER(false, true);

        public static ConnectorOpMode parseString(String opString) {
             switch (opString) {
                 case "schema_only": return SCHEMA_ONLY;
                 case "initial": return INITIAL;
                 case "never": return NEVER;
                 default: throw new IllegalArgumentException("Unsupported operation type for connector operation mode - " + opString);
            }
        }

        private final boolean loadSchema;
        private final boolean loadData;

        ConnectorOpMode(boolean loadSchema, boolean loadData) {
            this.loadSchema = loadSchema;
            this.loadData = loadData;
         }

        public boolean loadSchema() {
            return this.loadSchema;
        }
        public boolean loadData() {
            return this.loadData;
        }
    }
}
