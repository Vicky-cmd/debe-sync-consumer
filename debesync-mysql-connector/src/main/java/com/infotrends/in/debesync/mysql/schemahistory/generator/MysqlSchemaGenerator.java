package com.infotrends.in.debesync.mysql.schemahistory.generator;

import com.infotrends.in.debesync.api.errors.DebeSyncException;
import io.debezium.config.Configuration;
import io.debezium.engine.StopEngineException;
import io.debezium.relational.history.HistoryRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.errors.ToleranceType;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;


@Slf4j
public class MysqlSchemaGenerator implements Consumer<HistoryRecord> {

    private final Connection connection;

    private final List<String> sourceDatabases = new ArrayList<>();

    private final List<String> ALLOWED_DDL_QUERY_PREFIX = List.of("CREATE TABLE", "ALTER TABLE", "RENAME TABLE", "DROP TABLE");

    private final ToleranceType toleranceType;

    private final boolean executeDeleteQueries;

    private final String DELETE_DDL_PREFIX = "DROP TABLE";

    public MysqlSchemaGenerator(Configuration config) {
        Configuration mysqlConfig = config.subset("mysql", true);
        String connectionURL = mysqlConfig.getString("connection.url");
        log.info("Setting up connection to the target database=" + connectionURL);
        String username = mysqlConfig.getString("connection.user");
        String password = mysqlConfig.getString("connection.password");
        String driver = mysqlConfig.getString("driver", "com.mysql.cj.jdbc.Driver");
        this.toleranceType = this.errorToleranceType(config.getString(ConnectorConfig.ERRORS_TOLERANCE_CONFIG));
        if (StringUtils.isNotBlank(config.getString("sourceDatabases"))) {
            this.sourceDatabases.addAll(List.of(config.getString("sourceDatabases").split(",")));
            log.debug(String.format("Detected source database filters for populating the schema - %s", this.sourceDatabases));
        }
        this.executeDeleteQueries = config.getBoolean("enable.delete", false);

        try {
            log.debug("Using Driver instance - " + driver);
            Class.forName(driver);
            this.connection = DriverManager.getConnection(connectionURL, username, password);
            String targetDatabase = this.connection.getCatalog();
            log.debug(String.format("Connected to target database - %s", targetDatabase));
        } catch (ClassNotFoundException e) {
            throw new DebeSyncException(String.format("Unable to find a valid implementation for the %s class. Error - %s. Message: %s", driver, e.getClass().getSimpleName(), e.getMessage()), e);
        } catch (SQLException e) {
            throw new DebeSyncException(String.format("Error occurred while connecting to the target sink database. - %s. Message: %s", e.getClass().getCanonicalName(), e.getMessage()), e);
        }
        log.debug("Completed Schema Generator Configuration");
    }

    @Override
    public void accept(HistoryRecord record) {
        if (!this.sourceDatabases.isEmpty()
                && !this.sourceDatabases.contains(record.document()
                .getString(HistoryRecord.Fields.DATABASE_NAME))) {
            log.debug(String.format("Skipping Record History event for the database - %s", record.document()
                    .getString(HistoryRecord.Fields.DATABASE_NAME)));
            return;
        }

        String ddlQuery = record.document().getString(HistoryRecord.Fields.DDL_STATEMENTS);
        if (!isValidDDl(ddlQuery)) {
            log.debug(String.format("Skipping query %s as it does not match the supported ddl operations for tables", ddlQuery));
            return;
        }

        if (!this.executeDeleteQueries && ddlQuery.startsWith(DELETE_DDL_PREFIX)) {
            log.debug("Disabled the execution of the Delete Queries. Skipping ddl - " + ddlQuery);
            return;
        }

        for (String database: sourceDatabases) {
            if (ddlQuery.contains(String.format("`%s`", database))) {
                ddlQuery = ddlQuery.replace(String.format("`%s`.", database), "");
            }
        }

        try {
            log.debug("Executing the ddl query over the target Database - " + ddlQuery);
            this.connection.createStatement().executeUpdate(ddlQuery);
        } catch (SQLException e) {
            if (this.toleranceType.equals(ToleranceType.NONE))
                throw new StopEngineException(String.format("Error occurred while executing the create statement - %s. Error - %s - Message: %s", ddlQuery, e.getClass().getCanonicalName(), e.getMessage()));
            else
                log.error(String.format("Error occurred while executing the ddl query - %s. Error: %s. Message: %s", ddlQuery, e.getClass().getSimpleName(), e.getMessage()));
        }
    }

    private boolean isValidDDl(String ddl) {
        return this.ALLOWED_DDL_QUERY_PREFIX.stream()
                .anyMatch(ddl::startsWith);
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
