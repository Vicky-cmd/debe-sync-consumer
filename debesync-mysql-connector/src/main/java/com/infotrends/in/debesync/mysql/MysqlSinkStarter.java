package com.infotrends.in.debesync.mysql;

import com.infotrends.in.debesync.mysql.config.DebesyncMysqlSinkConfig;
import io.quarkus.runtime.Startup;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.control.ActivateRequestContext;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;

@Slf4j
@Startup
@ApplicationScoped
public class MysqlSinkStarter {

    @Inject
    DebesyncMysqlSinkConfig debesyncMongoSinkConfig;

    @ActivateRequestContext
    public void startUp(@Observes StartupEvent startupEvent) {
        System.out.println("Application Started!");
        Config config = ConfigProvider.getConfig();
        try {
            log.info("Loading className - " + Class.forName("io.confluent.connect.jdbc.JdbcSinkConnector").getSimpleName());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        String sinkType = config.getOptionalValue("debezium.sink.type", String.class).orElse("ANY");
        log.info("Detected the sink type - " + sinkType + " for the application");
        if (sinkType.equalsIgnoreCase("mysql")) {
            debesyncMongoSinkConfig.setup();
        }
    }
}
