package com.infotrends.in.debesync.health;

import com.infotrends.in.debesync.api.ConsumerSyncEngine;
import com.infotrends.in.debesync.core.events.ConnectorStartedEvent;
import com.infotrends.in.debesync.api.health.TaskMetrics;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Event;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.Liveness;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Liveness
@ApplicationScoped
public class ConsumerSyncHealth implements HealthCheck, ConsumerSyncEngine.CallbackSubscription, ConsumerSyncEngine.CompletionCallback {

    private boolean live;
    private String connectorStatus;
    private String taskStatus;

    @Inject
    Event<ConnectorStartedEvent> connectorStartedEvent;

    private long TOTAL_MESSAGES_COUNT = 0;
    private long SUCCESSFULLY_PROCESSED_COUNT = 0;
    private long FAILED_COUNT = 0;

    @Override
    public void connectorStarted() {
        log.info("Debe-sync-consumer - Started the connector application");
        connectorStartedEvent.fire(new ConnectorStartedEvent("Started Kafka Connector"));
        this.connectorStatus="STARTED";
        this.live = true;
    }

    @Override
    public void connectorStopped() {
        log.info("Debe-sync-consumer - Stopped the connector application");
        this.connectorStatus="STOPPED";
        this.live = false;
    }

    @Override
    public void taskStarted() {
        log.info("Debe-sync-consumer - Started the connector Task");
        this.taskStatus="STARTED";
    }

    @Override
    public void taskStatus(TaskMetrics metrics) {
        log.info("Updating the status");
        try {
            this.taskStatus = "RUNNING";
            TOTAL_MESSAGES_COUNT += metrics.getSuccessCount() + metrics.getErroredCount();
            SUCCESSFULLY_PROCESSED_COUNT += metrics.getSuccessCount();
            FAILED_COUNT += metrics.getErroredCount();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void taskStopped() {
        log.info("Debe-sync-consumer - Stopped the connector Task");
        this.taskStatus="STOPPED";
        this.live = false;
    }

    @Override
    public void completed(String status, String message, Throwable error) {
        this.taskStatus="COMPLETED";
        this.connectorStatus="COMPLETED";
        this.live = false;
    }

    @Override
    public HealthCheckResponse call() {
        return HealthCheckResponse.named("debe-sync-consumer").status(live)
                .withData("connector-status", connectorStatus)
                .withData("task-status", taskStatus)
                .withData("total-messages-count", TOTAL_MESSAGES_COUNT)
                .withData("successfully-processed-count", SUCCESSFULLY_PROCESSED_COUNT)
                .withData("failed-messages-count", FAILED_COUNT)
                .build();
    }

    public Map<String, Object> status() {
        Map<String, Object> status = new HashMap<>();
        status.put("connector-status", connectorStatus);
        status.put("task-status", taskStatus);
        status.put("total-messages-count", TOTAL_MESSAGES_COUNT);
        status.put("successfully-processed-count", SUCCESSFULLY_PROCESSED_COUNT);
        status.put("failed-messages-count", FAILED_COUNT);
        return status;
    }
}
