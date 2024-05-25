package com.infotrends.in.debesync.embedded.sink.connectors;

import com.infotrends.in.debesync.api.connectors.AbstractConnector;
import com.infotrends.in.debesync.api.entities.SourceRecord;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Named;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Dependent
@Named("default-connector")
public class EmbeddedConnector extends AbstractConnector<SourceRecord> {

    @Override
    public void run() {
        this.engine = this.builder.using(this.sinkTaskContext)
                .build();
        this.engine.run();
    }
}
