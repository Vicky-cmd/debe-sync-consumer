package com.infotrends.in.debesync.mongo.sink.connectors;

import com.infotrends.in.debesync.api.connectors.AbstractConnector;
import com.infotrends.in.debesync.api.entities.SourceRecord;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Named;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Dependent
@Named("mongo")
public class EmbeddedMongoConnector extends AbstractConnector<SourceRecord> {

    @Override
    public void run() {
        this.engine = this.builder.using(this.sinkTaskContext)
                .build();
        this.engine.run();
    }
}
