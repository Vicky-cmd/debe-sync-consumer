package com.infotrends.in.debesync.mongo.processors;

import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;
import com.mongodb.kafka.connect.sink.processor.PostProcessor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.sink.SinkRecord;

@Slf4j
@SuppressWarnings("unused")
public class LoggingPostProcessor extends PostProcessor {

    public LoggingPostProcessor(MongoSinkTopicConfig config) {
        super(config);
    }

    @Override
    public void process(SinkDocument doc, SinkRecord orig) {
        log.info("----- Applying the Logging Post Processor -----");
        log.info(String.format("Message Origin Details - Topic: %s. Kafka offsets: %s", orig.topic(), orig.kafkaOffset()));
        log.info(String.format("Key Document - %s", doc.getKeyDoc()));
        log.info(String.format("Value Document - %s", doc.getValueDoc()));
        log.info("----- Completed Logging the events -----");
    }
}
