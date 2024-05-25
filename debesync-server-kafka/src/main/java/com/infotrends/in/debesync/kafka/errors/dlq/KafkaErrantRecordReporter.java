//package com.infotrends.in.debesync.kafka.errors.dlq;
//
//import ErrorRecordsReporter;
//import io.debezium.config.Configuration;
//import org.apache.kafka.connect.sink.SinkRecord;
//
//import java.util.concurrent.Future;
//
//public class KafkaErrantRecordReporter extends ErrorRecordsReporter {
//
//    public KafkaErrantRecordReporter(Configuration config) {
//        super(config);
//    }
//
//    @Override
//    public Future<Void> report(SinkRecord record, Throwable error) {
//    return null;
//    }
//}
