//package com.infotrends.in.debesync.kafka.errors.dlq;
//
//import EmbeddedHeader;
//import ErrorRecordsReporter;
//import io.debezium.config.Configuration;
//import io.debezium.config.Field;
//import io.debezium.engine.Header;
//import org.apache.kafka.clients.producer.KafkaProducer;
//import org.apache.kafka.clients.producer.ProducerRecord;
//import org.apache.kafka.common.config.ConfigDef;
//import org.apache.kafka.connect.sink.SinkRecord;
//
//import java.nio.charset.StandardCharsets;
//import java.util.Collections;
//import java.util.List;
//import java.util.concurrent.CompletableFuture;
//import java.util.concurrent.ExecutionException;
//import java.util.concurrent.Future;
//import java.util.stream.Collectors;
//
//public class KafkaErrantRecordReporterOld extends ErrorRecordsReporter {
//
//    Field DLQ_DESTINATION_TOPIC = Field.create(CONFIGURATION_PREFIX + "topic.name")
//            .withDisplayName("Error Records DLQ Topic Name")
//            .withType(ConfigDef.Type.STRING)
//            .withImportance(ConfigDef.Importance.HIGH)
//            .withDescription("The destination topic name for the errored records")
//            .withDefault("${topic}-dlq")
//            .required();
//
//    private String dlqTopicName;
//    private boolean enableHeaders;
//    private KafkaProducer<String, String> kafkaProducer;
//
//    public KafkaErrantRecordReporterOld(Configuration config) {
//        super(config);
//        this.dlqTopicName = this.config.getString(DLQ_DESTINATION_TOPIC);
//        this.enableHeaders = this.config.getBoolean(ENABLE_HEADERS);
//        Configuration kafkaProducerConfig = this.config.subset(CONFIGURATION_PREFIX + "producer.", true);
//        this.kafkaProducer = new KafkaProducer<>(kafkaProducerConfig.asProperties());
//    }
//
//    @Override
//    public Future<Void> report(SinkRecord record, Throwable error) {
//        final byte[] key = this.keyConverter.fromConnectData(record.topic(), record.keySchema(), record.key());
//        final byte[] value = this.valueConverter.fromConnectData(record.topic(), record.valueSchema(), record.value());
//        List<Header<?>> headers = Collections.emptyList();
//        if (this.headerConverter != null) {
//            List<Header<byte[]>> byteArrayHeaders = this.convertHeaders(record);
//            headers = (List) byteArrayHeaders;
//            if (this.shouldConvertHeadersToString()) {
//                headers = byteArrayHeaders.stream()
//                        .map(h -> new EmbeddedHeader(h.getKey(), new String(h.getValue(), StandardCharsets.UTF_8)))
//                        .collect(Collectors.toList());
//            }
//        }
//        org.apache.kafka.common.header.Headers kafkaHeaders = this.convertKafkaHeaders(headers);
//        String destinationTopicName = dlqTopicName.replace("${topic}", record.topic());
//
//        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(destinationTopicName, new String(key, StandardCharsets.UTF_8), new String(value, StandardCharsets.UTF_8));
//        kafkaHeaders.forEach(header -> producerRecord.headers().add(header));
//        return CompletableFuture.runAsync(() -> {
//            try {
//                this.kafkaProducer.send(producerRecord).get();
//            } catch (InterruptedException | ExecutionException e) {
//                throw new RuntimeException(e);
//            }
//        });
//    }
//}
