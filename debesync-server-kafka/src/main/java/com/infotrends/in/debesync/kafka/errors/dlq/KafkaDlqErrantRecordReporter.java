package com.infotrends.in.debesync.kafka.errors.dlq;

import com.infotrends.in.debesync.api.entities.EmbeddedHeader;
import com.infotrends.in.debesync.api.entities.SourceRecord;
import com.infotrends.in.debesync.api.errors.ProcessingContext;
import com.infotrends.in.debesync.api.errors.dlq.DeadLetterQueueReporter;
import io.debezium.config.Configuration;
import io.debezium.engine.Header;
import io.debezium.engine.format.KeyValueHeaderChangeEventFormat;
import io.debezium.engine.format.SerializationFormat;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.util.ConnectorTaskId;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static org.apache.kafka.connect.runtime.errors.DeadLetterQueueReporter.*;

public class KafkaDlqErrantRecordReporter extends DeadLetterQueueReporter {

    private String dlqTopicName;
    protected String defaultClientId = "debesync-consumer-" + UUID.randomUUID().toString();

    private KafkaProducer<byte[], byte[]> kafkaProducer;

    public <K extends SerializationFormat<?>, V extends SerializationFormat<?>, H extends SerializationFormat<?>>
        KafkaDlqErrantRecordReporter(ConnectorTaskId connectorTaskId, Configuration config, SinkConnectorConfig connectorConfig,
                                                                                                                                                 KeyValueHeaderChangeEventFormat<K, V, H> format) {
        super(connectorTaskId, config, connectorConfig, format);
        this.dlqTopicName = Optional.ofNullable(this.connectorConfig.dlqTopicName()).orElse("${topic}-dlq");

        Configuration kafkaProducerConfig = config.subset(SinkConnectorConfig.DLQ_PREFIX + "producer.", true)
                .edit()
                .withDefault(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
                .withDefault(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
                .withDefault(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
                .withDefault(ProducerConfig.MAX_BLOCK_MS_CONFIG, Long.toString(Long.MAX_VALUE))
                .withDefault(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false")
                .withDefault(ProducerConfig.ACKS_CONFIG, "all")
                .withDefault(ProducerConfig.CLIENT_ID_CONFIG, defaultClientId)
                .build();
        this.kafkaProducer = new KafkaProducer<>(kafkaProducerConfig.asProperties());
    }

    @Override
    public Future<RecordMetadata> report(ProcessingContext context) {
        if (context.consumerRecord()!=null)
            return this.reportForConsumerRecord(context);
        else if (context.sinkRecord()!=null)
            return this.reportForSinkRecord(context);
        else if (context.sourceRecord()!=null)
            return this.reportForSourceRecord(context);
        else
            return CompletableFuture.completedFuture(null);
    }

    private Future<RecordMetadata> reportForConsumerRecord(ProcessingContext context) {

        ConsumerRecord<byte[], byte[]> record = context.consumerRecord();
        String destinationTopicName = this.createKafkaTopicForRecord(record.topic());
        ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(
                destinationTopicName,
                null,
                record.timestamp() == RecordBatch.NO_TIMESTAMP? null:record.timestamp(),
                record.key(),
                record.value(),
                record.headers()
        );
        return this.kafkaProducer.send(producerRecord);
    }

    private Future<RecordMetadata> reportForSourceRecord(ProcessingContext context) {
        SourceRecord record = context.sourceRecord();
        String destinationTopicName = this.createKafkaTopicForRecord(record.topic());
        org.apache.kafka.common.header.Headers kafkaHeaders = this.convertKafkaHeaders((List) record.headers());
        ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(
                destinationTopicName,
                null,
                record.timestamp() == RecordBatch.NO_TIMESTAMP? null:record.timestamp(),
                record.key(),
                record.value(),
                kafkaHeaders
        );
        return this.kafkaProducer.send(producerRecord);
    }

    private Future<RecordMetadata> reportForSinkRecord(ProcessingContext context) {
        SinkRecord record = context.sinkRecord();
        final byte[] key = this.keyConverter.fromConnectData(record.topic(), record.keySchema(), record.key());
        final byte[] value = this.valueConverter.fromConnectData(record.topic(), record.valueSchema(), record.value());
        List<Header<?>> headers = Collections.emptyList();
        if (this.headerConverter != null) {
            List<Header<byte[]>> byteArrayHeaders = this.convertHeaders(record);
            headers = (List) byteArrayHeaders;
            if (this.shouldConvertHeadersToString()) {
                headers = (List) byteArrayHeaders.stream()
                        .map(h -> new EmbeddedHeader(h.getKey(), new String(h.getValue(), StandardCharsets.UTF_8)))
                        .collect(Collectors.toList());
            }
        }
        String destinationTopicName = this.createKafkaTopicForRecord(record.topic());
        org.apache.kafka.common.header.Headers kafkaHeaders = this.convertKafkaHeaders(headers);
        ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(
                destinationTopicName,
                null,
                record.timestamp() == RecordBatch.NO_TIMESTAMP ? null: record.timestamp(),
                key,
                value);
        kafkaHeaders.forEach(header -> producerRecord.headers().add(header));
        if (connectorConfig.isDlqContextHeadersEnabled()) {
            this.populateContextHeaders(producerRecord, context);
        }
        return this.kafkaProducer.send(producerRecord);
    }

    private String createKafkaTopicForRecord(String recordTopic) {
        return dlqTopicName.replace("$topic", recordTopic);
    }

    @Override
    public void close() {
        super.close();
    }

    void populateContextHeaders(ProducerRecord<byte[], byte[]> producerRecord, ProcessingContext context) {
        Headers headers = producerRecord.headers();
        if (context.consumerRecord() != null) {
            headers.add(ERROR_HEADER_ORIG_TOPIC, toBytes(context.consumerRecord().topic()));
            headers.add(ERROR_HEADER_ORIG_PARTITION, toBytes(context.consumerRecord().partition()));
            headers.add(ERROR_HEADER_ORIG_OFFSET, toBytes(context.consumerRecord().offset()));
        }

        headers.add(ERROR_HEADER_CONNECTOR_NAME, toBytes(connectorTaskId.connector()));
        headers.add(ERROR_HEADER_TASK_ID, toBytes(String.valueOf(connectorTaskId.task())));
        headers.add(ERROR_HEADER_STAGE, toBytes(context.stage().name()));
        headers.add(ERROR_HEADER_EXECUTING_CLASS, toBytes(context.executingClass().getName()));
        if (context.error() != null) {
            headers.add(ERROR_HEADER_EXCEPTION, toBytes(context.error().getClass().getName()));
            headers.add(ERROR_HEADER_EXCEPTION_MESSAGE, toBytes(context.error().getMessage()));
            byte[] trace;
            if ((trace = stacktrace(context.error())) != null) {
                headers.add(ERROR_HEADER_EXCEPTION_STACK_TRACE, trace);
            }
        }
    }
}
