package com.infotrends.in.debesync.jdbc.storage.kafka;

import com.infotrends.in.debesync.api.errors.DebeSyncException;
import com.infotrends.in.debesync.jdbc.storage.SchemaHistoryStorageReader;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.document.DocumentReader;
import io.debezium.relational.history.HistoryRecord;
import io.vavr.control.Try;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigDef;

import java.time.Duration;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

@Slf4j
@SuppressWarnings("unused")
public class KafkaSchemaHistoryStorageReader extends SchemaHistoryStorageReader {

    private final KafkaConsumer<byte[], byte[]> consumer;

    final AtomicReference<Thread> runningThread = new AtomicReference<>();

    private final DocumentReader reader = DocumentReader.defaultReader();

    private static final String DEFAULT_CONSUMER_GROUP_ID = "debezium-consumer";

    Field KAFKA_READER = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "kafka.")
            .withDisplayName("kafka schema history reader")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("The configuration for the kafka schema history reader.")
            .required();

    Field TOPIC_NAME = Field.create("topic")
            .withDisplayName("The topic name to be consumed for reading the schema history changes")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("The topic having the ddl/schema history events published by debezium.")
            .required();

    private final String topicName;
    private static final Integer PARTITION = 0;

    public KafkaSchemaHistoryStorageReader(Configuration config) {
        super(config);
        Configuration kafkaProps = this.config.subset(KAFKA_READER.name(), true);
        this.topicName = kafkaProps.getString(TOPIC_NAME);

        Configuration consumerProps = kafkaProps.subset("consumer.", true).edit()
                .withDefault(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
                .withDefault(ConsumerConfig.GROUP_ID_CONFIG, DEFAULT_CONSUMER_GROUP_ID)
                .withDefault(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")
                .withDefault(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")
                .withDefault(ProducerConfig.MAX_BLOCK_MS_CONFIG, Long.toString(Long.MAX_VALUE))
                .withDefault(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
                .withDefault(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, false)
                .withDefault(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                .build();
        this.consumer = new KafkaConsumer<>(consumerProps.asProperties());
        this.consumer.subscribe(List.of(topicName));
    }

    @Override
    public void startConsumer(Consumer<HistoryRecord> consumer) {
        if (!this.runningThread.compareAndSet(null, Thread.currentThread())) return;

        try {
            while (this.runningThread.get() != null && !isConsumerInSync()) {
                ConsumerRecords<byte[], byte[]> consumerRecords =
                        this.consumer.poll(Duration.ofMillis(1000));
                log.debug(String.format("Reading %s History Record Changes from the Kafka Topic", consumerRecords.count()));
                consumerRecords.forEach(record -> Try.run(() -> consumer.accept(new HistoryRecord(reader.read(record.value()))))
                        .onSuccess(ignored -> log.debug("Successfully processed for the record with offset - " + record.offset()))
                        .onFailure(throwable -> log.error(String.format("Error occurred while reading the History record from Kafka. Error: %s. Message: %s",
                                throwable.getClass().getSimpleName(), throwable.getMessage())))
                        .getOrElseThrow(throwable -> new DebeSyncException(String.format("Error occurred while reading the History record from Kafka. Error: %s. Message: %s",
                            throwable.getClass().getSimpleName(), throwable.getMessage()), throwable)));
                log.debug("Completed processing of the batch. Committing the offsets...");
                this.consumer.commitSync();
                log.debug("Completed committing the offsets for the batch.");
            }
        } finally {
            if (this.runningThread.get()!=null) {
                log.info("Successfully Completed the schema creation and initial load of the application...");
            } else {
                log.error("Forcefully Terminated the schema creation process");
            }
        }
    }

    private boolean isConsumerInSync() {
        try {
            OptionalLong lag = this.consumer.currentLag(new TopicPartition(this.topicName, PARTITION));
            return lag.isPresent() && lag.getAsLong() <= 0;
        } catch (IllegalStateException e) {
            return false;
        }
    }

    @Override
    public void close() {
        this.runningThread.set(null);
        this.consumer.close();
    }
}
