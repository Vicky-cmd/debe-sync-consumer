package com.infotrends.in.debesync.kafka.emitters;

import com.infotrends.in.debesync.api.ConsumerSyncEngine;
import com.infotrends.in.debesync.api.entities.EmbeddedHeader;
import com.infotrends.in.debesync.api.entities.SourceRecord;
import com.infotrends.in.debesync.api.errors.dlq.DeadLetterQueueReporter;
import com.infotrends.in.debesync.kafka.errors.dlq.KafkaDlqErrantRecordReporter;
import io.debezium.config.Configuration;
import io.debezium.engine.Header;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.errors.ToleranceType;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
@SuppressWarnings("unused")
public class KafkaChangeEmitter implements ConsumerSyncEngine.ChangeEmitter<SourceRecord>, KafkaChangeEmitterConfig {

    public static final String PROP_SOURCE_PREFIX = "source.";
    private final static String KAFKA_EMITTER_PREFIX = PROP_SOURCE_PREFIX + "kafka.";
    private final static String KAFKA_CONSUMER_PREFIX = KAFKA_EMITTER_PREFIX + "consumer.";
    private KafkaConsumer<byte[], byte[]> kafkaConsumer;
    private final AtomicReference<Thread> runningThread = new AtomicReference<>();

    private Configuration config;
    List<String> topicNames;

    Pattern topicNameRegex;

    private static final String DEFAULT_CONSUMER_GROUP_ID = "debezium-consumer";

    private long pollDuration;
    private long commitDuration;

    private ToleranceType errorTolerance;

    final long DEFAULT_CONSUMER_SLEEP_INTERVAL = 100;

    private long nullRecordsRefreshInterval;

    @Override
    public void setup(Properties config) {
        log.info("Inside setup for the Kafka Change Producer");
        this.config = Configuration.from(config);
        log.info(this.config.toString());
        this.errorTolerance = this.errorToleranceType(this.config.getString("sink." + ConnectorConfig.ERRORS_TOLERANCE_CONFIG));
        this.nullRecordsRefreshInterval = this.config.getLong("null-records.refresh.interval", DEFAULT_CONSUMER_SLEEP_INTERVAL);
        this.initConsumer();
    }

    @Override
    public Set<TopicPartition> assignment() {
        return this.kafkaConsumer.assignment();
    }

    @Override
    public void subscribe(String ...topicNames) {
        this.topicNames = List.of(topicNames);
        this.kafkaConsumer.subscribe(this.topicNames);
    }

    @Override
    public void subscribe(String topicName) {
        this.topicNames = List.of(topicName);
        this.kafkaConsumer.subscribe(this.topicNames);
    }

    @Override
    public void subscribe(Pattern topicNameRegex) {
        this.topicNameRegex = topicNameRegex;
        this.kafkaConsumer.subscribe(topicNameRegex);
    }

    @Override
    public List<PartitionInfo> partitions(String topicName) {
        return this.kafkaConsumer.partitionsFor(topicName);
    }

    @Override
    public Map<TopicPartition, Long> offsets(String topicName) {
        List<PartitionInfo> partitionInfo = this.partitions(topicName);
        List<TopicPartition> topicPartitions = partitionInfo.stream()
                .map(info -> new TopicPartition(info.topic(), info.partition()))
                .collect(Collectors.toList());
        return this.kafkaConsumer.endOffsets(topicPartitions);
    }

    @Override
    public void pause(TopicPartition... partitions) {
        this.kafkaConsumer.pause(List.of(partitions));
    }

    @Override
    public void resume(TopicPartition... partitions) {
        this.kafkaConsumer.resume(List.of(partitions));
    }

    @Override
    public void run(Consumer<List<SourceRecord>> consumer) {
        if (!this.runningThread.compareAndSet(null,
                Thread.currentThread())) {
            return;
        }
        while (this.runningThread.get() != null) {
            try {
                log.debug("Polling the kafka for new records...");
                List<SourceRecord> records = this.poll();
                if (records==null || records.size()==0) {
                    log.debug("No records available in the emitter. Will retry...");
                    this.commit();
                    this.sleep();
                    continue;
                }
                log.debug(String.format("Total Number of records fetched - %s", records.size()));
                consumer.accept(records);
                log.debug("Completed processing the records. Committing the offset.");
                this.commit();
            } catch (Exception e) {
                log.error(String.format("Error occurred while running the runner implementation for the kafka emitter. Error - %s. Message: %s",
                        e.getClass().getSimpleName(), e.getMessage()));
                if (this.errorTolerance.equals(ToleranceType.NONE))
                    throw e;
            }
        }

    }

    @Override
    public List<SourceRecord> poll() {
        ConsumerRecords<byte[], byte[]> records = this.kafkaConsumer.poll(Duration.ofMillis(this.pollDuration));
        return this.convertRecords(records);
    }

    private List<SourceRecord> convertRecords(ConsumerRecords<byte[], byte[]> records) {
        List<SourceRecord> connectRecords = new ArrayList<>();
        records.forEach(record -> {
            log.debug("Logging the Record Details:");
            log.debug("Topic Name: " + record.topic());
            log.debug("Offset: " + record.offset());
            log.debug("Key: " + new String(record.key(), StandardCharsets.UTF_8));
            log.debug("Value: " + new String(record.value(), StandardCharsets.UTF_8));
            log.debug("Headers: " + record.headers());
            List<Header<byte[]>> headers = new ArrayList<>();
            if(record.headers()!=null)
                record.headers().forEach(header -> headers.add(new EmbeddedHeader<>(header.key(), header.value())));
            SourceRecord connectRecord = new SourceRecord(
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.timestamp(),
                    TimestampType.CREATE_TIME,
                    record.key(),
                    record.value(),
                    headers
            );
            connectRecords.add(connectRecord);
        });
        return connectRecords;
    }

    @Override
    public void commit() {
        this.kafkaConsumer.commitSync(Duration.ofMillis(this.commitDuration));
    }

    @Override
    public void close() {
        if(this.kafkaConsumer!=null) {
            this.kafkaConsumer.close();
        }
    }

    @Override
    public Class<? extends DeadLetterQueueReporter> deadLetterQueueReporter() {
        return KafkaDlqErrantRecordReporter.class;
    }

    @Override
    public EmitterType type() {
        return EmitterType.RUNNER_MODE;
    }

    private void initConsumer() {
        if (this.kafkaConsumer!=null) return;

        Configuration consumerProperties = this.config.subset(KAFKA_CONSUMER_PREFIX, true)
                .edit()
                .withDefault(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
                .withDefault(ConsumerConfig.GROUP_ID_CONFIG, DEFAULT_CONSUMER_GROUP_ID)
                .withDefault(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")
                .withDefault(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")
                .withDefault(ProducerConfig.MAX_BLOCK_MS_CONFIG, Long.toString(Long.MAX_VALUE))
                .withDefault(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
                 .withDefault(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                .build();
        log.info(consumerProperties.toString());
        this.pollDuration = consumerProperties.getLong(KAFKA_EMITTER_PREFIX + "poll.duration", 1000L);
        this.commitDuration = consumerProperties.getLong(KAFKA_EMITTER_PREFIX + "commit.duration", 1000L);

        this.kafkaConsumer =new KafkaConsumer<>(consumerProperties.asProperties());
    }

    public ToleranceType errorToleranceType(String tolerance) {
        for (ToleranceType type: ToleranceType.values()) {
            if (type.name().equalsIgnoreCase(tolerance)) {
                return type;
            }
        }
        return ConnectorConfig.ERRORS_TOLERANCE_DEFAULT;
    }

    private void sleep() {
        try {
            log.debug(String.format("Sleeping for %s - ", this.nullRecordsRefreshInterval));
            Thread.sleep(this.nullRecordsRefreshInterval);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
