package com.infotrends.in.debesync.mongo.namespace.mappings;

import com.infotrends.in.debesync.mongo.config.DebesyncMongoSinkConfig;
import com.mongodb.MongoNamespace;
import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;
import com.mongodb.kafka.connect.sink.namespace.mapping.NamespaceMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.sink.SinkRecord;

public class TopicNameSpaceMapper  implements NamespaceMapper {

    private String defaultDatabaseName;
    private String defaultCollectionName;
    private String topicSeparator;
    private Integer databaseLocation;
    private Integer collectionLocation;

    @Override
    public void configure(MongoSinkTopicConfig configuration) {
        this.defaultDatabaseName = configuration.getString(MongoSinkTopicConfig.DATABASE_CONFIG);
        this.defaultCollectionName = configuration.getString(MongoSinkTopicConfig.COLLECTION_CONFIG);
        if (StringUtils.isBlank(this.defaultCollectionName)) {
            this.defaultCollectionName = configuration.getTopic();
        }

        this.topicSeparator = configuration.getString(DebesyncMongoSinkConfig.FIELD_TOPIC_SEPARATOR);
        if (StringUtils.isBlank(this.topicSeparator)) {
            this.topicSeparator = DebesyncMongoSinkConfig.DEFAULT_TOPIC_SEPARATOR;
        }
        this.databaseLocation = configuration.getInt(DebesyncMongoSinkConfig.FIELD_DATABASE_LOCATION);
        if (this.databaseLocation == null) {
            this.databaseLocation = DebesyncMongoSinkConfig.DEFAULT_DATABASE_LOCATION;
        }
        this.collectionLocation = configuration.getInt(DebesyncMongoSinkConfig.FIELD_COLLECTION_LOCATION);
        if (this.collectionLocation == null) {
            this.collectionLocation = DebesyncMongoSinkConfig.DEFAULT_COLLECTION_LOCATION;
        }
    }

    @Override
    public MongoNamespace getNamespace(SinkRecord sinkRecord, SinkDocument sinkDocument) {
        String topic = sinkRecord.topic();
        String[] entries = topic.split(this.topicSeparator);
        String database = StringUtils.isBlank(this.defaultDatabaseName)
                && entries.length>this.databaseLocation
                && StringUtils.isNotEmpty(entries[this.databaseLocation])?
                entries[this.databaseLocation]:this.defaultDatabaseName;
        String collection = entries.length>this.collectionLocation && StringUtils.isNotEmpty(entries[this.collectionLocation])?
                entries[this.collectionLocation]:this.defaultCollectionName;
        return new MongoNamespace(
                database,
                collection
        );
    }
}
