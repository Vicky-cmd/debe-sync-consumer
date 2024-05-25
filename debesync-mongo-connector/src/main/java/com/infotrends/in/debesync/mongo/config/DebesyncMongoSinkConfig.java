package com.infotrends.in.debesync.mongo.config;

import com.infotrends.in.debesync.api.errors.DebeSyncException;
import com.mongodb.kafka.connect.sink.MongoSinkConfig;
import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import jakarta.enterprise.context.ApplicationScoped;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.ConfigDef;

import java.lang.reflect.Field;

@Slf4j
@ApplicationScoped
public class DebesyncMongoSinkConfig {
    public static ConfigDef ADDON_CONFIG;
    public void setup() {
        DebesyncMongoSinkConfig.createAddOnConfig(MongoSinkConfig.CONFIG);
        try {
            Field field = MongoSinkTopicConfig.class.getDeclaredField("CONFIG");
            field.setAccessible(true);
            ADDON_CONFIG = (ConfigDef) field.get(null);
            DebesyncMongoSinkConfig.createAddOnConfig(ADDON_CONFIG);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new DebeSyncException("Unable to setup the debesync mongo sink connector", e);
        }
    }

    public static String FIELD_TOPIC_SEPARATOR = "namespace.mapper.topic.separator";
    public static String DEFAULT_TOPIC_SEPARATOR = "\\.";
    public static String TOPIC_SEPARATOR_DOC = "This is used to define the separator for extracting the database name and collection name from Topic Name";
    public static String TOPIC_SEPARATOR_DISPLAY = "Topic Namespace Extractor";

    public static String FIELD_DATABASE_LOCATION = "namespace.mapper.location.database";
    public static int DEFAULT_DATABASE_LOCATION = 1;
    public static String DATABASE_LOCATION_DOC = "This is used to define the location to extract the database name";
    public static String DATABASE_LOCATION_DISPLAY = "Database Name Extractor Location";
    public static String FIELD_COLLECTION_LOCATION = "namespace.mapper.location.collection";
    public static int DEFAULT_COLLECTION_LOCATION = 2;
    public static String COLLECTION_LOCATION_DOC = "This is used to define the location to extract the collection name";
    public static String COLLECTION_LOCATION_DISPLAY = "Collection Name Extractor Location";


    public static void createAddOnConfig(ConfigDef configDef) {
        String group = "Mapper";
        int orderInGroup = 0;
        configDef.define(
                FIELD_TOPIC_SEPARATOR,
                ConfigDef.Type.STRING,
                DEFAULT_TOPIC_SEPARATOR,
                ConfigDef.Importance.HIGH,
                TOPIC_SEPARATOR_DOC,
                group,
                ++orderInGroup,
                ConfigDef.Width.SHORT,
                TOPIC_SEPARATOR_DISPLAY
        );
        configDef.define(
                FIELD_DATABASE_LOCATION,
                ConfigDef.Type.INT,
                DEFAULT_DATABASE_LOCATION,
                ConfigDef.Importance.HIGH,
                DATABASE_LOCATION_DOC,
                group,
                ++orderInGroup,
                ConfigDef.Width.SHORT,
                DATABASE_LOCATION_DISPLAY
        );
        configDef.define(
                FIELD_COLLECTION_LOCATION,
                ConfigDef.Type.INT,
                DEFAULT_COLLECTION_LOCATION,
                ConfigDef.Importance.HIGH,
                COLLECTION_LOCATION_DOC,
                group,
                ++orderInGroup,
                ConfigDef.Width.SHORT,
                COLLECTION_LOCATION_DISPLAY
        );

    }
}
