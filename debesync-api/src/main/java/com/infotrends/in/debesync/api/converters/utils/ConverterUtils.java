package com.infotrends.in.debesync.api.converters.utils;

import io.debezium.config.Configuration;
import io.debezium.engine.format.*;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;

import java.lang.reflect.InvocationTargetException;

@SuppressWarnings("rawtypes")
public class ConverterUtils {

    private static final String CLASS_FIELD = "class";
    private static final String APICURIO_SCHEMA_REGISTRY_URL_CONFIG = "apicurio.registry.url";

    public static boolean isFormat(Class<? extends SerializationFormat> format1, Class<? extends SerializationFormat> format2) {
        return format1 == format2;
    }

    public static HeaderConverter createHeaderConverter(Configuration config, String headerConverterPrefix, Class<? extends SerializationFormat> converterFormat) {

        Configuration converterConfig = config.subset(headerConverterPrefix, true);
        if (isFormat(converterFormat, Json.class) || isFormat(converterFormat, JsonByteArray.class)) {
            converterConfig = converterConfig.edit().withDefault(CLASS_FIELD, "org.apache.kafka.connect.json.JsonConverter").build();
        } else {
            throw new ConfigException("Debezium Header converter format - " + converterFormat.getName() + " not supported");
        }
        converterConfig = converterConfig.edit().withDefault("converter.type", "header")
                .build();
        HeaderConverter converter = converterConfig.getInstance(CLASS_FIELD, HeaderConverter.class);
        if (!converterConfig.isEmpty())
            converter.configure(converterConfig.asMap());
        return converter;
    }

    public static Converter createConverter(Configuration config, String converterPrefix,
                                            Class<? extends SerializationFormat> converterFormat, boolean key) {

        Configuration converterConfig = config.subset(converterPrefix + ".", true);
        if (isFormat(converterFormat, Json.class) || isFormat(converterFormat, JsonByteArray.class)) {
            if (converterConfig.hasKey(APICURIO_SCHEMA_REGISTRY_URL_CONFIG)) {
                converterConfig = converterConfig.edit()
                        .withDefault(CLASS_FIELD, "io.apicurio.registry.utils.converter.ExtJsonConverter").build();
            } else {
                converterConfig = converterConfig.edit()
                        .withDefault(CLASS_FIELD, "org.apache.kafka.connect.json.JsonConverter").build();
            }
        } else if (isFormat(converterFormat, Avro.class)) {
            if (converterConfig.hasKey(APICURIO_SCHEMA_REGISTRY_URL_CONFIG)) {
                converterConfig = converterConfig.edit()
                        .withDefault(CLASS_FIELD, "io.apicurio.registry.utils.converter.AvroConverter").build();
            } else {
                converterConfig = converterConfig.edit()
                        .withDefault(CLASS_FIELD, "io.confluent.connect.avro.AvroConverter").build();
            }
        }else if (isFormat(converterFormat, CloudEvents.class)) {
            converterConfig = converterConfig.edit()
                    .withDefault(CLASS_FIELD, "io.debezium.converters.CloudEventsConverter").build();
        } else {
            throw new ConfigException("Debezium converter format - " + converterFormat.getName() + " not supported");
        }

        Converter converter = converterConfig.getInstance(CLASS_FIELD, Converter.class);
        if (!converterConfig.isEmpty())
            converter.configure(converterConfig.subset("properties.", true).asMap(), key);

        return converter;

    }
}
