package com.infotrends.in.debesync.api.config;

public class DebeSyncConstants {

    public static final String DEBEZIUM_PREFIX_PROPS = "debezium.";
    public static final String SINK_PROP_PREFIX = DEBEZIUM_PREFIX_PROPS + "sink.";
    public static final String SOURCE_PROP_PREFIX = DEBEZIUM_PREFIX_PROPS + "source.";
    private static final String KEY_CONVERTER_PREFIX = SINK_PROP_PREFIX + "converters.key";
    private static final String VALUE_CONVERTER_PREFIX = SINK_PROP_PREFIX + "converters.value";

}
