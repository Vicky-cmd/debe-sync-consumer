package com.infotrends.in.debesync.embedded.transforms;

import com.infotrends.in.debesync.api.errors.DebeSyncException;
import com.infotrends.in.debesync.embedded.EmbeddedEngineConfig;
import io.debezium.config.Configuration;
import io.vavr.control.Try;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.predicates.Predicate;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;

@Slf4j
public class Transformations implements Closeable {

    final static String SUFFIX_TYPE = ".type";
    final static String SUFFIX_PREDICATES = ".predicate";
    final static String SUFFIX_NEGATE = ".negate";

    private final Configuration config;
    private final Predicates predicates;

    private final List<Transformation<SinkRecord>> transformations = new ArrayList<>();

    public Transformations(Configuration config) {
        this.config = config;
        this.predicates = new Predicates(config);
        final String transformationsStr = config.getString(EmbeddedEngineConfig.TRANSFORMS);
        if (StringUtils.isBlank(transformationsStr)) return;

        for(String transform: transformationsStr.split(",")) {
            this.transformations.add(this.createTransformations(transform));
        }
    }
    private String createTransformsPrefix(String transformationName) {
        return EmbeddedEngineConfig.TRANSFORMS.name() + "." + transformationName;
    }

    public SinkRecord apply(SinkRecord record) {
        for (Transformation<SinkRecord> transformation: this.transformations) {
            record = transformation.apply(record);
            if (record==null) return null;
        }
        return record;
    }

    @SuppressWarnings("unchecked")
    private Transformation<SinkRecord> createTransformations(String transformationName) {
        String transformsPrefix = this.createTransformsPrefix(transformationName);
        Transformation<SinkRecord> transform;
        try {
            transform = this.config.getInstance(transformsPrefix + SUFFIX_TYPE, Transformation.class);
        } catch (IllegalArgumentException e) {
            throw new DebeSyncException(String.format("Error occurred while instantiating the Transform - %s with Error - %s. Message: %s", transformationName, e.getClass().getSimpleName(), e.getMessage()), e);
        }
        if (transform==null) {
            throw new DebeSyncException(String.format("Cannot instantiate the predicate - %s - Value - %s", transformationName, config.getString(transformsPrefix + SUFFIX_TYPE)));
        }
        Configuration predicatesConfig = this.config.subset(transformationName, true);
        transform.configure(predicatesConfig.asMap());
        String predicateName = this.config.getString(transformsPrefix + SUFFIX_PREDICATES);
        if (predicateName==null) return transform;

        Predicate<SinkRecord> predicate = this.predicates.getPredicate(predicateName);
        if (predicate==null) {
            throw new DebeSyncException(String.format("Unable to find the configuration for the predicate - %s for the transformation - %s", predicateName, transformationName));
        }
        boolean negate = this.config.getBoolean(transformsPrefix + SUFFIX_NEGATE, false);
        return this.createTransformationWithPredicate(transform, predicate, negate);
    }

    private Transformation<SinkRecord> createTransformationWithPredicate(Transformation<SinkRecord> transform,
                                                                         Predicate<SinkRecord> predicate,
                                                                         boolean negate) {
        return new Transformation<>() {
            @Override
            public SinkRecord apply(SinkRecord record) {
                if (negate ^ predicate.test(record)) {
                    return transform.apply(record);
                }
                return record;
            }

            @Override
            public ConfigDef config() {
                return transform.config();
            }

            @Override
            public void close() {
                transform.close();
            }

            @Override
            public void configure(Map<String, ?> configs) {
                transform.configure(configs);
            }
        };
    }


    @Override
    public void close() {
        this.predicates.close();
        this.transformations.forEach(transform -> Try.run(transform::close));
    }
}
