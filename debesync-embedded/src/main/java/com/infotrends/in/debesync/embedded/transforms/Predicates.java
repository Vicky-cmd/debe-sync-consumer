package com.infotrends.in.debesync.embedded.transforms;

import com.infotrends.in.debesync.api.errors.DebeSyncException;
import com.infotrends.in.debesync.embedded.EmbeddedEngineConfig;
import io.debezium.config.Configuration;
import io.vavr.control.Try;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.predicates.Predicate;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class Predicates implements Closeable {


    static String SUFFIX_TYPE = ".type";

    private final Configuration config;

    private final Map<String, Predicate<SinkRecord>> predicates;

    public Predicates(Configuration config) {
        this.predicates = new HashMap<>();
        this.config = config;
        final String predicatesStr = config.getString(EmbeddedEngineConfig.PREDICATES);
        if (StringUtils.isBlank(predicatesStr)) return;

        for (String predicateName : predicatesStr.split(",")) {
            predicates.put(predicateName, createPredicate(predicateName));
        }
    }

    public Predicate<SinkRecord> getPredicate(String predicateName) {
        return this.predicates.get(predicateName);
    }

    private String createPredicatesPrefix(String predicateName) {
        return EmbeddedEngineConfig.PREDICATES.name() + "." + predicateName;
    }

    @SuppressWarnings("unchecked")
    private Predicate<SinkRecord> createPredicate(String predicateName) {
        String predicatePrefix = this.createPredicatesPrefix(predicateName);
        Predicate<SinkRecord> predicate;
        try {
            predicate = this.config.getInstance(predicatePrefix + SUFFIX_TYPE, Predicate.class);
        } catch (IllegalArgumentException e) {
            throw new DebeSyncException(String.format("Error occurred while instantiating the predicate - %s with Error - %s. Message: %s", predicateName, e.getClass().getSimpleName(), e.getMessage()), e);
        }
        if (predicate==null) {
            throw new DebeSyncException(String.format("Cannot instantiate the predicate - %s - Value - %s", predicateName, config.getString(predicatePrefix + SUFFIX_TYPE)));
        }
        Configuration predicatesConfig = this.config.subset(predicatePrefix, true);
        predicate.configure(predicatesConfig.asMap());
        return predicate;
    }

    @Override
    public void close(){
        this.predicates.values().forEach(predicate -> Try.run(predicate::close));
    }
}
