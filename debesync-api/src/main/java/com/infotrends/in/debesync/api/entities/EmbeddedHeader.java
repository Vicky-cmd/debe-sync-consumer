package com.infotrends.in.debesync.api.entities;

import io.debezium.engine.Header;
import lombok.AllArgsConstructor;
import lombok.ToString;

@ToString
@AllArgsConstructor
public class EmbeddedHeader<R> implements Header<R> {

    private String key;
    private R value;

    @Override
    public String getKey() {
        return this.key;
    }

    @Override
    public R getValue() {
        return this.value;
    }
}
