package com.infotrends.in.debesync.core.events;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class ConnectorStartedEvent {
    private String message;
}
