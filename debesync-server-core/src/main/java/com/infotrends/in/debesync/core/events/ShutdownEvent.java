package com.infotrends.in.debesync.core.events;

import lombok.Getter;

@Getter
@SuppressWarnings("unused")
public class ShutdownEvent {
    private final String status;
    private final String message;
    private String error;
    private String errorMessage;

    public ShutdownEvent(String status, String message) {
        this.status = status;
        this.message = message;
    }

    public ShutdownEvent(String status, String message, String error, String errorMessage) {
        this.status = status;
        this.message = message;
        this.error = error;
        this.errorMessage = errorMessage;
    }
}
