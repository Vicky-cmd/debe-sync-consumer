package com.infotrends.in.debesync.api.errors;

public class DebeSyncException extends RuntimeException {

    public DebeSyncException(String message) {
        super(message);
    }
    public DebeSyncException(String message, Throwable cause) {
        super(message, cause);
    }

}
