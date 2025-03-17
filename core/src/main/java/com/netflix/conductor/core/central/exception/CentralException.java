package com.netflix.conductor.core.central.exception;

public class CentralException extends RuntimeException {
    public CentralException(String message) {
        super(message);
    }

    public CentralException(String message, Throwable cause) {
        super(message, cause);
    }
}
