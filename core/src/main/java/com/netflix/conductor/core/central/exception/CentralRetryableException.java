package com.netflix.conductor.core.central.exception;

public class CentralRetryableException extends RuntimeException {
    public CentralRetryableException(String message) {
        super(message);
    }
}
