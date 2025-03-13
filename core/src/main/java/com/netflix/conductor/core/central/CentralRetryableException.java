package com.netflix.conductor.core.central;

public class CentralRetryableException extends RuntimeException {
    public CentralRetryableException(String message) {
        super(message);
    }
}
