package com.netflix.conductor.core.central.model;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Getter
@Setter
@Component
@ConfigurationProperties(prefix = "central.api.retry")
public class RetryProperties {
    private long initialInterval;

    private double multiplier;

    private int maxAttempts;

    private long maxInterval;
}
