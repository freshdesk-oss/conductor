package com.netflix.conductor.core.central;

import com.netflix.conductor.core.central.exception.CentralRetryableException;
import com.netflix.conductor.core.central.model.RetryProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class RetryConfig {
    private final RetryProperties retryProperties;

    public RetryConfig(RetryProperties retryProperties) {
        this.retryProperties = retryProperties;
    }

    @Bean
    public RetryTemplate centralRetryTemplate() {
        final RetryTemplate retryTemplate = new RetryTemplate();

        ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
        backOffPolicy.setInitialInterval(retryProperties.getInitialInterval()); // Initial delay
        backOffPolicy.setMultiplier(retryProperties.getMultiplier());// Exponential multiplier for backoff (1, 2, 4 seconds, etc.)
        backOffPolicy.setMaxInterval(retryProperties.getMaxInterval()); // Max delay

        Map<Class<? extends Throwable>, Boolean> retryableExceptions = new HashMap<>();
        retryableExceptions.put(CentralRetryableException.class, true);

        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy(retryProperties.getMaxAttempts(), retryableExceptions);

        retryTemplate.setBackOffPolicy(backOffPolicy);
        retryTemplate.setRetryPolicy(retryPolicy);

        return retryTemplate;
    }
}
