package com.netflix.conductor.core.central;

import com.netflix.conductor.core.central.exception.CentralRetryableException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class RetryConfig {
    @Value("${central.api.retry.initialInterval}")
    private long initialInterval;
    @Value("${central.api.retry.multiplier}")
    private double multiplier;
    @Value("${central.api.retry.maxAttempts}")
    private int maxAttempts;
    @Value("${central.api.retry.maxInterval}")
    private long maxInterval;

    @Bean
    public RetryTemplate centralRetryTemplate() {
        final RetryTemplate retryTemplate = new RetryTemplate();

        ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
        backOffPolicy.setInitialInterval(initialInterval); // Initial delay
        backOffPolicy.setMultiplier(multiplier);// Exponential multiplier for backoff (1, 2, 4 seconds, etc.)
        backOffPolicy.setMaxInterval(maxInterval); // Max delay

        Map<Class<? extends Throwable>, Boolean> retryableExceptions = new HashMap<>();
        retryableExceptions.put(CentralRetryableException.class, true);

        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy(maxAttempts, retryableExceptions);

        retryTemplate.setBackOffPolicy(backOffPolicy);
        retryTemplate.setRetryPolicy(retryPolicy);

        return retryTemplate;
    }
}
