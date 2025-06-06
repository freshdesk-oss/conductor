package com.netflix.conductor.health;

import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Health indicator for monitoring all ExecutorService instances in the application
 */
@Component
@ConditionalOnProperty(value = "conductor.executor.health.enabled", havingValue = "true", matchIfMissing = false)
public class ExecutorHealthIndicator extends AbstractHealthIndicator {
    private static final String EXECUTOR_STATUS = "Status";

    private static final Logger LOGGER = LoggerFactory.getLogger(ExecutorHealthIndicator.class);
    
    private final ApplicationContext applicationContext;

    public ExecutorHealthIndicator(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    @Override
    protected void doHealthCheck(Health.Builder builder) throws Exception {
        Map<String, ExecutorService> executors = applicationContext.getBeansOfType(ExecutorService.class);
        if (executors.isEmpty()) {
            LOGGER.warn("No ExecutorService beans found for health monitoring");
            builder.down().withDetail("status", "No ExecutorService beans found");
            return;
        }
        boolean isHealthy = true;
        for (Map.Entry<String, ExecutorService> entry : executors.entrySet()) {
            ExecutorService executor = entry.getValue();
            try {
                if (isExecutorUnhealthy(executor)) {
                    isHealthy = false;
                    break;
                }    
            } catch (Exception e) {
                LOGGER.error("Error checking health for ExecutorService bean '{}'", entry.getKey(), e);
                isHealthy = false;
                break;
            }
        }
        if (isHealthy) {
            builder.up().withDetail(EXECUTOR_STATUS, "ExecutorService is healthy");
        } else {
            builder.down().withDetail(EXECUTOR_STATUS, "ExecutorService is unhealthy");
        }
    }

    /**
     * Check if an executor is healthy
     */
    private boolean isExecutorUnhealthy(ExecutorService executor) {
        return !executor.isShutdown();
    }
}