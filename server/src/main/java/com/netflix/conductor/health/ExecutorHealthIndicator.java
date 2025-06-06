package com.netflix.conductor.health;

import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.conductor.core.execution.tasks.SystemTaskWorker;

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
        Map<String, ExecutorService> allExecutors = new HashMap<>();
        
        // 1. Get ExecutorService beans
        Map<String, ExecutorService> executorServiceBeans = applicationContext.getBeansOfType(ExecutorService.class);
        allExecutors.putAll(executorServiceBeans);
        
        // 2. Get Executor beans that are actually ExecutorService instances
        Map<String, Executor> executorBeans = applicationContext.getBeansOfType(Executor.class);
        executorBeans.forEach((name, executor) -> {
            if (executor instanceof ExecutorService && !allExecutors.containsKey(name)) {
                allExecutors.put(name, (ExecutorService) executor);
            }
        });
        
        // 3. Get SystemTaskWorker executors
        try {
            Map<String, SystemTaskWorker> systemTaskWorkers = applicationContext.getBeansOfType(SystemTaskWorker.class);
            systemTaskWorkers.forEach((name, worker) -> {
                Map<String, ExecutorService> workerExecutors = worker.getAllExecutorServices();
                allExecutors.putAll(workerExecutors);
            });
        } catch (Exception e) {
            LOGGER.warn("Error accessing SystemTaskWorker executors", e);
        }
        
        if (allExecutors.isEmpty()) {
            LOGGER.warn("No ExecutorService instances found for health monitoring");
            builder.down().withDetail("status", "No ExecutorService instances found");
            return;
        }
        
        boolean isHealthy = true;
        for (Map.Entry<String, ExecutorService> entry : allExecutors.entrySet()) {
            ExecutorService executor = entry.getValue();
            String executorName = entry.getKey();
            try {
                boolean isUnhealthy = isExecutorUnhealthy(executor);
                String status = isUnhealthy ? "UNHEALTHY" : "HEALTHY";
                LOGGER.info("Executor '{}' status: {}", executorName, status);
                
                if (isUnhealthy) {
                    isHealthy = false;
                    break;
                }    
            } catch (Exception e) {
                LOGGER.error("Error checking health for ExecutorService bean '{}'", executorName, e);
                LOGGER.info("Executor '{}' status: ERROR", executorName);
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
     * Check if an executor is unhealthy
     */
    private boolean isExecutorUnhealthy(ExecutorService executor) {
        return executor.isShutdown();
    }

}