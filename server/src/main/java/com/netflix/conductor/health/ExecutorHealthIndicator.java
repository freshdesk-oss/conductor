package com.netflix.conductor.health;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.patterns.ThreadPoolMonitor;
import com.netflix.spectator.api.Spectator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ExecutorService;
import java.util.List;
import java.util.ArrayList;

@Component
@ConditionalOnProperty(value = "conductor.executor.health.enabled", havingValue = "true", matchIfMissing = false)
public class ExecutorHealthIndicator extends AbstractHealthIndicator {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExecutorHealthIndicator.class);
    private static final String THREAD_STATUS = "threadStatus";
    private final List<ThreadPoolExecutor> executors = new ArrayList<>();
    private final Registry registry = Spectator.globalRegistry();

    @Autowired
    public ExecutorHealthIndicator(
            ExecutorService mainExecutor,
            ThreadPoolExecutor eventProcessorExecutor,
            ThreadPoolExecutor sweeperExecutor,
            ThreadPoolExecutor systemTaskWorkerExecutor,
            ThreadPoolExecutor elasticsearchMainExecutor,
            ThreadPoolExecutor elasticsearchLogExecutor,
            ThreadPoolExecutor taskPollExecutor) {
        
        // Add all executors to the list
        if (mainExecutor instanceof ThreadPoolExecutor) {
            executors.add((ThreadPoolExecutor) mainExecutor);
            ThreadPoolMonitor.attach(registry, (ThreadPoolExecutor) mainExecutor, "main-executor");
            LOGGER.info("Registered main executor for health monitoring");
        }
        executors.add(eventProcessorExecutor);
        ThreadPoolMonitor.attach(registry, eventProcessorExecutor, "event-processor-executor");
        LOGGER.info("Registered event processor executor for health monitoring");
        
        executors.add(sweeperExecutor);
        ThreadPoolMonitor.attach(registry, sweeperExecutor, "workflow-sweeper-executor");
        LOGGER.info("Registered workflow sweeper executor for health monitoring");
        
        executors.add(systemTaskWorkerExecutor);
        ThreadPoolMonitor.attach(registry, systemTaskWorkerExecutor, "system-task-worker-executor");
        LOGGER.info("Registered system task worker executor for health monitoring");
        
        executors.add(elasticsearchMainExecutor);
        ThreadPoolMonitor.attach(registry, elasticsearchMainExecutor, "elasticsearch-main-executor");
        LOGGER.info("Registered elasticsearch main executor for health monitoring");
        
        executors.add(elasticsearchLogExecutor);
        ThreadPoolMonitor.attach(registry, elasticsearchLogExecutor, "elasticsearch-log-executor");
        LOGGER.info("Registered elasticsearch log executor for health monitoring");
        
        executors.add(taskPollExecutor);
        ThreadPoolMonitor.attach(registry, taskPollExecutor, "task-poll-executor");
        LOGGER.info("Registered task poll executor for health monitoring");
    }

    @Override
    protected void doHealthCheck(Health.Builder builder) throws Exception {
        if (executors.isEmpty()) {
            LOGGER.info("Health check failed: No Conductor Thread pools were created");
            builder.down().withDetail(THREAD_STATUS, "No Conductor Thread pools got created");
            return;
        }
        for (ThreadPoolExecutor executor : executors) {
            String executorName = executor.toString();
            if (executor.isShutdown()) {
                LOGGER.info("Health check failed: Executor {} is shut down", executorName);
                builder.down().withDetail(THREAD_STATUS, "Conductor Thread pool is shut down");
                break;
            } else {
                builder.up().withDetail(THREAD_STATUS, "Conductor Thread pool is healthy");
                LOGGER.info("Health check passed: Executor {} is healthy", executorName);
            }
        }
    }
}