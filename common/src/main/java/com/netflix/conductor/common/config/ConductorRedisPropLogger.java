package com.netflix.conductor.common.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

@Component
public class ConductorRedisPropLogger {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConductorRedisPropLogger.class);
    
    private final Environment env;

    public ConductorRedisPropLogger(Environment env) {
        this.env = env;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void loggerResolvedRedisProps() {
        // QUEUE (Orkes RedisQueueConfiguration)
        LOGGER.info("[CONDUCTOR_REDIS_UPGRADE] ==== QUEUE_CFG (Orkes queues) ====");
        loggerKey("conductor.queue.type");
        // common keys for Orkes redis queues:
        loggerKey("conductor.redis.hosts");
        loggerKey("conductor.redis.ssl");
        loggerKey("conductor.redis.workflowNamespacePrefix");
        loggerKey("conductor.redis.queueNamespacePrefix");
        LOGGER.info("[CONDUCTOR_REDIS_UPGRADE] ==================================");

        // LOCK (Netflix RedisLockConfiguration)
        LOGGER.info("[CONDUCTOR_REDIS_UPGRADE] ==== LOCK_CFG (Execution lock) ====");
        loggerKey("conductor.workflow-scylla-execution-lock.enabled");
        loggerKey("conductor.redis-lock.serverType");
        loggerKey("conductor.redis-lock.serverAddress");
        loggerKey("conductor.redis-lock.serverPassword");
        loggerKey("conductor.redis-lock.namespace");
        LOGGER.info("[CONDUCTOR_REDIS_UPGRADE] ==================================");
    }

    private void loggerKey(String key) {
        String val = env.getProperty(key);
        LOGGER.info("[CONDUCTOR_REDIS_UPGRADE] [{}] {}", key, val == null ? "<unset>" : val);
    }
}
