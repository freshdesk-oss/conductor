package com.netflix.conductor.redislock.config;

import org.springframework.boot.autoconfigure.condition.AnyNestedCondition;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

public class AnyLockCondition extends AnyNestedCondition {
    public AnyLockCondition() {
        super(ConfigurationPhase.PARSE_CONFIGURATION);
    }

    @ConditionalOnProperty(name = "conductor.workflow-execution-lock.type", havingValue = "redis")
    static class RedisLockEnabled {}

    @ConditionalOnProperty(name = "conductor.workflow-scylla-execution-lock.enabled", havingValue = "true")
    static class ScyllaExecutionLockEnabled {}
}
