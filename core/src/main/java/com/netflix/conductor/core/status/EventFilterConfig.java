package com.netflix.conductor.core.status;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Getter
@Setter
@Component
@ConfigurationProperties(prefix = "event-filter")
public class EventFilterConfig {

    // Map to hold module level configurations (e.g., journey, alert)
    private Map<String, ModuleConfig> modules;

    @Getter
    @Setter
    public static class ModuleConfig {
        // Map to hold entity level configurations (e.g., workflow, task)
        private Map<String, EntityRules> entities;
    }

    @Getter
    @Setter
    public static class EntityRules {
        private List<Rule> rules;
    }

    @Getter
    @Setter
    public static class Rule {
        private String field; // "status", "taskType" etc..,
        private String caseType; // "includes" or "excludes"
        private String operator; // "contains", "startsWith"
        private List<String> values; // "completed", "running" etc..,
    }

    public EntityRules getRulesForEntityType(String entity, String moduleType) {
        ModuleConfig filterConfig = modules.get(moduleType);
        Map<String, EntityRules> moduleMap = filterConfig.getEntities();
        return (moduleMap != null) ? moduleMap.get(entity) : null;
    }
}
