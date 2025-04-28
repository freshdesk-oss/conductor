package com.netflix.conductor.core.status;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

@Component
@ConfigurationProperties(prefix = "event-filter")
public class EventFilterConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventFilterConfig.class);

    // Map to hold module level configurations (e.g., journey, alert)
    private Map<String, ModuleConfig> modules;

    public Map<String, ModuleConfig> getModules() {
        return modules;
    }
    public void setModules(Map<String, ModuleConfig> modules) {
        this.modules = modules;
    }

    public static class ModuleConfig {
        // Map to hold entity level configurations (e.g., workflow, task)
        private Map<String, EntityRules> entities;

        public Map<String, EntityRules> getEntities() {
            return entities;
        }
        public void setEntities(Map<String, EntityRules> entities) {
            this.entities = entities;
        }
    }

    public static class EntityRules {
        private List<Rule> rules;

        public List<Rule> getRules() {
            return rules;
        }
        public void setRules(List<Rule> rules) {
            this.rules = rules;
        }
    }

    public static class Rule {
        private String field; // "status", "taskType" etc..,
        private String caseType; // "includes" or "excludes"
        private String operator; // "contains", "startsWith"
        private List<String> values; // "completed", "running" etc..,

        public String getField() {
            return field;
        }
        public void setField(String field) {
            this.field = field;
        }
        public String getCaseType() {
            return caseType;
        }
        public void setCaseType(String caseType) {
            this.caseType = caseType;
        }
        public String getOperator() {
            return operator;
        }
        public void setOperator(String operator) {
            this.operator = operator;
        }
        public List<String> getValues() {
            return values;
        }
        public void setValues(List<String> values) {
            this.values = values;
        }
    }

    private EntityRules getRulesForEntityType(String entity, String moduleType) {
        ModuleConfig filterConfig = modules.get(moduleType);
        Map<String, EntityRules> moduleMap = filterConfig.getEntities();
        return (moduleMap != null) ? moduleMap.get(entity) : null;
    }

    /***
     * This method checks if the event should be published based on the rules set configured for the module.
     * Module type refers to journey or alert
     * Entity type refers to workflow or task
     * For workflow:
     *    - the status should be completed/failed/terminated/running to publish the event
     * For task:
     *    - the task status should be cancelled/failed/timeout/failed_with_terminate
     *    - the task type should not be fork_join/switch/join/sub_workflow/wait
     *    - the task reference name should not start with wTimer/wCleanup/wDecision
     * Sample Rule:
     *   - field: "status"
     *   - caseType: "includes"
     *   - operator: "contains"
     *   - values: ["completed", "failed", "terminated", "running"]
     * @param eventType
     * @param model
     * @param moduleType
     * @return
     */
    public boolean shouldPublishEvent(String eventType, Object model, String moduleType) {
        EventFilterConfig.EntityRules entityRules = getRulesForEntityType(eventType, moduleType);
        boolean result = validateEntityRule(model, entityRules);
        LOGGER.info("Workflow-Event-123 Event validation for model: {}, result: {}", model, result);
        return result;
    }

    private boolean validateEntityRule(Object entity, EventFilterConfig.EntityRules entityRules) {
        // If no rules configured for the module all events should be published
        if (entityRules == null || entityRules.getRules().isEmpty()) {
            return true;
        }

        for (EventFilterConfig.Rule rule : entityRules.getRules()) {
            if (!isRuleValid(entity, rule)) {
                return false; // If any rule fails, return false immediately
            }
        }
        return true;
    }

    private boolean isRuleValid(Object entity, EventFilterConfig.Rule rule) {
        Object fieldValue = getFieldValue(entity, rule.getField());
        if (fieldValue == null) {
            return false; // return false if the required field is missing or null from the workflowModel or taskModel
        }
        return evaluateRule(rule, fieldValue.toString());
    }

    private boolean evaluateRule(EventFilterConfig.Rule rule, String fieldValue) {
        boolean rulePassed = switch (rule.getOperator()) {
            case "contains" -> rule.getValues().contains(fieldValue);
            case "startsWith" -> rule.getValues().stream().anyMatch(fieldValue::startsWith);
            default -> false;
        };
        return "excludes".equals(rule.getCaseType()) != rulePassed;
    }

    private Object getFieldValue(Object entity, String fieldName) {
        try {
            Method method = entity.getClass().getMethod("get" + capitalize(fieldName));
            return method.invoke(entity);
        } catch (Exception e) {
            LOGGER.error("Error occurred while fetching the value for the field: {}", fieldName);
            return null;
        }
    }

    private String capitalize(String str) {
        return str.substring(0, 1).toUpperCase() + str.substring(1);
    }
}
