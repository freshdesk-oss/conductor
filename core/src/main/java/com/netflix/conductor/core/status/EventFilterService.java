package com.netflix.conductor.core.status;

import org.springframework.stereotype.Service;

import java.lang.reflect.Method;

import static com.netflix.conductor.core.status.JourneyConstants.CONTAINS;
import static com.netflix.conductor.core.status.JourneyConstants.STARTS_WITH;
import static com.netflix.conductor.core.status.JourneyConstants.EXCLUDES;
import static com.netflix.conductor.core.status.JourneyConstants.GET;

@Service
public class EventFilterService {

    private final EventFilterConfig eventFilterConfig;

    public EventFilterService(EventFilterConfig eventFilterConfig) {
        this.eventFilterConfig = eventFilterConfig;
    }

    /***
     * This method checks if the event should be published based on the rules set configured for the module.
     * Module type refers to journey or alert
     * Entity type refers to workflow or task
     * For workflow:
     *    - the status should be completed/failed/terminated/running to publish the event
     * For task:
     *    - the task status should be cancelled/failed/timeout/failed_with_terminate
     *    - the task type should not be fork_join/switch/join/sub_workflow
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
        EventFilterConfig.EntityRules entityRules = eventFilterConfig.getRulesForEntityType(eventType, moduleType);
        return validateEntityRule(model, entityRules);
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
            case CONTAINS -> rule.getValues().contains(fieldValue);
            case STARTS_WITH -> rule.getValues().stream().anyMatch(fieldValue::startsWith);
            default -> false;
        };
        return EXCLUDES.equals(rule.getCaseType()) != rulePassed;
    }

    private Object getFieldValue(Object entity, String fieldName) {
        try {
            Method method = entity.getClass().getMethod(GET + capitalize(fieldName));
            return method.invoke(entity);
        } catch (Exception e) {
            return null;
        }
    }

    private String capitalize(String str) {
        return str.substring(0, 1).toUpperCase() + str.substring(1);
    }
}
