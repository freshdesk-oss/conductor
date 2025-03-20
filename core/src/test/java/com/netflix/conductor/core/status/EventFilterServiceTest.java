package com.netflix.conductor.core.status;

import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class EventFilterServiceTest {

    @InjectMocks
    private EventFilterService eventFilterService;
    
    @Mock
    private EventFilterConfig eventFilterConfig;

    @Test
    // When no rules set for the module all events should be published
    void shouldPublishEvent_WhenRulesAreNull() {
        when(eventFilterConfig.getRulesForEntityType("workflow", "journey")).thenReturn(null);
        boolean result = eventFilterService.shouldPublishEvent("workflow", new WorkflowModel(), "journey");
        assertTrue(result);
    }


    @Test
    // When rules are empty for the module all events should be published
    void shouldPublishEvent_WhenRulesListIsEmpty() {
        EventFilterConfig.EntityRules entityRules = new EventFilterConfig.EntityRules();
        entityRules.setRules(List.of());
        when(eventFilterConfig.getRulesForEntityType("workflow", "journey")).thenReturn(entityRules);

        boolean result = eventFilterService.shouldPublishEvent("workflow", new WorkflowModel(), "journey");
        assertTrue(result);
    }

    @Test
    // When workflow status is completed/failed/terminated/running events should be published
    void shouldPublishWorkflowEvent_WhenStatusIsCompleted() {
        when(eventFilterConfig.getRulesForEntityType("workflow", "journey")).thenReturn(getEntityRulesForWorkflow());

        WorkflowModel workflowModel = new WorkflowModel();
        workflowModel.setStatus(WorkflowModel.Status.COMPLETED);

        boolean result = eventFilterService.shouldPublishEvent("workflow", workflowModel, "journey");
        assertTrue(result);
    }

    @Test
    // When workflow status is paused/timed_out events should not be published
    void shouldNotPublishWorkflowEvent_WhenStatusIsPaused() {
        when(eventFilterConfig.getRulesForEntityType("workflow", "journey")).thenReturn(getEntityRulesForWorkflow());

        WorkflowModel workflowModel = new WorkflowModel();
        workflowModel.setStatus(WorkflowModel.Status.PAUSED);

        boolean result = eventFilterService.shouldPublishEvent("workflow", workflowModel, "journey");
        assertFalse(result);
    }

    @Test
    // task status should be cancelled/completed_with_errors/failed/failed_with_terminal_error
    // task type should not be fork_join/switch/join/sub_workflow
    // task reference name should not start with wTimer/wCleanup/wDecision
    // only if all the above conditions are met task event should be published
    void shouldPublishTaskEvent_WhenAllRulesArePassed() {
        when(eventFilterConfig.getRulesForEntityType("task", "journey")).thenReturn(getEntityRulesForTask());

        TaskModel taskModel = new TaskModel();
        taskModel.setStatus(TaskModel.Status.CANCELED);
        taskModel.setTaskType("SIMPLE");
        taskModel.setReferenceTaskName("wDataArrangement_123");

        boolean result = eventFilterService.shouldPublishEvent("task", taskModel, "journey");
        assertTrue(result);
    }

    @Test
    // when the task reference name starts with wTimer/wCleanup/wDecision event should not be published
    void shouldNotPublishTaskEvent_ForTimerTask() {
        when(eventFilterConfig.getRulesForEntityType("task", "journey")).thenReturn(getEntityRulesForTask());

        TaskModel taskModel = new TaskModel();
        taskModel.setStatus(TaskModel.Status.CANCELED);
        taskModel.setTaskType("SIMPLE");
        taskModel.setReferenceTaskName("wTimer_123");

        boolean result = eventFilterService.shouldPublishEvent("task", taskModel, "journey");
        assertFalse(result);
    }

    @Test
    // when the task type is fork_join/switch/join/sub_workflow event should not be published
    void shouldNotPublishTaskEvent_WhenTaskTypeIsJoin() {
        when(eventFilterConfig.getRulesForEntityType("task", "journey")).thenReturn(getEntityRulesForTask());

        TaskModel taskModel = new TaskModel();
        taskModel.setStatus(TaskModel.Status.CANCELED);
        taskModel.setTaskType("JOIN");
        taskModel.setReferenceTaskName("wTimer_123");

        boolean result = eventFilterService.shouldPublishEvent("task", taskModel, "journey");
        assertFalse(result);
    }

    @Test
    // when the task type is other than cancelled/completed_with_errors/failed/failed_with_terminal_error event should not be published
    void shouldNotPublishTaskEvent_WhenTaskStatusIsCompleted() {
        when(eventFilterConfig.getRulesForEntityType("task", "journey")).thenReturn(getEntityRulesForTask());

        TaskModel taskModel = new TaskModel();
        taskModel.setStatus(TaskModel.Status.COMPLETED);
        taskModel.setTaskType("JOIN");
        taskModel.setReferenceTaskName("wTimer_123");

        boolean result = eventFilterService.shouldPublishEvent("task", taskModel, "journey");
        assertFalse(result);
    }

    @Test
    // when any of the task status/taskType/referenceTaskName fields are missing or null event should not be published
    void shouldNotPublishTaskEvent_WhenTaskFieldsAreMissing() {
        when(eventFilterConfig.getRulesForEntityType("task", "journey")).thenReturn(getEntityRulesForTask());

        boolean result = eventFilterService.shouldPublishEvent("task", new TaskModel(), "journey");
        assertFalse(result);
    }

    private EventFilterConfig.EntityRules getEntityRulesForWorkflow() {
        EventFilterConfig.EntityRules entityRules = new EventFilterConfig.EntityRules();
        entityRules.setRules(Collections.singletonList(getRule("status", "includes", "contains", List.of("COMPLETED", "TERMINATED", "FAILED", "RUNNING"))));
        return entityRules;
    }

    private EventFilterConfig.EntityRules getEntityRulesForTask() {
        EventFilterConfig.EntityRules entityRules = new EventFilterConfig.EntityRules();
        entityRules.setRules(List.of(
                getRule("status", "includes", "contains", List.of("CANCELED", "COMPLETED_WITH_ERRORS", "FAILED", "FAILED_WITH_TERMINAL_ERROR")),
                getRule("taskType", "excludes", "contains", List.of("FORK_JOIN", "SWITCH", "JOIN", "SUB_WORKFLOW")),
                getRule("referenceTaskName", "excludes", "startsWith", List.of("wTimer", "wCleanup", "wDecision"))
        ));
        return entityRules;
    }

    private EventFilterConfig.Rule getRule(String fieldName, String caseType, String operator, List<String> values) {
        EventFilterConfig.Rule rule = new EventFilterConfig.Rule();
        rule.setField(fieldName);
        rule.setCaseType(caseType);
        rule.setOperator(operator);
        rule.setValues(values);
        return rule;
    }
}
