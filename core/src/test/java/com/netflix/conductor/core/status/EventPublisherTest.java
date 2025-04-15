package com.netflix.conductor.core.status;

import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class EventPublisherTest {

    @InjectMocks
    private EventPublisher eventPublisher;

    @Mock
    private EventFilterConfig eventFilterConfig;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(eventPublisher, "isStatusListenerEnabled", true);
        ReflectionTestUtils.setField(eventPublisher, "moduleType", "journey");
    }

    @Test
    void testPushWorkflowEvents() {
        when(eventFilterConfig.shouldPublishEvent(anyString(), any(), anyString())).thenReturn(true);
        WorkflowModel workflow = new WorkflowModel();
        workflow.setWorkflowId("100");
        workflow.setStatus(WorkflowModel.Status.COMPLETED);
        workflow.setInput(Map.of("accountId", 10));

        assertDoesNotThrow(() -> eventPublisher.pushWorkflowEvents(workflow));
    }

    @Test
    void testPushTaskEvents() {
        when(eventFilterConfig.shouldPublishEvent(anyString(), any(), anyString())).thenReturn(true);
        TaskModel task = new TaskModel();
        task.setTaskId("100");
        task.setStatus(TaskModel.Status.FAILED);
        task.setRetryCount(1);
        WorkflowTask workflowTask = new WorkflowTask();
        workflowTask.setName("dataArrangement");
        workflowTask.setTaskReferenceName("dataArrangement_1");
        TaskDef taskDef = new TaskDef();
        taskDef.setName("dataArrangement");
        taskDef.setRetryCount(1);
        workflowTask.setTaskDefinition(taskDef);
        task.setWorkflowTask(workflowTask);

        assertDoesNotThrow(() -> eventPublisher.pushTaskEvents(task));
    }
}
