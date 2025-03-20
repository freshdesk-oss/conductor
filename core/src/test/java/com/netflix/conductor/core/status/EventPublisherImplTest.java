package com.netflix.conductor.core.status;

import com.fasterxml.jackson.databind.JsonNode;
import com.netflix.conductor.core.central.service.CentralProducer;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.never;

@ExtendWith(MockitoExtension.class)
class EventPublisherImplTest {

    @InjectMocks
    private EventPublisherImpl eventPublisher;

    @Mock
    private CentralProducer centralProducer;
    @Mock
    private EventFilterService eventFilterService;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(eventPublisher, "isStatusListenerEnabled", true);
        ReflectionTestUtils.setField(eventPublisher, "moduleType", "journey");
    }

    @Test
    void testPushWorkflowEvents_WhenStatusListenerEnabled_ShouldPublishMessage() {
        when(eventFilterService.shouldPublishEvent(anyString(), any(), anyString())).thenReturn(true);

        WorkflowModel workflow = new WorkflowModel();
        workflow.setWorkflowId("workflow-123");
        workflow.setStatus(WorkflowModel.Status.COMPLETED);

        Map<String, Object> input = new HashMap<>();
        input.put("journeyRequestId", "journey-123");
        input.put("accountId", "100");
        input.put("nodeId", "100");
        workflow.setInput(input);

        eventPublisher.pushWorkflowEvents(workflow);
        verify(centralProducer, times(1)).publish(eq("100"), any(JsonNode.class), eq("journey_conductor_workflow_event"));
    }

    @Test
    void testPushWorkflowEvents_WhenStatusListenerEnabled_ShouldNotPublishMessage() {
        when(eventFilterService.shouldPublishEvent(anyString(), any(), anyString())).thenReturn(false);
        eventPublisher.pushWorkflowEvents(new WorkflowModel());
        verify(centralProducer, never()).publish(any(), any(), any());
    }

    @Test
    void testPushWorkflowEvents_WhenStatusListenerDisabled() {
        ReflectionTestUtils.setField(eventPublisher, "isStatusListenerEnabled", false);
        eventPublisher.pushWorkflowEvents(new WorkflowModel());
        verify(centralProducer, never()).publish(any(), any(), any());
    }

    @Test
    void testPushTaskEvents_WhenStatusListenerEnabled_ShouldPublishMessage() {
        when(eventFilterService.shouldPublishEvent(anyString(), any(), anyString())).thenReturn(true);

        TaskModel task = new TaskModel();
        task.setTaskId("task-123");
        task.setStatus(TaskModel.Status.CANCELED);
        task.setTaskType("SIMPLE");
        task.setReferenceTaskName("wDataArrangement_123");

        Map<String, Object> inputData = new HashMap<>();
        inputData.put("journeyRequestId", "journey-123");
        inputData.put("nodeId", "200");
        inputData.put("accountId", "100");
        task.setInputData(inputData);

        eventPublisher.pushTaskEvents(task);
        verify(centralProducer, times(1)).publish(eq("100"), any(JsonNode.class), eq("journey_conductor_task_event"));
    }

    @Test
    void testPushTaskEvents_WhenStatusListenerEnabled_ShouldNotPublishMessage() {
        when(eventFilterService.shouldPublishEvent(anyString(), any(), anyString())).thenReturn(false);
        eventPublisher.pushTaskEvents(new TaskModel());
        verify(centralProducer, never()).publish(any(), any(), any());
    }

    @Test
    void testPushTaskEvents_WhenStatusListenerDisabled() {
        ReflectionTestUtils.setField(eventPublisher, "isStatusListenerEnabled", false);
        eventPublisher.pushTaskEvents(new TaskModel());
        verify(centralProducer, never()).publish(any(), any(), any());
    }
}
