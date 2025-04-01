package com.netflix.conductor.core.status;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.core.central.service.CentralProducer;
import com.netflix.conductor.model.TaskModel;

import com.netflix.conductor.model.WorkflowModel;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import static com.netflix.conductor.core.central.CentralConstants.JOURNEY_CONDUCTOR_TASK_EVENT;
import static com.netflix.conductor.core.central.CentralConstants.JOURNEY_CONDUCTOR_WORKFLOW_EVENT;
import static com.netflix.conductor.core.status.JourneyConstants.*;

@Service
public class EventPublisherImpl implements EventPublisher {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final EventFilterService eventFilterService;
    private final CentralProducer centralProducer;

    /***
     * This is an env variable to get the event filter rule config based for the module
     */
    @Value("${conductor.status-listener.module.type}")
    private String moduleType;

    public EventPublisherImpl(EventFilterService eventFilterService, CentralProducer centralProducer) {
        this.eventFilterService = eventFilterService;
        this.centralProducer = centralProducer;
    }

    /***
     * This method pushes the workflow events to the central service.
     * If the status listener is enabled and the workflow status is RUNNING, COMPLETED, FAILED, TERMINATED
     * then the workflow event is published to the central service.
     * @param workflow
     */
    @Override
    public void pushWorkflowEvents(WorkflowModel workflow) {
        if (eventFilterService.shouldPublishEvent(WORKFLOW, workflow, moduleType)) {
            WorkflowEvent event =
                    WorkflowEvent.builder()
                            .journeyReqId(String.valueOf(workflow.getInput().get(JOURNEY_REQUEST_ID)))
                            .entityId(getEntityId(workflow))
                            .entityType(getEntityType(workflow))
                            .workflowInstanceId(workflow.getWorkflowId())
                            .status(workflow.getStatus().name())
                            .statusId(workflow.getStatus().ordinal())
                            .workflowTerminationReason(workflow.getReasonForIncompletion())
                            .build();

            publishMessage(String.valueOf(workflow.getInput().get(ACCOUNT_ID)), objectMapper.valueToTree(event), EventType.WORKFLOW);
        }
    }

    private Long getEntityId(WorkflowModel workflow) {
        return workflow.hasParent() ? Long.valueOf(String.valueOf(workflow.getInput().get(JOURNEY_CONFIG_ID))) : Long.valueOf(String.valueOf(workflow.getInput().get(NODE_ID)));
    }

    /***
     * This method returns the entity type of the workflow
     * If the workflow has a parent, then that is a sub-workflow so the entity type is PHASE
     * If the workflow does not have a parent, then that is a main workflow so the entity type is JOURNEY
     * @param workflow
     * @return
     */
    private WorkflowType getEntityType(WorkflowModel workflow) {
        return workflow.hasParent() ? WorkflowType.PHASE : WorkflowType.JOURNEY;
    }

    /***
     * This method evaluates whether the event needs to be published and push the event payload.
     * @param task
     */
    @Override
    public void pushTaskEvents(TaskModel task) {
        if (eventFilterService.shouldPublishEvent(TASK, task, moduleType)) {
            TaskEvent event =
                    TaskEvent.builder()
                            .journeyReqId(String.valueOf(task.getInputData().get(JOURNEY_REQUEST_ID)))
                            .nodeId(Long.valueOf(String.valueOf(task.getInputData().get(NODE_ID))))
                            .taskInstanceId(task.getTaskId())
                            .status(task.getStatus().name())
                            .statusId(task.getStatus().ordinal())
                            .build();

            publishMessage(String.valueOf(task.getInputData().get(ACCOUNT_ID)), objectMapper.valueToTree(event), EventType.TASK);
        }
    }

    private void publishMessage(String accountId, JsonNode event, EventType eventType) {
        centralProducer.publish(accountId, event, getPayloadType(eventType));
    }

    private String getPayloadType(JourneyConstants.EventType eventType) {
        return eventType == JourneyConstants.EventType.WORKFLOW ? JOURNEY_CONDUCTOR_WORKFLOW_EVENT : JOURNEY_CONDUCTOR_TASK_EVENT;
    }
}
