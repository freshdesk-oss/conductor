package com.netflix.conductor.core.status;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.tasks.TaskType;
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

    private final CentralProducer centralProducer;

    @Value("${conductor.status-listener.enabled}")
    private boolean isStatusListenerEnabled;

    public EventPublisherImpl(CentralProducer centralProducer) {
        this.centralProducer = centralProducer;
    }

    @Override
    public void pushWorkflowEvents(WorkflowModel workflow) {
        if (isStatusListenerEnabled && JOURNEY_WORKFLOW_STATUS.contains(workflow.getStatus())) {
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

    private WorkflowType getEntityType(WorkflowModel workflow) {
        return workflow.hasParent() ? WorkflowType.PHASE : WorkflowType.JOURNEY;
    }

    @Override
    public void pushTaskEvents(TaskModel task) {
        if (isValidTaskEvent(task)) {
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

    private boolean isValidTaskEvent(TaskModel task) {
        if(!isStatusListenerEnabled) {
            return false;
        }
        if (!JOURNEY_TASK_STATUS.contains(task.getStatus())) {
            return false;
        }
        if (JOURNEY_TASK_TYPE.contains(TaskType.of(task.getTaskType()))) {
            return false;
        }
        return checkTaskReferenceName(task.getReferenceTaskName());
    }

    private boolean checkTaskReferenceName(String taskReferenceName) {
        for (String referenceName : JOURNEY_TASK_REFERENCE_NAME) {
            if (taskReferenceName.startsWith(referenceName)) {
                return false;
            }
        }
        return true;
    }

    private void publishMessage(String accountId, JsonNode event, EventType eventType) {
        centralProducer.publish(accountId, event, getPayloadType(eventType));
    }

    private String getPayloadType(JourneyConstants.EventType eventType) {
        return eventType == JourneyConstants.EventType.WORKFLOW ? JOURNEY_CONDUCTOR_WORKFLOW_EVENT : JOURNEY_CONDUCTOR_TASK_EVENT;
    }


}
