package com.netflix.conductor.core.status;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.tasks.TaskType;
import com.netflix.conductor.core.central.service.CentralProducer;
import com.netflix.conductor.model.TaskModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.conductor.model.WorkflowModel;
import org.springframework.stereotype.Service;

import static com.netflix.conductor.core.central.CentralConstants.CONDUCTOR_TASK_EVENT;
import static com.netflix.conductor.core.central.CentralConstants.CONDUCTOR_WORKFLOW_EVENT;
import static com.netflix.conductor.core.status.JourneyConstants.*;

@Service
//@ConditionalOnProperty(name = "conductor.status.event.enabled", havingValue = "true")
public class EventPublisherImpl implements EventPublisher {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventPublisherImpl.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final CentralProducer centralProducer;

    public EventPublisherImpl(CentralProducer centralProducer) {
        this.centralProducer = centralProducer;
    }

    @Override
    public void pushWorkflowEvents(WorkflowModel workflow) {
        if (!JOURNEY_WORKFLOW_STATUS.contains(workflow.getStatus())) {
            return;
        }

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

    private Long getEntityId(WorkflowModel workflow) {
        return workflow.hasParent() ? Long.valueOf(String.valueOf(workflow.getInput().get(JOURNEY_CONFIG_ID))) : Long.valueOf(String.valueOf(workflow.getInput().get(NODE_ID)));
    }

    private WorkflowType getEntityType(WorkflowModel workflow) {
        return workflow.hasParent() ? WorkflowType.JOURNEY : WorkflowType.PHASE;
    }

    @Override
    public void pushTaskEvents(TaskModel task) {
        if (JOURNEY_TASK_STATUS.contains(task.getStatus()) && !JOURNEY_TASK_TYPE.contains(TaskType.of(task.getTaskType())) && checkTaskReferenceName(task.getReferenceTaskName())) {
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

    private boolean checkTaskReferenceName(String taskReferenceName) {
        for (String referenceName : JOURNEY_TASK_REFERENCE_NAME) {
            if (referenceName.startsWith(taskReferenceName)) {
                return true;
            }
        }
        return false;
    }

    private void publishMessage(String accountId, JsonNode event, EventType eventType) {
        centralProducer.publish(accountId, event, getPayloadType(eventType));
        LOGGER.debug("Publishing event: {}", event);
    }

    private String getPayloadType(JourneyConstants.EventType eventType) {
        return eventType == JourneyConstants.EventType.WORKFLOW ? CONDUCTOR_WORKFLOW_EVENT : CONDUCTOR_TASK_EVENT;
    }


}
