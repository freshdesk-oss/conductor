package com.netflix.conductor.core.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.core.exception.TransientException;
import com.netflix.conductor.model.TaskModel;

import com.netflix.conductor.model.WorkflowModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static com.netflix.conductor.model.TaskModel.Status.COMPLETED_WITH_ERRORS;
import static com.netflix.conductor.model.TaskModel.Status.FAILED;


@Service
public class EventPublisher {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventPublisher.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final EventFilterConfig eventFilterConfig;
    private final RetryTemplate retryTemplate;

    @Value("${conductor.central.url}")
    private String url;
    @Value("${conductor.central.token}")
    private String token;

    @Value("${conductor.status-listener.enabled}")
    private boolean isStatusListenerEnabled;
    @Value("${conductor.status-listener.module.type}")
    private String moduleType;

    private static final Long DEFAULT_INITIAL_INTERVAL = 500L;
    private static final Double DEFAULT_MULTIPLIER = 2.0;
    private static final Integer DEFAULT_MAX_ATTEMPTS = 3;
    private static final String PAYLOAD_VERSION = "1.0";
    private static final int HTTP_STATUS_SUCCESS = 2;
    private static final int HTTP_STATUS_SERVER_ERROR_5XX = 5;
    private static final int HTTP_STATUS_CLIENT_ERROR_4XX = 4;

    public EventPublisher(EventFilterConfig eventFilterConfig) {
        this.eventFilterConfig = eventFilterConfig;
        this.retryTemplate = centralRetryTemplate();
    }

    /***
     * This method creates a retry template with an exponential backoff policy.
     * TransientException is used to trigger the retry and this will be triggered only for the server error 5xx status code from the central.
     * @return
     */
    private RetryTemplate centralRetryTemplate() {
        RetryTemplate template = new RetryTemplate();

        ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
        backOffPolicy.setInitialInterval(DEFAULT_INITIAL_INTERVAL); // Initial delay
        backOffPolicy.setMultiplier(DEFAULT_MULTIPLIER);// Exponential multiplier for backoff (1, 2, 4 seconds, etc.)

        Map<Class<? extends Throwable>, Boolean> retryableExceptions = new HashMap<>();
        retryableExceptions.put(TransientException.class, true);

        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy(DEFAULT_MAX_ATTEMPTS, retryableExceptions);

        template.setBackOffPolicy(backOffPolicy);
        template.setRetryPolicy(retryPolicy);

        return template;
    }

    /***
     * This method pushes the workflow events to the central service.
     * If the status listener is enabled and the workflow status is RUNNING, COMPLETED, FAILED, TERMINATED
     * then the workflow event is published to the central service.
     * @param workflow
     */
    public void pushWorkflowEvents(WorkflowModel workflow) {
        if (!isStatusListenerEnabled) {
            return;
        }
        if (eventFilterConfig.shouldPublishEvent("workflow", workflow, moduleType)) {
            ObjectNode payload = objectMapper.createObjectNode();
            payload.set("input_params", objectMapper.valueToTree(workflow.getInput()));
            payload.put("parent_workflow_id", workflow.getParentWorkflowId());
            payload.put("workflow_id", workflow.getWorkflowId());
            payload.put("status", workflow.getStatus().name());
            payload.put("reason_for_incompletion", workflow.getReasonForIncompletion());

            Object accountId = workflow.getInput().get("accountId");
            if (Objects.nonNull(accountId)) {
                sendCentralMessage(String.valueOf(accountId), "journey_conductor_workflow_event", payload);
            } else {
                LOGGER.error("Account ID is missing in the workflow input. Workflow ID: {}", workflow.getWorkflowId());
            }
        } else {
            LOGGER.info("Skipping workflow event for workflow id: {} status: {}", workflow.getWorkflowId(), workflow.getStatus());
        }
    }

    /***
     * This method pushes the workflow events to the central service.
     * If the status listener is enabled and the current retry should be the last retry for the failed and completed_with_error task
     * and also if it passes the event filter conditions then the task event is published to the central service.
     * @param task
     */
    public void pushTaskEvents(TaskModel task) {
        if (!isStatusListenerEnabled) {
            return;
        }
        if (maxRetryReached(task) && eventFilterConfig.shouldPublishEvent("task", task, moduleType)) {
            ObjectNode payload = objectMapper.createObjectNode();
            payload.set("input_params", objectMapper.valueToTree(task.getInputData()));
            payload.put("task_id", task.getTaskId());
            payload.put("status", task.getStatus().name());
            payload.put("reason_for_incompletion", task.getReasonForIncompletion());

            Object accountId = task.getInputData().get("accountId");
            if (Objects.nonNull(accountId)) {
                sendCentralMessage(String.valueOf(accountId), "journey_conductor_task_event", payload);
            } else {
                LOGGER.error("Account ID is missing in the task input. Task ID: {}", task.getTaskId());
            }
        } else {
            LOGGER.info("Skipping task event for task id: {} status: {} current retry count: {}", task.getTaskId(), task.getStatus(), task.getRetryCount());
        }
    }

    /***
     * This method checks if the current retry should be the last retry for the failed and completed_with_error task
     * @param task
     * @return true if the current retry should be the last retry for the failed and completed_with_error task
     */
    private boolean maxRetryReached(TaskModel task) {
        if (!FAILED.equals(task.getStatus()) && !COMPLETED_WITH_ERRORS.equals(task.getStatus())) {
            return true;
        }
        TaskDef taskDefinition = task.getTaskDefinition().orElse(null);
        return taskDefinition != null && task.getRetryCount() == taskDefinition.getRetryCount();
    }

    /***
     * This method pushes the workflow and task events to the central collector api.
     * Retry is configured only for the server error 5xx status code, in case of client error 4xx status code the error is logged.
     * @param accountId
     * @param payloadType
     * @param payload
     */
    private void sendCentralMessage(String accountId, String payloadType, ObjectNode payload) {
        ObjectNode centralMessage = objectMapper.createObjectNode();
        centralMessage.put("account_id", accountId);
        centralMessage.set("payload", payload);
        centralMessage.put("payload_type", payloadType);
        centralMessage.put("payload_version", PAYLOAD_VERSION);

        try {
            retryTemplate.execute(context -> {
                HttpClient client = HttpClient.newHttpClient();
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .headers(getHeaders())
                        .POST(HttpRequest.BodyPublishers.ofString(centralMessage.toString()))
                        .build();
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

                int responseStatus = getStatus(response.statusCode());
                if (responseStatus == HTTP_STATUS_SUCCESS) {
                    LOGGER.info("Successfully sent central message. Payload: {}", payload);
                } else if (responseStatus == HTTP_STATUS_SERVER_ERROR_5XX) {
                    throw new TransientException("Server error: " + response.body()); // Triggers retry
                } else if (responseStatus == HTTP_STATUS_CLIENT_ERROR_4XX) {
                    LOGGER.error("Non-retryable error while sending central message. Payload: {}, Response: {}", payload, response.body());
                }
                return null;
            });
        } catch (Exception ex) {
            LOGGER.error("Unexpected error while sending central message. Payload: {}, Error: {}", payload, ex.getMessage());
        }
    }

    private String[] getHeaders() {
        return new String[] {
                "content-type", "application/json",
                "service", token
        };
    }

    private int getStatus(int status) {
        return status / 100;
    }
}
