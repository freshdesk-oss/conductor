package com.netflix.conductor.core.status;

import com.netflix.conductor.common.metadata.tasks.TaskType;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;

import java.util.List;

public class JourneyConstants {

    public enum WorkflowType {
        JOURNEY,
        PHASE
    }

    public enum EventType {
        WORKFLOW,
        TASK
    }

    public static final String JOURNEY_REQUEST_ID = "journeyRequestId";
    public static final String ACCOUNT_ID = "accountId";
    public static final String JOURNEY_CONFIG_ID = "journeyConfigId";
    public static final String NODE_ID = "nodeId";
    public static final String W_TIMER = "wTimer";
    public static final String W_CLEANUP = "wCleanup";
    public static final String W_DECISION = "wDecision";

    public static final List<WorkflowModel.Status> JOURNEY_WORKFLOW_STATUS = List.of(WorkflowModel.Status.RUNNING, WorkflowModel.Status.COMPLETED, WorkflowModel.Status.FAILED, WorkflowModel.Status.TERMINATED);
    public static final List<TaskModel.Status> JOURNEY_TASK_STATUS = List.of(TaskModel.Status.CANCELED, TaskModel.Status.COMPLETED_WITH_ERRORS, TaskModel.Status.FAILED, TaskModel.Status.FAILED_WITH_TERMINAL_ERROR);
    public static final List<TaskType> JOURNEY_TASK_TYPE = List.of(TaskType.FORK_JOIN, TaskType.SWITCH, TaskType.JOIN, TaskType.SUB_WORKFLOW);
    public static final List<String> JOURNEY_TASK_REFERENCE_NAME = List.of(W_TIMER, W_CLEANUP, W_DECISION);

}
