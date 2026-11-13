/*
 * Copyright 2022 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.core.execution;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.netflix.conductor.core.config.ConductorProperties;
import com.netflix.conductor.core.dal.ExecutionDAOFacade;
import com.netflix.conductor.core.execution.tasks.WorkflowSystemTask;
import com.netflix.conductor.core.sync.Lock;
import com.netflix.conductor.core.utils.QueueUtils;
import com.netflix.conductor.dao.MetadataDAO;
import com.netflix.conductor.dao.QueueDAO;
import com.netflix.conductor.metrics.Monitors;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;

@Component
public class AsyncSystemTaskExecutor {

    private final ExecutionDAOFacade executionDAOFacade;
    private final QueueDAO queueDAO;
    private final MetadataDAO metadataDAO;
    private final long queueTaskMessagePostponeSecs;
    private final long systemTaskCallbackTime;
    private final WorkflowExecutor workflowExecutor;

    /**
     * JVM-local guard against concurrent {@link WorkflowSystemTask#start} for the same task id.
     *
     * <p>Closed together with {@link #startLock}: {@code WorkflowRepairService} re-queues a
     * still-{@code SCHEDULED} task while an earlier delivery is inside {@code start()} or has
     * returned but not yet persisted {@code IN_PROGRESS}. For {@code SUB_WORKFLOW}, a second
     * {@code start()} creates a second child workflow for one parent task (FS-399078).
     *
     * <p>The local map only sees this process. {@link #startLock} is the Redis lock
     * ({@code provideRedisLock}) so another replica cannot start the same task. The claim is held
     * until after {@code updateTask}. The task is re-read after the lock is acquired, so a
     * delivery that already loaded {@code SCHEDULED} cannot start from that stale copy.
     */
    private final ConcurrentHashMap<String, Boolean> tasksStarting = new ConcurrentHashMap<>();

    /** How long a second replica waits for the replica already inside {@code start()}. */
    private static final long START_LOCK_TIME_TO_TRY_SECS = 5;

    private static final String START_LOCK_PREFIX = "system-task-start:";

    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncSystemTaskExecutor.class);

    /**
     * Redis lock when {@code conductor.workflow-scylla-execution-lock.enabled=true}. Null in
     * deployments that do not create {@code provideRedisLock}; the in-process map still applies.
     */
    private final Lock startLock;

    @Autowired
    public AsyncSystemTaskExecutor(
            ExecutionDAOFacade executionDAOFacade,
            QueueDAO queueDAO,
            MetadataDAO metadataDAO,
            ConductorProperties conductorProperties,
            WorkflowExecutor workflowExecutor,
            @Qualifier("provideRedisLock") ObjectProvider<Lock> startLockProvider) {
        this(
                executionDAOFacade,
                queueDAO,
                metadataDAO,
                conductorProperties,
                workflowExecutor,
                startLockProvider.getIfAvailable());
    }

    AsyncSystemTaskExecutor(
            ExecutionDAOFacade executionDAOFacade,
            QueueDAO queueDAO,
            MetadataDAO metadataDAO,
            ConductorProperties conductorProperties,
            WorkflowExecutor workflowExecutor,
            Lock startLock) {
        this.executionDAOFacade = executionDAOFacade;
        this.queueDAO = queueDAO;
        this.metadataDAO = metadataDAO;
        this.workflowExecutor = workflowExecutor;
        this.startLock = startLock;
        this.systemTaskCallbackTime =
                conductorProperties.getSystemTaskWorkerCallbackDuration().getSeconds();
        this.queueTaskMessagePostponeSecs =
                conductorProperties.getTaskExecutionPostponeDuration().getSeconds();
    }

    /**
     * Executes and persists the results of an async {@link WorkflowSystemTask}.
     *
     * @param systemTask The {@link WorkflowSystemTask} to be executed.
     * @param taskId The id of the {@link TaskModel} object.
     */
    public void execute(WorkflowSystemTask systemTask, String taskId) {
        TaskModel task = loadTaskQuietly(taskId);
        if (task == null) {
            LOGGER.error("TaskId: {} could not be found while executing {}", taskId, systemTask);
            try {
                LOGGER.debug(
                        "Cleaning up dead task from queue message: taskQueue={}, taskId={}",
                        systemTask.getTaskType(),
                        taskId);
                queueDAO.remove(systemTask.getTaskType(), taskId);
            } catch (Exception e) {
                LOGGER.error(
                        "Failed to remove dead task from queue message: taskQueue={}, taskId={}",
                        systemTask.getTaskType(),
                        taskId);
            }
            return;
        }

        LOGGER.debug("Task: {} fetched from execution DAO for taskId: {}", task, taskId);
        String queueName = QueueUtils.getQueueName(task);
        if (task.getStatus().isTerminal()) {
            // Tune the SystemTaskWorkerCoordinator's queues - if the queue size is very big this
            // can happen!
            LOGGER.info("Task {}/{} was already completed.", task.getTaskType(), task.getTaskId());
            queueDAO.remove(queueName, task.getTaskId());
            return;
        }

        if (task.getStatus().equals(TaskModel.Status.SCHEDULED)) {
            if (executionDAOFacade.exceedsInProgressLimit(task)) {
                LOGGER.warn("Concurrent Execution limited for {}:{}", taskId, task.getTaskDefName());
                postponeQuietly(queueName, task);
                return;
            }
            if (task.getRateLimitPerFrequency() > 0
                    && executionDAOFacade.exceedsRateLimitPerFrequency(
                            task, metadataDAO.getTaskDef(task.getTaskDefName()))) {
                LOGGER.warn(
                        "RateLimit Execution limited for {}:{}, limit:{}",
                        taskId,
                        task.getTaskDefName(),
                        task.getRateLimitPerFrequency());
                postponeQuietly(queueName, task);
                return;
            }
            if (!claimStart(taskId)) {
                LOGGER.warn(
                        "Skipping start for {}/{} - another delivery is already starting this task",
                        task.getTaskType(),
                        taskId);
                postponeQuietly(queueName, task);
                return;
            }
            try {
                // Re-read under the lock: the copy loaded above may predate another delivery's
                // updateTask.
                TaskModel current = loadTaskQuietly(taskId);
                if (current == null || current.getStatus() != TaskModel.Status.SCHEDULED) {
                    LOGGER.warn("Skipping start for {}/{} - task already started", task.getTaskType(), taskId);
                    return;
                }
                run(systemTask, current, queueName);
            } finally {
                releaseStart(taskId);
            }
        } else {
            run(systemTask, task, queueName);
        }
    }

    /**
     * Guards {@code start()} for one task id (FS-399078). Held until after {@code updateTask}, so a
     * repair re-queue cannot call {@code start()} again while the DB row is still SCHEDULED. Local
     * map covers this pod; {@link #startLock} (Redis, optional) covers other pods.
     */
    private boolean claimStart(String taskId) {
        if (tasksStarting.putIfAbsent(taskId, Boolean.TRUE) != null) {
            return false;
        }
        if (startLock != null
                && !startLock.acquireLock(
                        START_LOCK_PREFIX + taskId, START_LOCK_TIME_TO_TRY_SECS, TimeUnit.SECONDS)) {
            tasksStarting.remove(taskId);
            return false;
        }
        return true;
    }

    private void releaseStart(String taskId) {
        if (startLock != null) {
            startLock.releaseLock(START_LOCK_PREFIX + taskId);
        }
        tasksStarting.remove(taskId);
    }

    private void run(WorkflowSystemTask systemTask, TaskModel task, String queueName) {
        String taskId = task.getTaskId();
        boolean hasTaskExecutionCompleted = false;
        String workflowId = task.getWorkflowInstanceId();
        // if we are here the Task object is updated and needs to be persisted regardless of an
        // exception
        try {
            WorkflowModel workflow =
                    executionDAOFacade.getWorkflowModel(
                            workflowId, systemTask.isTaskRetrievalRequired());

            if (workflow.getStatus().isTerminal()) {
                LOGGER.info(
                        "Workflow {} has been completed for {}/{}",
                        workflow.toShortString(),
                        systemTask,
                        task.getTaskId());
                if (!task.getStatus().isTerminal()) {
                    task.setStatus(TaskModel.Status.CANCELED);
                    task.setReasonForIncompletion(
                            String.format(
                                    "Workflow is in %s state", workflow.getStatus().toString()));
                }
                queueDAO.remove(queueName, task.getTaskId());
                return;
            }

            LOGGER.debug(
                    "Executing {}/{} in {} state",
                    task.getTaskType(),
                    task.getTaskId(),
                    task.getStatus());

            boolean isTaskAsyncComplete = systemTask.isAsyncComplete(task);
            if (task.getStatus() == TaskModel.Status.SCHEDULED || !isTaskAsyncComplete) {
                task.incrementPollCount();
            }

            if (task.getStatus() == TaskModel.Status.SCHEDULED) {
                task.setStartTime(System.currentTimeMillis());
                Monitors.recordQueueWaitTime(task.getTaskType(), task.getQueueWaitTime());
                systemTask.start(workflow, task, workflowExecutor);
            } else if (task.getStatus() == TaskModel.Status.IN_PROGRESS) {
                systemTask.execute(workflow, task, workflowExecutor);
            }

            // Update message in Task queue based on Task status
            // Remove asyncComplete system tasks from the queue that are not in SCHEDULED state
            if (isTaskAsyncComplete && task.getStatus() != TaskModel.Status.SCHEDULED) {
                queueDAO.remove(queueName, task.getTaskId());
                hasTaskExecutionCompleted = true;
            } else if (task.getStatus().isTerminal()) {
                task.setEndTime(System.currentTimeMillis());
                queueDAO.remove(queueName, task.getTaskId());
                hasTaskExecutionCompleted = true;
                LOGGER.debug("{} removed from queue: {}", task, queueName);
            } else {
                task.setCallbackAfterSeconds(systemTaskCallbackTime);
                systemTask
                        .getEvaluationOffset(task, systemTaskCallbackTime)
                        .ifPresentOrElse(
                                task::setCallbackAfterSeconds,
                                () -> task.setCallbackAfterSeconds(systemTaskCallbackTime));
                queueDAO.postpone(
                        queueName,
                        task.getTaskId(),
                        task.getWorkflowPriority(),
                        task.getCallbackAfterSeconds());
                LOGGER.debug("{} postponed in queue: {}", task, queueName);
            }

            LOGGER.debug(
                    "Finished execution of {}/{}-{}",
                    systemTask,
                    task.getTaskId(),
                    task.getStatus());
        } catch (Exception e) {
            Monitors.error(AsyncSystemTaskExecutor.class.getSimpleName(), "executeSystemTask");
            LOGGER.error("Error executing system task - {}, with id: {}", systemTask, taskId, e);
        } finally {
            executionDAOFacade.updateTask(task);
            // if the current task execution has completed, then the workflow needs to be evaluated
            if (hasTaskExecutionCompleted) {
                workflowExecutor.decide(workflowId);
            }
        }
    }

    private void postponeQuietly(String queueName, TaskModel task) {
        try {
            queueDAO.postpone(
                    queueName,
                    task.getTaskId(),
                    task.getWorkflowPriority(),
                    queueTaskMessagePostponeSecs);
        } catch (Exception e) {
            LOGGER.error("Error postponing task: {} in queue: {}", task.getTaskId(), queueName);
        }
    }

    private TaskModel loadTaskQuietly(String taskId) {
        try {
            return executionDAOFacade.getTaskModel(taskId);
        } catch (Exception e) {
            return null;
        }
    }
}
