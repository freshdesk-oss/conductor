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

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;

import com.netflix.conductor.core.config.ConductorProperties;
import com.netflix.conductor.core.dal.ExecutionDAOFacade;
import com.netflix.conductor.core.execution.tasks.WorkflowSystemTask;
import com.netflix.conductor.dao.MetadataDAO;
import com.netflix.conductor.dao.QueueDAO;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Reproduces the "duplicate SUB_WORKFLOW start" race described in FS-399078: two concurrent
 * deliveries for the *same* SCHEDULED task id (e.g. the original poll, and a re-queue by
 * WorkflowRepairService that mistakes an in-flight task for a lost one) must not both invoke
 * {@link WorkflowSystemTask#start(WorkflowModel, TaskModel, WorkflowExecutor)} - otherwise a
 * SUB_WORKFLOW task ends up starting two child workflow instances for the same parent task.
 */
public class TestAsyncSystemTaskExecutor {

    private static final String TASK_ID = "task-1";
    private static final String WORKFLOW_ID = "workflow-1";

    private ExecutionDAOFacade executionDAOFacade;
    private QueueDAO queueDAO;
    private MetadataDAO metadataDAO;
    private WorkflowExecutor workflowExecutor;
    private AsyncSystemTaskExecutor asyncSystemTaskExecutor;

    @Before
    public void setUp() {
        executionDAOFacade = mock(ExecutionDAOFacade.class);
        queueDAO = mock(QueueDAO.class);
        metadataDAO = mock(MetadataDAO.class);
        workflowExecutor = mock(WorkflowExecutor.class);
        ConductorProperties properties = mock(ConductorProperties.class);
        when(properties.getSystemTaskWorkerCallbackDuration()).thenReturn(Duration.ofSeconds(30));
        when(properties.getTaskExecutionPostponeDuration()).thenReturn(Duration.ofSeconds(60));

        asyncSystemTaskExecutor =
                new AsyncSystemTaskExecutor(
                        executionDAOFacade, queueDAO, metadataDAO, properties, workflowExecutor);

        WorkflowModel workflow = new WorkflowModel();
        workflow.setWorkflowId(WORKFLOW_ID);
        workflow.setStatus(WorkflowModel.Status.RUNNING);
        when(executionDAOFacade.getWorkflowModel(anyString(), org.mockito.ArgumentMatchers.anyBoolean()))
                .thenReturn(workflow);
    }

    /**
     * Every call to {@link ExecutionDAOFacade#getTaskModel(String)} simulates a fresh read of the
     * task row from the datastore. Both concurrent deliveries observe the task as SCHEDULED
     * because - just like in production - neither delivery has persisted the IN_PROGRESS
     * transition yet at the moment the second delivery reads the row.
     */
    private TaskModel freshScheduledTask() {
        TaskModel task = new TaskModel();
        task.setTaskId(TASK_ID);
        task.setWorkflowInstanceId(WORKFLOW_ID);
        task.setTaskType("SUB_WORKFLOW");
        task.setTaskDefName("SUB_WORKFLOW");
        task.setStatus(TaskModel.Status.SCHEDULED);
        return task;
    }

    /**
     * A stand-in for {@code SubWorkflow}: records every {@code start()} invocation and blocks the
     * *first* caller on a latch so the test can deterministically force a second, concurrent
     * delivery to observe the task as still SCHEDULED while the first start() is in flight -
     * mirroring the WorkflowRepairService re-queue race.
     */
    private static final class BlockingSystemTask extends WorkflowSystemTask {
        private final List<Thread> startedBy = new CopyOnWriteArrayList<>();
        private final AtomicInteger startCount = new AtomicInteger(0);
        private final CountDownLatch firstCallerEntered = new CountDownLatch(1);
        private final CountDownLatch releaseFirstCaller = new CountDownLatch(1);

        BlockingSystemTask() {
            super("SUB_WORKFLOW");
        }

        @Override
        public void start(WorkflowModel workflow, TaskModel task, WorkflowExecutor executor) {
            startedBy.add(Thread.currentThread());
            int callNumber = startCount.incrementAndGet();
            if (callNumber == 1) {
                firstCallerEntered.countDown();
                await(releaseFirstCaller);
            }
            task.setSubWorkflowId("child-workflow-" + callNumber);
            task.setStatus(TaskModel.Status.IN_PROGRESS);
        }

        @Override
        public boolean isAsync() {
            return true;
        }

        @Override
        public boolean isAsyncComplete(TaskModel task) {
            return true;
        }

        private static void await(CountDownLatch latch) {
            try {
                if (!latch.await(5, TimeUnit.SECONDS)) {
                    throw new AssertionError("Timed out waiting for latch");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        }
    }

    /**
     * Without the fix: two concurrent deliveries for the same SCHEDULED task id both call
     * start() -> two child workflows would be created for one SUB_WORKFLOW task (FS-399078).
     * With the fix: the second, overlapping delivery is recognized as a duplicate of an
     * in-flight start() and is skipped, so start() is invoked exactly once.
     */
    @Test
    public void duplicateConcurrentDeliveryOfSameScheduledTaskDoesNotStartTwice() throws Exception {
        BlockingSystemTask systemTask = new BlockingSystemTask();
        when(executionDAOFacade.getTaskModel(TASK_ID))
                .thenAnswer(invocation -> freshScheduledTask());

        // Delivery #1: e.g. the original poll of the SUB_WORKFLOW queue. Runs on its own thread
        // and blocks inside start() until we release it, simulating a start() call that is slow
        // (DB write latency, lock contention, GC pause, etc.) - exactly the condition that opens
        // the race window in AsyncSystemTaskExecutor.execute().
        Thread firstDelivery =
                new Thread(() -> asyncSystemTaskExecutor.execute(systemTask, TASK_ID), "delivery-1");
        firstDelivery.start();

        // Wait until delivery #1 is confirmed to be inside start() (i.e. mid-flight) before
        // firing delivery #2, so this test deterministically hits the race window instead of
        // relying on wall-clock timing.
        assertTrue(
                "Delivery #1 should have entered start() within 5s",
                systemTask.firstCallerEntered.await(5, TimeUnit.SECONDS));

        // Delivery #2: e.g. WorkflowRepairService re-queueing the same task id because it
        // observed the task as SCHEDULED and not present in the queue (queueDAO.containsMessage
        // returned false) while delivery #1 was still executing start(). This must NOT invoke
        // start() a second time.
        asyncSystemTaskExecutor.execute(systemTask, TASK_ID);

        systemTask.releaseFirstCaller.countDown();
        firstDelivery.join(TimeUnit.SECONDS.toMillis(5));

        assertEquals(
                "start() must be invoked exactly once across both concurrent deliveries of the "
                        + "same SCHEDULED task id - a second invocation means a duplicate child "
                        + "workflow would be created for the same SUB_WORKFLOW task (FS-399078)",
                1,
                systemTask.startCount.get());
    }
}
