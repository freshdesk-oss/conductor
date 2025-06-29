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
package com.netflix.conductor.core.execution.tasks;

import org.springframework.stereotype.Component;

import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;

import static com.netflix.conductor.common.metadata.tasks.TaskType.TASK_TYPE_DECISION;

/**
 * @deprecated {@link Decision} is deprecated. Use {@link Switch} task for condition evaluation
 *     using the extensible evaluation framework. Also see ${@link
 *     com.netflix.conductor.common.metadata.workflow.WorkflowTask}).
 */
@Deprecated
@Component(TASK_TYPE_DECISION)
public class Decision extends WorkflowSystemTask {

    private static final Tracer tracer = GlobalOpenTelemetry.getTracer("conductor-server-system-task");

    public Decision() {
        super(TASK_TYPE_DECISION);
    }

    @Override
    public boolean execute(
            WorkflowModel workflow, TaskModel task, WorkflowExecutor workflowExecutor) {
        Span span = tracer.spanBuilder("system-task-execute_DECISION")
                .setAttribute("taskId", task.getTaskId()).startSpan();
        try (Scope scope = span.makeCurrent()) {
            task.setStatus(TaskModel.Status.COMPLETED);
            return true;
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            throw e;
        } finally {
            span.end();
        }
    }
}
