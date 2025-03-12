package com.netflix.conductor.core.status;

import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;
import org.springframework.stereotype.Service;


@Service
public interface EventPublisher {

    void pushWorkflowEvents(WorkflowModel workflow);

    void pushTaskEvents(TaskModel task);

}
