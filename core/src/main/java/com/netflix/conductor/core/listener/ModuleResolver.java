package com.netflix.conductor.core.listener;

import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;

public class ModuleResolver {

    private static final String DEFAULT_MODULE = "default";

    private ModuleResolver() {
        throw new UnsupportedOperationException("Utility class should not be instantiated");
    }

    public static String from(WorkflowModel workflow) {
        Object module = workflow.getInput().get("module");
        return module != null ? module.toString().trim().toLowerCase() : DEFAULT_MODULE;
    }

    public static String from(TaskModel task) {
        Object module = task.getInputData().get("module");
        return module != null ? module.toString().trim().toLowerCase() : DEFAULT_MODULE;
    }
}
