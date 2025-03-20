package com.netflix.conductor.core.status;

public class JourneyConstants {

    public enum WorkflowType {
        JOURNEY,
        PHASE
    }

    public enum EventType {
        WORKFLOW,
        TASK
    }

    public static final String WORKFLOW = "workflow";
    public static final String TASK = "task";
    public static final String JOURNEY_REQUEST_ID = "journeyRequestId";
    public static final String ACCOUNT_ID = "accountId";
    public static final String JOURNEY_CONFIG_ID = "journeyConfigId";
    public static final String NODE_ID = "nodeId";
    public static final String CONTAINS = "contains";
    public static final String STARTS_WITH = "startsWith";
    public static final String EXCLUDES = "excludes";
    public static final String GET = "get";
}
