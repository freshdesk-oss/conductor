package com.netflix.conductor.core.central;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class CentralConstants {
    public static final String CONDUCTOR_WORKFLOW_EVENT = "journey_conductor_workflow_event";
    public static final String CONDUCTOR_TASK_EVENT = "journey_conductor_task_event";
    public static final String PAYLOAD_VERSION = "1.0";
    public static final String CONTENT_TYPE = "content-type";
    public static final String SERVICE = "service";
    public static final String X_REQUEST_ID = "x-request-id";
    public static final String CONTENT_TYPE_JSON = "application/json";
}
