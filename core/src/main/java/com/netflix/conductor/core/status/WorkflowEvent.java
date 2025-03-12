package com.netflix.conductor.core.status;

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class WorkflowEvent {
    private String journeyReqId;

    private Long entityId;

    private JourneyConstants.WorkflowType entityType;

    private String workflowInstanceId;

    private String status;

    private Integer statusId;

    private String workflowTerminationReason;
}
