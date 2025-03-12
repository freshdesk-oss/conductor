package com.netflix.conductor.core.status;

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class TaskEvent {
    private String journeyReqId;
    private Long nodeId;
    private String taskInstanceId;
    private String status;
    private Integer statusId;
}
