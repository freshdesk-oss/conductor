package com.netflix.conductor.core.central.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import lombok.Builder;
import lombok.Data;

import javax.validation.constraints.NotNull;

@Data
@Builder
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class CentralData {
    @NotNull(message = "The account id is required.")
    private String accountId;

    @NotNull(message = "The payload is required.")
    private JsonNode payload;

    @NotNull(message = "The payload type is required.")
    private String payloadType;

    private String service;

    private String region;

    private String pod;

    private String payloadVersion;
}
