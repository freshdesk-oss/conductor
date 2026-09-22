package com.netflix.conductor.freshworks.deletion.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * An inbound {@code ACCOUNT_DELETION_REQUESTED} request as Central delivers it, shaped as
 * {@code {"data": {"payload": {...}, "service": "...", "region": "..."}, "meta": {...}}}.
 *
 * <p>{@code data.payload} carries the request itself; {@code data.service} and {@code data.region}
 * are echoed back onto the outbound {@code ACCOUNT_DELETION_STATUS} envelope.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DataDeletionRequest {

    @JsonProperty("data")
    private RequestData data;

    public DataDeletionRequestPayload getPayload() {
        return data != null ? data.payload : null;
    }

    public String getService() {
        return data != null ? data.service : null;
    }

    public String getRegion() {
        return data != null ? data.region : null;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class RequestData {

        @JsonProperty("payload")
        private DataDeletionRequestPayload payload;

        @JsonProperty("service")
        private String service;

        @JsonProperty("region")
        private String region;
    }
}
