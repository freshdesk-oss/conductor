package com.netflix.conductor.freshworks.deletion.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Outer Central envelope for an inbound event, shaped as
 * {@code {"data": {"payload": {...}}, "meta": {...}}}. Only {@code data.payload} is consumed; every
 * other field (including {@code meta}) is ignored.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CentralDeletionEnvelope {

    @JsonProperty("data")
    private EnvelopeData data;

    public EnvelopeData getData() {
        return data;
    }

    public void setData(EnvelopeData data) {
        this.data = data;
    }

    public AccountDeletionRequestedEvent getPayload() {
        return data != null ? data.getPayload() : null;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class EnvelopeData {

        @JsonProperty("payload")
        private AccountDeletionRequestedEvent payload;

        public AccountDeletionRequestedEvent getPayload() {
            return payload;
        }

        public void setPayload(AccountDeletionRequestedEvent payload) {
            this.payload = payload;
        }
    }
}
