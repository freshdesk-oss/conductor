package com.netflix.conductor.freshworks.deletion.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Kafka record value published to Central, wrapping {@link CentralData}. Mirrors the shape of
 * {@code com.freshworks.boot.sdk.kafka.model.CentralPayload}.
 *
 * @param <P> the inner payload type (e.g. {@link AccountDeletionStatusPayload})
 */
public class CentralPayload<P> {

    @JsonProperty("data")
    private CentralData<P> data;

    public CentralPayload() {}

    public CentralPayload(CentralData<P> data) {
        this.data = data;
    }

    public CentralData<P> getData() {
        return data;
    }

    public void setData(CentralData<P> data) {
        this.data = data;
    }
}
