package com.netflix.conductor.freshworks.deletion.config;

import org.springframework.stereotype.Component;

import com.freshworks.boot.sdk.kafka.service.PayloadVersionFetcher;
import com.netflix.conductor.freshworks.deletion.model.DataDeletionStatusPayload;

/**
 * {@code freshworks-boot-central-kafka-sdk}'s producer auto-configuration requires a
 * {@link PayloadVersionFetcher} bean to exist at startup, so this must always be registered.
 *
 * <p>{@link com.netflix.conductor.freshworks.deletion.DataDeletionStatusPublisher} always sets
 * {@code payloadVersion} explicitly on the payload it builds, so this fetcher is only consulted as
 * a fallback and never needs to branch on payload type — data-deletion is the only publisher in
 * this service.
 */
@Component
public class DataDeletionPayloadVersionFetcher implements PayloadVersionFetcher {

    @Override
    public String getPayloadVersionByPayloadType(String payloadType) {
        return DataDeletionStatusPayload.PAYLOAD_VERSION;
    }
}
