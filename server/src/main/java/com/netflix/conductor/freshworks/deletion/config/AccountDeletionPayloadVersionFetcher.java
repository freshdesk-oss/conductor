package com.netflix.conductor.freshworks.deletion.config;

import org.springframework.stereotype.Component;

import com.freshworks.boot.sdk.kafka.service.PayloadVersionFetcher;

/**
 * Registered unconditionally (not gated on {@code conductor.account-deletion.enabled}), because
 * {@code freshworks-boot-central-kafka-sdk}'s producer auto-configuration requires a
 * {@link PayloadVersionFetcher} bean to exist at startup regardless of whether the feature is
 * enabled. Without it, the app would fail to boot whenever account-deletion is disabled.
 *
 * <p>{@link com.netflix.conductor.freshworks.deletion.AccountDeletionStatusPublisher} always sets
 * {@code payloadVersion} explicitly on the payload it builds, so this fetcher is only consulted as
 * a fallback and never needs to branch on payload type — account-deletion is the only publisher in
 * this service.
 */
@Component
public class AccountDeletionPayloadVersionFetcher implements PayloadVersionFetcher {

    private static final String PAYLOAD_VERSION = "2.0";

    @Override
    public String getPayloadVersionByPayloadType(String payloadType) {
        return PAYLOAD_VERSION;
    }
}
