package com.netflix.conductor.freshworks.deletion;

import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import com.netflix.conductor.freshworks.deletion.config.AccountDeletionProperties;
import com.netflix.conductor.freshworks.deletion.model.AccountDeletionRequestedEvent;
import com.netflix.conductor.freshworks.deletion.model.AccountDeletionStatusPayload;
import com.netflix.conductor.freshworks.deletion.model.CentralData;
import com.netflix.conductor.freshworks.deletion.model.CentralPayload;
import com.netflix.conductor.freshworks.deletion.model.DeletionStatus;
import com.netflix.conductor.metrics.Monitors;

/**
 * Builds and publishes {@code ACCOUNT_DELETION_STATUS} events to Central for each stage of an
 * account purge. A publish failure is logged (and counted) but never propagated so it cannot crash
 * the purge worker; a missing terminal status is caught by Baikal SLA monitoring.
 */
@Component
@ConditionalOnProperty(name = "conductor.account-deletion.enabled", havingValue = "true")
public class AccountDeletionStatusPublisher {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(AccountDeletionStatusPublisher.class);
    private static final String PAYLOAD_VERSION = "2.0";

    private final CentralKafkaPublisher kafkaPublisher;
    private final AccountDeletionProperties properties;

    public AccountDeletionStatusPublisher(
            CentralKafkaPublisher kafkaPublisher, AccountDeletionProperties properties) {
        this.kafkaPublisher = kafkaPublisher;
        this.properties = properties;
    }

    public void publish(
            DeletionStatus status,
            AccountDeletionRequestedEvent event,
            String message,
            String traceId) {
        try {
            CentralPayload<AccountDeletionStatusPayload> payload =
                    buildPayload(status, event, message);
            kafkaPublisher
                    .publish(event.getProductAccountId(), payload)
                    .addCallback(
                            result -> {
                                LOGGER.info(
                                        "Published ACCOUNT_DELETION_STATUS deletion_request_id={} account_id={} "
                                                + "product_account_id={} status={} traceId={} topic={} partition={} offset={}",
                                        event.getDeletionRequestId(),
                                        event.getAccountId(),
                                        event.getProductAccountId(),
                                        status,
                                        traceId,
                                        result.getRecordMetadata().topic(),
                                        result.getRecordMetadata().partition(),
                                        result.getRecordMetadata().offset());
                                Monitors.recordCounter(
                                        "account_deletion_status_published",
                                        1,
                                        "status",
                                        status.name());
                            },
                            ex -> {
                                Monitors.recordCounter(
                                        "account_deletion_status_publish_failed",
                                        1,
                                        "status",
                                        status.name());
                                LOGGER.error(
                                        "Failed to publish ACCOUNT_DELETION_STATUS deletion_request_id={} "
                                                + "account_id={} product_account_id={} status={} traceId={}",
                                        event.getDeletionRequestId(),
                                        event.getAccountId(),
                                        event.getProductAccountId(),
                                        status,
                                        traceId,
                                        ex);
                            });
        } catch (Exception e) {
            Monitors.recordCounter(
                    "account_deletion_status_publish_failed", 1, "status", status.name());
            LOGGER.error(
                    "Failed to publish ACCOUNT_DELETION_STATUS deletion_request_id={} account_id={} "
                            + "product_account_id={} status={} traceId={}",
                    event.getDeletionRequestId(),
                    event.getAccountId(),
                    event.getProductAccountId(),
                    status,
                    traceId,
                    e);
        }
    }

    private CentralPayload<AccountDeletionStatusPayload> buildPayload(
            DeletionStatus status, AccountDeletionRequestedEvent event, String message) {
        AccountDeletionStatusPayload payload = new AccountDeletionStatusPayload();
        payload.setDeletionRequestId(event.getDeletionRequestId());
        payload.setService(properties.getService());
        payload.setOrganisationId(event.getOrganisationId());
        payload.setBundleId(event.getBundleId());
        payload.setAccountId(event.getAccountId());
        payload.setProduct(event.getProduct());
        payload.setProductAccountId(event.getProductAccountId());
        payload.setProductId(event.getProductId());
        payload.setStatus(status.name());
        payload.setTimestamp(Instant.now().toString());
        payload.setMessage(message);

        CentralData<AccountDeletionStatusPayload> data = new CentralData<>();
        data.setAccountId(event.getProductAccountId());
        data.setRegion(properties.getRegion());
        data.setOrganisationId(event.getOrganisationId());
        data.setProductId(event.getProductId());
        data.setBundleId(event.getBundleId());
        data.setPod(properties.getPod());
        data.setPayloadType(AccountDeletionStatusPayload.EVENT_TYPE);
        data.setPayloadVersion(PAYLOAD_VERSION);
        data.setPayload(payload);
        return new CentralPayload<>(data);
    }
}
