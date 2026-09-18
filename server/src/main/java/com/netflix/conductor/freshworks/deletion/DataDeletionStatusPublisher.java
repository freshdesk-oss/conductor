package com.netflix.conductor.freshworks.deletion;

import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.freshworks.boot.messaging.KafkaMessageKey;
import com.freshworks.boot.sdk.kafka.model.CentralData;
import com.freshworks.boot.sdk.kafka.model.CentralPayload;
import com.freshworks.boot.sdk.kafka.service.KafkaPublisher;
import com.netflix.conductor.freshworks.deletion.model.DataDeletionRequestedEvent;
import com.netflix.conductor.freshworks.deletion.model.DataDeletionStatusPayload;
import com.netflix.conductor.freshworks.deletion.model.DeletionStatus;

/**
 * Builds and publishes {@code ACCOUNT_DELETION_STATUS} events to Central for each stage of an
 * account purge, via {@code freshworks-boot-central-kafka-sdk}'s {@link KafkaPublisher}. A publish
 * failure is logged but never propagated so it cannot crash the purge worker; a missing terminal
 * status is caught by Baikal SLA monitoring.
 */
@Component
public class DataDeletionStatusPublisher {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(DataDeletionStatusPublisher.class);

    private final KafkaPublisher<KafkaMessageKey, CentralPayload<DataDeletionStatusPayload>>
            kafkaPublisher;
    private final String service;

    public DataDeletionStatusPublisher(
            KafkaPublisher<KafkaMessageKey, CentralPayload<DataDeletionStatusPayload>>
                    kafkaPublisher,
            @Value("${conductor.data-deletion.service:fs-caas}") String service) {
        this.kafkaPublisher = kafkaPublisher;
        this.service = service;
    }

    public void publish(
            DeletionStatus status,
            DataDeletionRequestedEvent event,
            String message,
            String traceId) {
        try {
            CentralPayload<DataDeletionStatusPayload> payload =
                    buildPayload(status, event, message);
            kafkaPublisher
                    .publish(payload)
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
                            },
                            ex -> logPublishFailure(status, event, traceId, ex));
        } catch (Exception e) {
            logPublishFailure(status, event, traceId, e);
        }
    }

    private void logPublishFailure(
            DeletionStatus status,
            DataDeletionRequestedEvent event,
            String traceId,
            Throwable cause) {
        LOGGER.error(
                "Failed to publish ACCOUNT_DELETION_STATUS deletion_request_id={} account_id={} "
                        + "product_account_id={} status={} traceId={}",
                event.getDeletionRequestId(),
                event.getAccountId(),
                event.getProductAccountId(),
                status,
                traceId,
                cause);
    }

    /**
     * {@code region}/{@code pod} are intentionally left unset on the envelope here — {@link
     * KafkaPublisher}'s {@code DefaultKafkaPublisher} auto-fills both from {@code
     * freshworks.boot.kafka.producer.region}/{@code .pod} when null, so there's no need to
     * duplicate that config under {@code conductor.data-deletion.*} as well.
     */
    private CentralPayload<DataDeletionStatusPayload> buildPayload(
            DeletionStatus status, DataDeletionRequestedEvent event, String message) {
        DataDeletionStatusPayload payload = new DataDeletionStatusPayload();
        payload.setDeletionRequestId(event.getDeletionRequestId());
        payload.setService(service);
        payload.setOrganisationId(event.getOrganisationId());
        payload.setBundleId(event.getBundleId());
        payload.setAccountId(event.getAccountId());
        payload.setProduct(event.getProduct());
        payload.setProductAccountId(event.getProductAccountId());
        payload.setProductId(event.getProductId());
        payload.setStatus(status.name());
        payload.setTimestamp(Instant.now().toString());
        payload.setMessage(message);

        CentralData<DataDeletionStatusPayload> data =
                CentralData.<DataDeletionStatusPayload>builder()
                        .accountId(event.getProductAccountId())
                        .organisationId(event.getOrganisationId())
                        .productId(event.getProductId())
                        .bundleId(event.getBundleId())
                        .payloadType(DataDeletionStatusPayload.EVENT_TYPE)
                        .payloadVersion(DataDeletionStatusPayload.PAYLOAD_VERSION)
                        .payload(payload)
                        .build();
        return new CentralPayload<>(data);
    }
}
