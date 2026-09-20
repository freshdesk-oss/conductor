package com.netflix.conductor.freshworks.deletion;

import java.time.Instant;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.freshworks.boot.messaging.KafkaMessageKey;
import com.freshworks.boot.sdk.kafka.model.CentralData;
import com.freshworks.boot.sdk.kafka.model.CentralPayload;
import com.freshworks.boot.sdk.kafka.service.KafkaPublisher;
import com.netflix.conductor.freshworks.deletion.model.DataDeletionRequest;
import com.netflix.conductor.freshworks.deletion.model.DataDeletionRequestPayload;
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
    private final String serviceName;

    public DataDeletionStatusPublisher(
            KafkaPublisher<KafkaMessageKey, CentralPayload<DataDeletionStatusPayload>>
                    kafkaPublisher,
            @Value("${conductor.data-deletion.service:fs-caas}") String serviceName) {
        this.kafkaPublisher = kafkaPublisher;
        this.serviceName = serviceName;
    }

    public void publish(
            DeletionStatus status,
            DataDeletionRequest request,
            String message,
            String traceId) {
        DataDeletionRequestPayload requestPayload = request.getPayload();
        try {
            CentralPayload<DataDeletionStatusPayload> payload =
                    buildPayload(status, request, message);
            kafkaPublisher
                    .publish(payload)
                    .addCallback(
                            result -> {
                                LOGGER.info(
                                        "Published ACCOUNT_DELETION_STATUS deletion_request_id={} account_id={} "
                                                + "product_account_id={} status={} traceId={} topic={} partition={} offset={}",
                                        requestPayload.getDeletionRequestId(),
                                        requestPayload.getAccountId(),
                                        requestPayload.getProductAccountId(),
                                        status,
                                        traceId,
                                        result.getRecordMetadata().topic(),
                                        result.getRecordMetadata().partition(),
                                        result.getRecordMetadata().offset());
                            },
                            ex -> logPublishFailure(status, requestPayload, traceId, ex));
        } catch (Exception e) {
            logPublishFailure(status, requestPayload, traceId, e);
        }
    }

    private void logPublishFailure(
            DeletionStatus status,
            DataDeletionRequestPayload requestPayload,
            String traceId,
            Throwable cause) {
        LOGGER.error(
                "Failed to publish ACCOUNT_DELETION_STATUS deletion_request_id={} account_id={} "
                        + "product_account_id={} status={} traceId={}",
                requestPayload.getDeletionRequestId(),
                requestPayload.getAccountId(),
                requestPayload.getProductAccountId(),
                status,
                traceId,
                cause);
    }

    /**
     * {@code region} and {@code service} are echoed from the inbound request. {@code pod} is not
     * part of this status event, so it is left unset.
     *
     * <p>Setting {@code region}/{@code service} explicitly also means {@code DefaultKafkaPublisher}
     * never falls back to {@code freshworks.boot.kafka.producer.region}/{@code .serviceName}, which
     * is why those properties no longer need to be configured at all.
     */
    private CentralPayload<DataDeletionStatusPayload> buildPayload(
            DeletionStatus status, DataDeletionRequest request, String message) {
        DataDeletionRequestPayload requestPayload = request.getPayload();
        DataDeletionStatusPayload payload = new DataDeletionStatusPayload();
        payload.setDeletionRequestId(requestPayload.getDeletionRequestId());
        payload.setServiceName(serviceName);
        payload.setOrganisationId(requestPayload.getOrganisationId());
        payload.setBundleId(requestPayload.getBundleId());
        payload.setAccountId(requestPayload.getAccountId());
        payload.setProduct(requestPayload.getProduct());
        payload.setProductAccountId(requestPayload.getProductAccountId());
        payload.setProductId(requestPayload.getProductId());
        payload.setStatus(status.name());
        payload.setActionTimestamp(Instant.now().toEpochMilli());
        payload.setMessage(message);

        CentralData<DataDeletionStatusPayload> data =
                CentralData.<DataDeletionStatusPayload>builder()
                        .accountId(requestPayload.getProductAccountId())
                        .organisationId(requestPayload.getOrganisationId())
                        .productId(requestPayload.getProductId())
                        .bundleId(requestPayload.getBundleId())
                        .payloadType(DataDeletionStatusPayload.EVENT_TYPE)
                        .payloadVersion(DataDeletionStatusPayload.PAYLOAD_VERSION)
                        .region(StringUtils.defaultIfBlank(request.getRegion(), ""))
                        .service(StringUtils.defaultIfBlank(request.getService(), ""))
                        .payload(payload)
                        .build();
        return new CentralPayload<>(data);
    }
}
