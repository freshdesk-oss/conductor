package com.netflix.conductor.freshworks.deletion;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.netflix.conductor.freshworks.deletion.model.DataDeletionRequest;
import com.netflix.conductor.freshworks.deletion.model.DataDeletionRequestPayload;
import com.netflix.conductor.freshworks.deletion.model.DeletionStatus;

/**
 * Orchestrates data deletion request: runs the hard delete synchronously on the Kafka
 * listener thread, emitting {@code STARTED} then {@code SUCCESS}/{@code NOT_FOUND}/{@code
 * FAILURE}.
 */
@Component
public class DataDeletionService {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataDeletionService.class);

    private final DataDeletionStatusPublisher statusPublisher;
    private final AccountDataPurger purger;
    private final String product;

    public DataDeletionService(
            DataDeletionStatusPublisher statusPublisher,
            AccountDataPurger purger,
            @Value("${conductor.product}") String product) {
        this.statusPublisher = statusPublisher;
        this.purger = purger;
        this.product = product;
    }

    /** Acknowledges the request and runs the purge. */
    public void handle(DataDeletionRequest request, String traceId) {
        DataDeletionRequestPayload requestPayload = request.getPayload();
        LOGGER.info(
                "Received ACCOUNT_DELETION_REQUESTED deletion_request_id={} account_id={} "
                        + "product_account_id={} product={} traceId={}",
                requestPayload.getDeletionRequestId(),
                requestPayload.getAccountId(),
                requestPayload.getProductAccountId(),
                requestPayload.getProduct(),
                traceId);

        if (StringUtils.isBlank(requestPayload.getProductAccountId())) {
            LOGGER.warn(
                    "Rejected ACCOUNT_DELETION_REQUESTED with missing product_account_id"
                            + " deletion_request_id={} traceId={}",
                    requestPayload.getDeletionRequestId(),
                    traceId);
            statusPublisher.publish(
                    DeletionStatus.NOT_FOUND, request, "Missing product_account_id", traceId);
            return;
        }

        if (!product.equals(requestPayload.getProduct())) {
            String message =
                    "No matching product on this instance: received product="
                            + requestPayload.getProduct()
                            + ", expected product="
                            + product;
            LOGGER.info(
                    "Rejected ACCOUNT_DELETION_REQUESTED for product={} (this instance only acts"
                            + " on product={}) deletion_request_id={} traceId={}",
                    requestPayload.getProduct(),
                    product,
                    requestPayload.getDeletionRequestId(),
                    traceId);
            statusPublisher.publish(DeletionStatus.NOT_FOUND, request, message, traceId);
            return;
        }

        runPurge(request, traceId);
    }

    private void runPurge(DataDeletionRequest request, String traceId) {
        DataDeletionRequestPayload requestPayload = request.getPayload();
        try {
            statusPublisher.publish(DeletionStatus.STARTED, request, null, traceId);
            boolean purged =
                    purger.purge(requestPayload.getProductAccountId(), requestPayload.getDeletionRequestId(), traceId);
            if (purged) {
                statusPublisher.publish(
                        DeletionStatus.SUCCESS, request, "Account data deleted", traceId);
            } else {
                statusPublisher.publish(
                        DeletionStatus.NOT_FOUND, request, "No account data found", traceId);
            }
        } catch (RuntimeException e) {
            LOGGER.error(
                    "Data deletion FAILED deletion_request_id={} product_account_id={} traceId={}",
                    requestPayload.getDeletionRequestId(),
                    requestPayload.getProductAccountId(),
                    traceId,
                    e);
            statusPublisher.publish(DeletionStatus.FAILURE, request, e.getMessage(), traceId);
        }
    }
}
