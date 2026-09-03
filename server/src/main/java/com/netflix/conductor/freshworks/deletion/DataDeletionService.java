package com.netflix.conductor.freshworks.deletion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.netflix.conductor.freshworks.deletion.model.DataDeletionRequestedEvent;
import com.netflix.conductor.freshworks.deletion.model.DeletionStatus;
import com.netflix.conductor.metrics.Monitors;

/**
 * Orchestrates an account deletion request: acknowledges with {@code QUEUED}, then runs the hard
 * delete synchronously on the Kafka listener thread (emitting {@code STARTED} then {@code
 * SUCCESS}/{@code FAILURE}). Failures are rethrown so {@code freshworks-boot-kafka}'s consumer error
 * handler redelivers the message with exponential backoff instead of this class managing its own
 * retry.
 *
 * <p>Events whose {@code product} doesn't match {@code conductor.product} (this Conductor instance
 * may share the FreshID event stream with other products) are rejected with {@code NOT_FOUND}
 * rather than purged.
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
    public void handle(DataDeletionRequestedEvent event, String traceId) {
        LOGGER.info(
                "Received ACCOUNT_DELETION_REQUESTED deletion_request_id={} account_id={} "
                        + "product_account_id={} product={} traceId={}",
                event.getDeletionRequestId(),
                event.getAccountId(),
                event.getProductAccountId(),
                event.getProduct(),
                traceId);

        if (!product.equals(event.getProduct())) {
            String message =
                    "No matching product on this instance: received product="
                            + event.getProduct()
                            + ", expected product="
                            + product;
            LOGGER.info(
                    "Rejected ACCOUNT_DELETION_REQUESTED for product={} (this instance only acts"
                            + " on product={}) deletion_request_id={} traceId={}",
                    event.getProduct(),
                    product,
                    event.getDeletionRequestId(),
                    traceId);
            statusPublisher.publish(DeletionStatus.NOT_FOUND, event, message, traceId);
            return;
        }

        statusPublisher.publish(DeletionStatus.QUEUED, event, null, traceId);
        runPurge(event, traceId);
    }

    private void runPurge(DataDeletionRequestedEvent event, String traceId) {
        try {
            statusPublisher.publish(DeletionStatus.STARTED, event, null, traceId);
            int deleted = purger.purge(event.getProductAccountId(), event.getDeletionRequestId(), traceId);
            statusPublisher.publish(
                    DeletionStatus.SUCCESS,
                    event,
                    "Deleted " + deleted + " workflow(s)",
                    traceId);
            Monitors.recordCounter("account_deletion_completed", 1, "result", "success");
        } catch (RuntimeException e) {
            LOGGER.error(
                    "Account deletion FAILED deletion_request_id={} product_account_id={} traceId={}",
                    event.getDeletionRequestId(),
                    event.getProductAccountId(),
                    traceId,
                    e);
            statusPublisher.publish(DeletionStatus.FAILURE, event, e.getMessage(), traceId);
            Monitors.recordCounter("account_deletion_completed", 1, "result", "failure");
            throw e;
        }
    }
}
