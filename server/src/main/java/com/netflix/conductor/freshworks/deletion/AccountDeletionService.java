package com.netflix.conductor.freshworks.deletion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import com.netflix.conductor.freshworks.deletion.model.AccountDeletionRequestedEvent;
import com.netflix.conductor.freshworks.deletion.model.DeletionStatus;
import com.netflix.conductor.metrics.Monitors;

/**
 * Orchestrates an account deletion request: acknowledges with {@code QUEUED}, then runs the hard
 * delete synchronously on the Kafka listener thread (emitting {@code STARTED} then {@code
 * SUCCESS}/{@code FAILURE}). Failures are rethrown so {@code freshworks-boot-kafka}'s consumer error
 * handler redelivers the message with exponential backoff instead of this class managing its own
 * retry.
 */
@Component
@ConditionalOnProperty(name = "conductor.account-deletion.enabled", havingValue = "true")
public class AccountDeletionService {

    private static final Logger LOGGER = LoggerFactory.getLogger(AccountDeletionService.class);

    private final AccountDeletionStatusPublisher statusPublisher;
    private final AccountDataPurger purger;

    public AccountDeletionService(AccountDeletionStatusPublisher statusPublisher, AccountDataPurger purger) {
        this.statusPublisher = statusPublisher;
        this.purger = purger;
    }

    /** Acknowledges the request and runs the purge. */
    public void handle(AccountDeletionRequestedEvent event, String traceId) {
        LOGGER.info(
                "Received ACCOUNT_DELETION_REQUESTED deletion_request_id={} account_id={} "
                        + "product_account_id={} product={} traceId={}",
                event.getDeletionRequestId(),
                event.getAccountId(),
                event.getProductAccountId(),
                event.getProduct(),
                traceId);

        statusPublisher.publish(DeletionStatus.QUEUED, event, null, traceId);
        runPurge(event, traceId);
    }

    private void runPurge(AccountDeletionRequestedEvent event, String traceId) {
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
