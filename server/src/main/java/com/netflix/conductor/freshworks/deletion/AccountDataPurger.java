package com.netflix.conductor.freshworks.deletion;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.dal.ExecutionDAOFacade;

/**
 * Hard-deletes all conductor workflow/task execution data for an account by reusing conductor's
 * existing primitives: enumerate workflows for the account's shard ({@code correlationId ==
 * product_account_id}) and delete each. A failure propagates to the caller so the inbound Kafka
 * message is redelivered by {@code freshworks-boot-kafka}'s consumer error handler, which retries
 * with backoff and re-enumerates — that redelivery is what converges the purge, rather than an
 * in-process retry/pass loop. The operation is idempotent — re-running on an already-purged account
 * finds nothing and succeeds.
 */
@Component
@ConditionalOnProperty(name = "conductor.data-deletion.enabled", havingValue = "true")
public class AccountDataPurger {

    private static final Logger LOGGER = LoggerFactory.getLogger(AccountDataPurger.class);

    private final ExecutionDAOFacade executionDAOFacade;

    public AccountDataPurger(ExecutionDAOFacade executionDAOFacade) {
        this.executionDAOFacade = executionDAOFacade;
    }

    /** @return the total number of workflows deleted */
    public int purge(String productAccountId, String deletionRequestId, String traceId) {
        List<Workflow> workflows =
                executionDAOFacade.getWorkflowsByCorrelationId(null, productAccountId, false);
        if (workflows == null || workflows.isEmpty()) {
            LOGGER.info(
                    "Account purge complete deletion_request_id={} product_account_id={} traceId={} "
                            + "totalDeleted=0",
                    deletionRequestId,
                    productAccountId,
                    traceId);
            return 0;
        }

        LOGGER.info(
                "Account purge deletion_request_id={} product_account_id={} traceId={} workflows={}",
                deletionRequestId,
                productAccountId,
                traceId,
                workflows.size());

        int totalDeleted = 0;
        for (Workflow workflow : workflows) {
            executionDAOFacade.removeWorkflow(workflow.getWorkflowId(), false);
            LOGGER.debug(
                    "Hard-deleted workflow deletion_request_id={} workflowId={} traceId={}",
                    deletionRequestId,
                    workflow.getWorkflowId(),
                    traceId);
            totalDeleted++;
        }
        return totalDeleted;
    }
}
