package com.netflix.conductor.freshworks.deletion;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.dal.ExecutionDAOFacade;
import com.netflix.conductor.freshworks.deletion.config.AccountDeletionProperties;

/**
 * Hard-deletes all conductor workflow/task execution data for an account by reusing conductor's
 * existing primitives: enumerate workflows for the account's shard ({@code correlationId ==
 * product_account_id}) and delete each. Repeated until enumeration returns empty, so replays and
 * concurrent writes converge. The operation is idempotent — re-running on an already-purged account
 * finds nothing and succeeds.
 */
@Component
@ConditionalOnProperty(name = "conductor.account-deletion.enabled", havingValue = "true")
public class AccountDataPurger {

    private static final Logger LOGGER = LoggerFactory.getLogger(AccountDataPurger.class);

    private final ExecutionDAOFacade executionDAOFacade;
    private final RetryTemplate retryTemplate;
    private final AccountDeletionProperties properties;

    public AccountDataPurger(
            ExecutionDAOFacade executionDAOFacade,
            RetryTemplate accountDeletionRetryTemplate,
            AccountDeletionProperties properties) {
        this.executionDAOFacade = executionDAOFacade;
        this.retryTemplate = accountDeletionRetryTemplate;
        this.properties = properties;
    }

    /**
     * @return the total number of workflows deleted
     * @throws IllegalStateException if data still remains after the configured pass budget
     */
    public int purge(String productAccountId, String deletionRequestId, String traceId) {
        int totalDeleted = 0;
        int maxPasses = properties.getMaxPurgePasses();

        for (int pass = 1; pass <= maxPasses; pass++) {
            List<Workflow> workflows =
                    executionDAOFacade.getWorkflowsByCorrelationId(null, productAccountId, false);
            if (workflows == null || workflows.isEmpty()) {
                LOGGER.info(
                        "Account purge complete deletion_request_id={} product_account_id={} "
                                + "traceId={} pass={} totalDeleted={}",
                        deletionRequestId,
                        productAccountId,
                        traceId,
                        pass,
                        totalDeleted);
                return totalDeleted;
            }

            LOGGER.info(
                    "Account purge pass deletion_request_id={} product_account_id={} traceId={} "
                            + "pass={} workflowsInPass={}",
                    deletionRequestId,
                    productAccountId,
                    traceId,
                    pass,
                    workflows.size());

            for (Workflow workflow : workflows) {
                deleteWorkflow(workflow.getWorkflowId(), deletionRequestId, traceId);
                totalDeleted++;
            }
        }

        List<Workflow> remaining =
                executionDAOFacade.getWorkflowsByCorrelationId(null, productAccountId, false);
        if (remaining != null && !remaining.isEmpty()) {
            throw new IllegalStateException(
                    "Account data still present after "
                            + maxPasses
                            + " purge passes for product_account_id="
                            + productAccountId
                            + " (remaining="
                            + remaining.size()
                            + ")");
        }
        return totalDeleted;
    }

    private void deleteWorkflow(String workflowId, String deletionRequestId, String traceId) {
        retryTemplate.execute(
                ctx -> {
                    executionDAOFacade.removeWorkflow(workflowId, false);
                    LOGGER.debug(
                            "Hard-deleted workflow deletion_request_id={} workflowId={} traceId={}",
                            deletionRequestId,
                            workflowId,
                            traceId);
                    return null;
                });
    }
}
