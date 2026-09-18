package com.netflix.conductor.freshworks.deletion;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.netflix.conductor.common.metadata.workflow.WorkflowDefSummary;
import com.netflix.conductor.dao.MetadataDAO;

/**
 * Hard-deletes an account's workflow definitions: enumerate them through the {@code
 * workflow_defs_by_account} index and unregister each, which clears {@code workflow_definitions},
 * {@code workflow_defs_index} and the index row itself.
 *
 * <p>Execution data is deliberately not purged here. The {@code workflows} table carries a TTL in
 * production, so workflow and task rows age out on their own; deleting them on this event would
 * duplicate that.
 *
 * <p>A failure propagates to {@link DataDeletionService}, which reports {@code FAILURE} to Central
 * and does not rethrow — so the Kafka offset commits and the message is not redelivered.
 * Recovery is therefore whoever re-triggers the deletion request, not an automatic retry. The
 * operation is idempotent, so re-running on a partly-purged account completes the rest.
 *
 * <p>Definitions registered before the product account id was threaded through registration carry
 * no index row and so are not found here; those remain the responsibility of the calling service's
 * own delete call.
 */
@Component
public class AccountDataPurger {

    private static final Logger LOGGER = LoggerFactory.getLogger(AccountDataPurger.class);

    private final MetadataDAO metadataDAO;

    public AccountDataPurger(MetadataDAO metadataDAO) {
        this.metadataDAO = metadataDAO;
    }

    /** @return {@code true} if the account had definitions and they were deleted, {@code false} if none were found */
    public boolean purge(String productAccountId, String deletionRequestId, String traceId) {
        List<WorkflowDefSummary> workflowDefs = metadataDAO.getWorkflowDefsByAccount(productAccountId);

        LOGGER.info(
                "Data purge deletion_request_id={} product_account_id={} traceId={} workflow_defs={}",
                deletionRequestId,
                productAccountId,
                traceId,
                workflowDefs.size());

        for (WorkflowDefSummary workflowDef : workflowDefs) {
            metadataDAO.removeWorkflowDef(workflowDef.getName(), workflowDef.getVersion());
            LOGGER.debug(
                    "Hard-deleted workflow definition deletion_request_id={} name={} version={} "
                            + "traceId={}",
                    deletionRequestId,
                    workflowDef.getName(),
                    workflowDef.getVersion(),
                    traceId);
        }

        return !workflowDefs.isEmpty();
    }
}
