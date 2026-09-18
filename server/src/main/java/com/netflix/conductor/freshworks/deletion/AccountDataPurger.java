package com.netflix.conductor.freshworks.deletion;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.netflix.conductor.common.metadata.workflow.WorkflowDefSummary;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.dal.ExecutionDAOFacade;
import com.netflix.conductor.dao.MetadataDAO;

/**
 * Hard-deletes all conductor data for an account by reusing conductor's existing primitives:
 * enumerate workflows for the account's shard ({@code correlationId == product_account_id}) and
 * delete each, then enumerate the account's workflow definitions via the {@code
 * workflow_defs_by_account} index and unregister each. A failure propagates to the caller so the
 * inbound Kafka message is redelivered by {@code freshworks-boot-kafka}'s consumer error handler,
 * which retries with backoff and re-enumerates — that redelivery is what converges the purge,
 * rather than an in-process retry/pass loop. The operation is idempotent — re-running on an
 * already-purged account finds nothing and succeeds.
 *
 * <p>Definitions registered before the product account id was threaded through registration carry
 * no index row and are therefore not found here; those remain the responsibility of the caller
 * service's own delete call.
 */
@Component
public class AccountDataPurger {

    private static final Logger LOGGER = LoggerFactory.getLogger(AccountDataPurger.class);

    private final ExecutionDAOFacade executionDAOFacade;
    private final MetadataDAO metadataDAO;

    public AccountDataPurger(ExecutionDAOFacade executionDAOFacade, MetadataDAO metadataDAO) {
        this.executionDAOFacade = executionDAOFacade;
        this.metadataDAO = metadataDAO;
    }

    /** @return {@code true} if the account had data and it was deleted, {@code false} if none was found */
    public boolean purge(String productAccountId, String deletionRequestId, String traceId) {
        List<Workflow> workflows =
                executionDAOFacade.getWorkflowsByCorrelationId(null, productAccountId, false);
        List<WorkflowDefSummary> workflowDefs = metadataDAO.getWorkflowDefsByAccount(productAccountId);

        LOGGER.info(
                "Data purge deletion_request_id={} product_account_id={} traceId={} workflows={} "
                        + "workflow_defs={}",
                deletionRequestId,
                productAccountId,
                traceId,
                workflows.size(),
                workflowDefs.size());

        for (Workflow workflow : workflows) {
            executionDAOFacade.removeWorkflow(workflow.getWorkflowId(), false);
            LOGGER.debug(
                    "Hard-deleted workflow deletion_request_id={} workflowId={} traceId={}",
                    deletionRequestId,
                    workflow.getWorkflowId(),
                    traceId);
        }

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

        return !workflows.isEmpty() || !workflowDefs.isEmpty();
    }
}
