package com.netflix.conductor.freshworks.deletion;

import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.netflix.conductor.common.metadata.workflow.WorkflowDefSummary;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.dal.ExecutionDAOFacade;
import com.netflix.conductor.core.exception.TransientException;
import com.netflix.conductor.dao.MetadataDAO;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AccountDataPurgerTest {

    private final ExecutionDAOFacade facade = mock(ExecutionDAOFacade.class);
    private final MetadataDAO metadataDAO = mock(MetadataDAO.class);
    private final AccountDataPurger purger = new AccountDataPurger(facade, metadataDAO);

    @Test
    void enumeratesAndHardDeletesEveryWorkflow() {
        when(facade.getWorkflowsByCorrelationId(isNull(), eq("5001"), eq(false)))
                .thenReturn(List.of(workflow("w1"), workflow("w2")));
        when(metadataDAO.getWorkflowDefsByAccount("5001")).thenReturn(Collections.emptyList());

        boolean purged = purger.purge("5001", "req-1", "trace-1");

        assertTrue(purged);
        verify(facade).removeWorkflow("w1", false);
        verify(facade).removeWorkflow("w2", false);
    }

    @Test
    void unregistersEveryWorkflowDefinitionIndexedForTheAccount() {
        when(facade.getWorkflowsByCorrelationId(isNull(), eq("5001"), eq(false)))
                .thenReturn(List.of(workflow("w1")));
        when(metadataDAO.getWorkflowDefsByAccount("5001"))
                .thenReturn(List.of(workflowDef("flow_a", 3), workflowDef("flow_b", 1)));

        assertTrue(purger.purge("5001", "req-1", "trace-1"));

        verify(metadataDAO).removeWorkflowDef("flow_a", 3);
        verify(metadataDAO).removeWorkflowDef("flow_b", 1);
    }

    /** Guards the ordering trap: an account can own definitions but have no executions left. */
    @Test
    void purgesDefinitionsEvenWhenTheAccountHasNoWorkflows() {
        when(facade.getWorkflowsByCorrelationId(isNull(), eq("5001"), eq(false)))
                .thenReturn(Collections.emptyList());
        when(metadataDAO.getWorkflowDefsByAccount("5001"))
                .thenReturn(List.of(workflowDef("flow_a", 2)));

        assertTrue(purger.purge("5001", "req-1", "trace-1"));

        verify(metadataDAO).removeWorkflowDef("flow_a", 2);
        verify(facade, times(0)).removeWorkflow(anyString(), eq(false));
    }

    @Test
    void returnsFalseWhenAccountHasNoData() {
        when(facade.getWorkflowsByCorrelationId(isNull(), eq("5001"), eq(false)))
                .thenReturn(Collections.emptyList());
        when(metadataDAO.getWorkflowDefsByAccount("5001")).thenReturn(Collections.emptyList());

        assertFalse(purger.purge("5001", "req-1", "trace-1"));
        verify(facade, times(0)).removeWorkflow(anyString(), eq(false));
        verify(metadataDAO, times(0)).removeWorkflowDef(anyString(), anyInt());
    }

    @Test
    void propagatesFailureSoKafkaRedeliveryCanRetry() {
        when(facade.getWorkflowsByCorrelationId(isNull(), eq("5001"), eq(false)))
                .thenReturn(List.of(workflow("w1")));
        when(metadataDAO.getWorkflowDefsByAccount("5001")).thenReturn(Collections.emptyList());
        doThrow(new TransientException("scylla timeout"))
                .when(facade)
                .removeWorkflow("w1", false);

        assertThrows(
                TransientException.class, () -> purger.purge("5001", "req-1", "trace-1"));
    }

    @Test
    void propagatesDefinitionDeleteFailure() {
        when(facade.getWorkflowsByCorrelationId(isNull(), eq("5001"), eq(false)))
                .thenReturn(Collections.emptyList());
        when(metadataDAO.getWorkflowDefsByAccount("5001"))
                .thenReturn(List.of(workflowDef("flow_a", 1)));
        doThrow(new TransientException("scylla timeout"))
                .when(metadataDAO)
                .removeWorkflowDef("flow_a", 1);

        assertThrows(
                TransientException.class, () -> purger.purge("5001", "req-1", "trace-1"));
    }

    private static Workflow workflow(String id) {
        Workflow workflow = new Workflow();
        workflow.setWorkflowId(id);
        return workflow;
    }

    private static WorkflowDefSummary workflowDef(String name, int version) {
        WorkflowDefSummary summary = new WorkflowDefSummary();
        summary.setName(name);
        summary.setVersion(version);
        return summary;
    }
}
