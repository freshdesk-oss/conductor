package com.netflix.conductor.freshworks.deletion;

import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.netflix.conductor.common.metadata.workflow.WorkflowDefSummary;
import com.netflix.conductor.core.exception.TransientException;
import com.netflix.conductor.dao.MetadataDAO;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AccountDataPurgerTest {

    private final MetadataDAO metadataDAO = mock(MetadataDAO.class);
    private final AccountDataPurger purger = new AccountDataPurger(metadataDAO);

    @Test
    void unregistersEveryWorkflowDefinitionIndexedForTheAccount() {
        when(metadataDAO.getWorkflowDefsByAccount("5001"))
                .thenReturn(List.of(workflowDef("flow_a", 3), workflowDef("flow_b", 1)));

        assertTrue(purger.purge("5001", "req-1", "trace-1"));

        verify(metadataDAO).removeWorkflowDef("flow_a", 3);
        verify(metadataDAO).removeWorkflowDef("flow_b", 1);
    }

    @Test
    void deletesEveryVersionOfTheSameDefinition() {
        when(metadataDAO.getWorkflowDefsByAccount("5001"))
                .thenReturn(List.of(workflowDef("flow_a", 1), workflowDef("flow_a", 2)));

        assertTrue(purger.purge("5001", "req-1", "trace-1"));

        verify(metadataDAO).removeWorkflowDef("flow_a", 1);
        verify(metadataDAO).removeWorkflowDef("flow_a", 2);
    }

    @Test
    void returnsFalseWhenAccountHasNoDefinitions() {
        when(metadataDAO.getWorkflowDefsByAccount("5001")).thenReturn(Collections.emptyList());

        assertFalse(purger.purge("5001", "req-1", "trace-1"));
        verify(metadataDAO, times(0)).removeWorkflowDef(anyString(), anyInt());
    }

    @Test
    void propagatesFailureSoKafkaRedeliveryCanRetry() {
        when(metadataDAO.getWorkflowDefsByAccount("5001"))
                .thenReturn(List.of(workflowDef("flow_a", 1)));
        doThrow(new TransientException("scylla timeout"))
                .when(metadataDAO)
                .removeWorkflowDef("flow_a", 1);

        assertThrows(TransientException.class, () -> purger.purge("5001", "req-1", "trace-1"));
    }

    @Test
    void propagatesEnumerationFailure() {
        when(metadataDAO.getWorkflowDefsByAccount("5001"))
                .thenThrow(new TransientException("scylla timeout"));

        assertThrows(TransientException.class, () -> purger.purge("5001", "req-1", "trace-1"));
    }

    private static WorkflowDefSummary workflowDef(String name, int version) {
        WorkflowDefSummary summary = new WorkflowDefSummary();
        summary.setName(name);
        summary.setVersion(version);
        return summary;
    }
}
