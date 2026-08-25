package com.netflix.conductor.freshworks.deletion;

import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.retry.support.RetryTemplate;

import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.dal.ExecutionDAOFacade;
import com.netflix.conductor.freshworks.deletion.config.AccountDeletionProperties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AccountDataPurgerTest {

    private final ExecutionDAOFacade facade = mock(ExecutionDAOFacade.class);
    private final AccountDeletionProperties properties = new AccountDeletionProperties();
    private final AccountDataPurger purger =
            new AccountDataPurger(facade, RetryTemplate.builder().maxAttempts(1).build(), properties);

    @Test
    void enumeratesAndHardDeletesEveryWorkflowUntilEmpty() {
        when(facade.getWorkflowsByCorrelationId(isNull(), eq("5001"), eq(false)))
                .thenReturn(List.of(workflow("w1"), workflow("w2")))
                .thenReturn(Collections.emptyList());

        int deleted = purger.purge("5001", "req-1", "trace-1");

        assertEquals(2, deleted);
        verify(facade).removeWorkflow("w1", false);
        verify(facade).removeWorkflow("w2", false);
    }

    @Test
    void returnsZeroWhenAccountHasNoData() {
        when(facade.getWorkflowsByCorrelationId(isNull(), eq("5001"), eq(false)))
                .thenReturn(Collections.emptyList());

        assertEquals(0, purger.purge("5001", "req-1", "trace-1"));
        verify(facade, times(0)).removeWorkflow(org.mockito.ArgumentMatchers.anyString(), eq(false));
    }

    @Test
    void throwsWhenDataRemainsAfterPassBudget() {
        properties.setMaxPurgePasses(2);
        // Always returns one workflow → never converges.
        when(facade.getWorkflowsByCorrelationId(isNull(), eq("5001"), eq(false)))
                .thenReturn(List.of(workflow("w1")));

        assertThrows(
                IllegalStateException.class, () -> purger.purge("5001", "req-1", "trace-1"));
    }

    private static Workflow workflow(String id) {
        Workflow workflow = new Workflow();
        workflow.setWorkflowId(id);
        return workflow;
    }
}
