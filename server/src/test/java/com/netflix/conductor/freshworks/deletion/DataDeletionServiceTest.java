package com.netflix.conductor.freshworks.deletion;

import org.junit.jupiter.api.Test;

import com.netflix.conductor.freshworks.deletion.model.DataDeletionRequestedEvent;
import com.netflix.conductor.freshworks.deletion.model.DeletionStatus;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class DataDeletionServiceTest {

    private final DataDeletionStatusPublisher statusPublisher = mock(DataDeletionStatusPublisher.class);
    private final AccountDataPurger purger = mock(AccountDataPurger.class);
    private final DataDeletionService service =
            new DataDeletionService(statusPublisher, purger, "freshservice");

    @Test
    void matchingProductQueuesAndPurges() {
        when(purger.purge(anyString(), anyString(), anyString())).thenReturn(3);

        service.handle(event("freshservice"), "trace-1");

        verify(statusPublisher).publish(eq(DeletionStatus.QUEUED), any(), eq(null), eq("trace-1"));
        verify(statusPublisher).publish(eq(DeletionStatus.STARTED), any(), eq(null), eq("trace-1"));
        verify(statusPublisher)
                .publish(eq(DeletionStatus.SUCCESS), any(), anyString(), eq("trace-1"));
    }

    @Test
    void mismatchedProductPublishesNotFoundAndSkipsPurge() {
        service.handle(event("freshdesk"), "trace-1");

        verify(statusPublisher)
                .publish(eq(DeletionStatus.NOT_FOUND), any(), anyString(), eq("trace-1"));
        verify(statusPublisher, never()).publish(eq(DeletionStatus.QUEUED), any(), any(), any());
        verifyNoInteractions(purger);
    }

    private static DataDeletionRequestedEvent event(String product) {
        DataDeletionRequestedEvent event = new DataDeletionRequestedEvent();
        event.setDeletionRequestId("req-1");
        event.setProductAccountId("5001");
        event.setProduct(product);
        return event;
    }
}
