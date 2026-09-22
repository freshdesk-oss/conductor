package com.netflix.conductor.freshworks.deletion;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.freshworks.deletion.model.DataDeletionRequest;
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
    void matchingProductStartsAndPurges() {
        when(purger.purge(anyString(), anyString(), anyString())).thenReturn(true);

        service.handle(request("freshservice"), "trace-1");

        verify(statusPublisher).publish(eq(DeletionStatus.STARTED), any(), eq(null), eq("trace-1"));
        verify(statusPublisher)
                .publish(eq(DeletionStatus.SUCCESS), any(), anyString(), eq("trace-1"));
    }

    @Test
    void noAccountDataPublishesNotFound() {
        when(purger.purge(anyString(), anyString(), anyString())).thenReturn(false);

        service.handle(request("freshservice"), "trace-1");

        verify(statusPublisher).publish(eq(DeletionStatus.STARTED), any(), eq(null), eq("trace-1"));
        verify(statusPublisher)
                .publish(eq(DeletionStatus.NOT_FOUND), any(), anyString(), eq("trace-1"));
    }

    @Test
    void missingProductAccountIdPublishesNotFoundAndSkipsPurge() {
        service.handle(request("freshservice", ""), "trace-1");

        verify(statusPublisher)
                .publish(eq(DeletionStatus.NOT_FOUND), any(), anyString(), eq("trace-1"));
        verifyNoInteractions(purger);
    }

    @Test
    void mismatchedProductPublishesNotFoundAndSkipsPurge() {
        service.handle(request("freshdesk"), "trace-1");

        verify(statusPublisher)
                .publish(eq(DeletionStatus.NOT_FOUND), any(), anyString(), eq("trace-1"));
        verify(statusPublisher, never()).publish(eq(DeletionStatus.STARTED), any(), any(), any());
        verifyNoInteractions(purger);
    }

    private static DataDeletionRequest request(String product) {
        return request(product, "5001");
    }

    /** Built by deserialization, the way the listener receives it. */
    private static DataDeletionRequest request(String product, String productAccountId) {
        String json =
                """
                {"data": {"payload": {
                    "deletion_request_id": "req-1",
                    "product_account_id": "%s",
                    "product": "%s"}}}
                """
                        .formatted(productAccountId, product);
        try {
            return new ObjectMapper().readValue(json, DataDeletionRequest.class);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }
}
