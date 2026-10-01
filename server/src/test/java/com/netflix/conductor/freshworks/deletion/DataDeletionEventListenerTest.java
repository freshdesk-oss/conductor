package com.netflix.conductor.freshworks.deletion;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.freshworks.deletion.model.DataDeletionRequest;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class DataDeletionEventListenerTest {

    private final DataDeletionService service = mock(DataDeletionService.class);
    private final DataDeletionEventListener listener = new DataDeletionEventListener(service);

    @Test
    void validRequestDelegatesToService() {
        listener.onDataDeletionRequested(request("req-1", "5001"));

        verify(service)
                .handle(
                        argThat(
                                env ->
                                        "req-1".equals(env.getPayload().getDeletionRequestId())
                                                && "5001"
                                                        .equals(
                                                                env.getPayload()
                                                                        .getProductAccountId())),
                        any());
    }

    @Test
    void missingProductAccountIdStillDelegatesToService() {
        listener.onDataDeletionRequested(request("req-1", ""));

        verify(service)
                .handle(
                        argThat(
                                env ->
                                        "req-1".equals(env.getPayload().getDeletionRequestId())
                                                && ""
                                                        .equals(
                                                                env.getPayload()
                                                                        .getProductAccountId())),
                        any());
    }

    @Test
    void nullRequestIsIgnored() {
        listener.onDataDeletionRequested(null);

        verifyNoInteractions(service);
    }

    @Test
    void nullPayloadIsIgnored() {
        listener.onDataDeletionRequested(requestWithoutData());

        verifyNoInteractions(service);
    }

    /** Built by deserialization, the way the listener receives it. */
    private static DataDeletionRequest request(String deletionRequestId, String productAccountId) {
        String json =
                """
                {"data": {"payload": {
                    "deletion_request_id": "%s",
                    "product_account_id": "%s"}}}
                """
                        .formatted(deletionRequestId, productAccountId);
        return read(json);
    }

    /** A message whose {@code data} node is absent entirely. */
    private static DataDeletionRequest requestWithoutData() {
        return read("{\"meta\": {}}");
    }

    private static DataDeletionRequest read(String json) {
        try {
            return new ObjectMapper().readValue(json, DataDeletionRequest.class);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }
}
