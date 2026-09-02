package com.netflix.conductor.freshworks.deletion;

import org.junit.jupiter.api.Test;

import com.netflix.conductor.freshworks.deletion.model.AccountDeletionRequestedEvent;
import com.netflix.conductor.freshworks.deletion.model.CentralDeletionEnvelope;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class AccountDeletionEventListenerTest {

    private final AccountDeletionService service = mock(AccountDeletionService.class);
    private final AccountDeletionEventListener listener = new AccountDeletionEventListener(service);

    @Test
    void validEnvelopeDelegatesToService() {
        listener.onAccountDeletionRequested(envelope("req-1", "5001"));

        verify(service)
                .handle(
                        argThat(
                                e ->
                                        "req-1".equals(e.getDeletionRequestId())
                                                && "5001".equals(e.getProductAccountId())),
                        any());
    }

    @Test
    void missingDeletionRequestIdIsIgnored() {
        listener.onAccountDeletionRequested(envelope(null, "5001"));

        verifyNoInteractions(service);
    }

    @Test
    void missingProductAccountIdIsIgnored() {
        listener.onAccountDeletionRequested(envelope("req-1", ""));

        verifyNoInteractions(service);
    }

    @Test
    void nullEnvelopeIsIgnored() {
        listener.onAccountDeletionRequested(null);

        verifyNoInteractions(service);
    }

    @Test
    void nullPayloadIsIgnored() {
        listener.onAccountDeletionRequested(new CentralDeletionEnvelope());

        verifyNoInteractions(service);
    }

    private static CentralDeletionEnvelope envelope(String deletionRequestId, String productAccountId) {
        AccountDeletionRequestedEvent event = new AccountDeletionRequestedEvent();
        event.setDeletionRequestId(deletionRequestId);
        event.setProductAccountId(productAccountId);

        CentralDeletionEnvelope.EnvelopeData data = new CentralDeletionEnvelope.EnvelopeData();
        data.setPayload(event);

        CentralDeletionEnvelope envelope = new CentralDeletionEnvelope();
        envelope.setData(data);
        return envelope;
    }
}
