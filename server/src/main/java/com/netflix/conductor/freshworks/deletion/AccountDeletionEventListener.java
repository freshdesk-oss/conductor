package com.netflix.conductor.freshworks.deletion;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import com.freshworks.boot.kafka.CentralListener;
import com.netflix.conductor.freshworks.deletion.model.AccountDeletionRequestedEvent;
import com.netflix.conductor.freshworks.deletion.model.CentralDeletionEnvelope;

import io.opentelemetry.api.trace.Span;

/**
 * Kafka ingress for the FreshID {@code ACCOUNT_DELETION_REQUESTED} event, replacing the old WHaaS
 * REST webhook. Central publishes the event on the shared {@code freshidv2-external-events} topic;
 * {@link CentralListener}'s {@code messageSelectors} routes only this event type to this method.
 */
@Service
@ConditionalOnProperty(name = "conductor.account-deletion.enabled", havingValue = "true")
public class AccountDeletionEventListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(AccountDeletionEventListener.class);

    private final AccountDeletionService service;

    public AccountDeletionEventListener(AccountDeletionService service) {
        this.service = service;
    }

    @CentralListener(
            messageSelectors = "freshidv2:ACCOUNT_DELETION_REQUESTED:*",
            messageFilterEnabled = false)
    public void onAccountDeletionRequested(CentralDeletionEnvelope envelope) {
        AccountDeletionRequestedEvent event = envelope != null ? envelope.getPayload() : null;
        String traceId = Span.current().getSpanContext().getTraceId();

        if (event == null
                || StringUtils.isBlank(event.getDeletionRequestId())
                || StringUtils.isBlank(event.getProductAccountId())) {
            LOGGER.warn(
                    "Rejected ACCOUNT_DELETION_REQUESTED with missing deletion_request_id/"
                            + "product_account_id traceId={}",
                    traceId);
            return;
        }

        service.handle(event, traceId);
    }
}
