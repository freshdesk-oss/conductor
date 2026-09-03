package com.netflix.conductor.freshworks.deletion;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.freshworks.boot.kafka.CentralListener;
import com.netflix.conductor.freshworks.deletion.model.DataDeletionRequestedEvent;
import com.netflix.conductor.freshworks.deletion.model.CentralDeletionEnvelope;

import io.opentelemetry.api.trace.Span;

/**
 * Kafka ingress for the FreshID {@code ACCOUNT_DELETION_REQUESTED} event, replacing the old WHaaS
 * REST webhook. Central publishes the event on the shared {@code freshidv2-external-events} topic;
 * {@link CentralListener}'s {@code messageSelectors} routes only this event type to this method.
 */
@Service
public class DataDeletionEventListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataDeletionEventListener.class);

    private final DataDeletionService service;

    public DataDeletionEventListener(DataDeletionService service) {
        this.service = service;
    }

    @CentralListener(
            messageSelectors = "${freshid.service.name}:ACCOUNT_DELETION_REQUESTED:*",
            messageFilterEnabled = false)
    public void onDataDeletionRequested(CentralDeletionEnvelope envelope) {
        DataDeletionRequestedEvent event = envelope != null ? envelope.getPayload() : null;
        String traceId = Span.current().getSpanContext().getTraceId();

        if (event == null || StringUtils.isBlank(event.getProductAccountId())) {
            LOGGER.warn(
                    "Rejected ACCOUNT_DELETION_REQUESTED with missing product_account_id traceId={}",
                    traceId);
            return;
        }

        service.handle(event, traceId);
    }
}
