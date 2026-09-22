package com.netflix.conductor.freshworks.deletion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.freshworks.boot.kafka.CentralListener;
import com.netflix.conductor.freshworks.deletion.model.DataDeletionRequest;

import io.opentelemetry.api.trace.Span;

/**
 * Kafka ingress for the FreshID {@code ACCOUNT_DELETION_REQUESTED} event.
 * Payload lacking {@code product_account_id} is still handed to {@link
 * DataDeletionService}, which reports it back as {@code NOT_FOUND}.
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
    public void onDataDeletionRequested(DataDeletionRequest request) {
        String traceId = Span.current().getSpanContext().getTraceId();

        if (request == null || request.getPayload() == null) {
            LOGGER.warn("Rejected ACCOUNT_DELETION_REQUESTED with missing payload traceId={}", traceId);
            return;
        }

        service.handle(request, traceId);
    }
}
