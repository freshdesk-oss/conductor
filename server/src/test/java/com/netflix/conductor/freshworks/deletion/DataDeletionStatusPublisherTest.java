package com.netflix.conductor.freshworks.deletion;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.SettableListenableFuture;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.freshworks.boot.messaging.KafkaMessageKey;
import com.freshworks.boot.sdk.kafka.model.CentralData;
import com.freshworks.boot.sdk.kafka.model.CentralPayload;
import com.freshworks.boot.sdk.kafka.service.KafkaPublisher;
import com.netflix.conductor.freshworks.deletion.config.DataDeletionProperties;
import com.netflix.conductor.freshworks.deletion.model.DataDeletionRequestedEvent;
import com.netflix.conductor.freshworks.deletion.model.DataDeletionStatusPayload;
import com.netflix.conductor.freshworks.deletion.model.DeletionStatus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DataDeletionStatusPublisherTest {

    @SuppressWarnings("unchecked")
    private final KafkaPublisher<KafkaMessageKey, CentralPayload<DataDeletionStatusPayload>>
            kafkaPublisher = mock(KafkaPublisher.class);
    private final DataDeletionProperties properties = new DataDeletionProperties();
    private final DataDeletionStatusPublisher publisher =
            new DataDeletionStatusPublisher(kafkaPublisher, properties);
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void buildsEnvelopeAndPayloadFromEvent() {
        properties.setService("conductor");

        when(kafkaPublisher.publish(any())).thenReturn(completedSendResult());

        publisher.publish(DeletionStatus.SUCCESS, event(), "done", "trace-1");

        @SuppressWarnings("unchecked")
        ArgumentCaptor<CentralPayload<DataDeletionStatusPayload>> captor =
                ArgumentCaptor.forClass(CentralPayload.class);
        verify(kafkaPublisher).publish(captor.capture());

        CentralData<DataDeletionStatusPayload> data = captor.getValue().getData();
        assertEquals("ACCOUNT_DELETION_STATUS", data.getPayloadType());
        assertEquals("2.0", data.getPayloadVersion());
        assertEquals("5001", data.getAccountId()); // envelope account_id = product_account_id

        DataDeletionStatusPayload payload = data.getPayload();
        assertEquals("ACCOUNT_DELETION_STATUS", payload.getEventType());
        assertEquals("req-1", payload.getDeletionRequestId());
        assertEquals("conductor", payload.getService());
        assertEquals("freshid-acc-1", payload.getAccountId());
        assertEquals("5001", payload.getProductAccountId());
        assertEquals("SUCCESS", payload.getStatus());
        assertEquals("done", payload.getMessage());
    }

    @Test
    void serializesPayloadAsSnakeCaseAndDropsNullMessage() throws Exception {
        DataDeletionStatusPayload payload = new DataDeletionStatusPayload();
        payload.setDeletionRequestId("req-1");
        payload.setStatus("QUEUED");
        payload.setMessage(null);

        String json = objectMapper.writeValueAsString(payload);

        assertTrue(json.contains("\"event_type\":\"ACCOUNT_DELETION_STATUS\""));
        assertTrue(json.contains("\"deletion_request_id\":\"req-1\""));
        assertFalse(json.contains("message"));
    }

    private static ListenableFuture<SendResult<KafkaMessageKey, CentralPayload<DataDeletionStatusPayload>>>
            completedSendResult() {
        RecordMetadata metadata =
                new RecordMetadata(new TopicPartition("account-deletion-notifications", 0), 0, 0, 0, 0, 0);
        SettableListenableFuture<SendResult<KafkaMessageKey, CentralPayload<DataDeletionStatusPayload>>> future =
                new SettableListenableFuture<>();
        future.set(new SendResult<>(null, metadata));
        return future;
    }

    private static DataDeletionRequestedEvent event() {
        DataDeletionRequestedEvent event = new DataDeletionRequestedEvent();
        event.setDeletionRequestId("req-1");
        event.setOrganisationId("org-1");
        event.setBundleId("bundle-1");
        event.setAccountId("freshid-acc-1");
        event.setProduct("freshservice");
        event.setProductId("prod-1");
        event.setProductAccountId("5001");
        return event;
    }
}
