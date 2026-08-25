package com.netflix.conductor.freshworks.deletion;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.SettableListenableFuture;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.freshworks.deletion.config.AccountDeletionProperties;
import com.netflix.conductor.freshworks.deletion.model.AccountDeletionRequestedEvent;
import com.netflix.conductor.freshworks.deletion.model.AccountDeletionStatusPayload;
import com.netflix.conductor.freshworks.deletion.model.CentralData;
import com.netflix.conductor.freshworks.deletion.model.CentralPayload;
import com.netflix.conductor.freshworks.deletion.model.DeletionStatus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AccountDeletionStatusPublisherTest {

    private final CentralKafkaPublisher kafkaPublisher = mock(CentralKafkaPublisher.class);
    private final AccountDeletionProperties properties = new AccountDeletionProperties();
    private final AccountDeletionStatusPublisher publisher =
            new AccountDeletionStatusPublisher(kafkaPublisher, properties);
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void buildsEnvelopeAndPayloadFromEvent() {
        properties.setService("conductor");
        properties.setRegion("us-east-1");
        properties.setPod("pod-1");

        when(kafkaPublisher.publish(eq("5001"), any())).thenReturn(completedSendResult());

        publisher.publish(DeletionStatus.SUCCESS, event(), "done", "trace-1");

        @SuppressWarnings("unchecked")
        ArgumentCaptor<CentralPayload<AccountDeletionStatusPayload>> captor =
                ArgumentCaptor.forClass(CentralPayload.class);
        verify(kafkaPublisher).publish(eq("5001"), captor.capture());

        CentralData<AccountDeletionStatusPayload> data = captor.getValue().getData();
        assertEquals("ACCOUNT_DELETION_STATUS", data.getPayloadType());
        assertEquals("2.0", data.getPayloadVersion());
        assertEquals("5001", data.getAccountId()); // envelope account_id = product_account_id
        assertEquals("us-east-1", data.getRegion());
        assertEquals("pod-1", data.getPod());

        AccountDeletionStatusPayload payload = data.getPayload();
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
        AccountDeletionStatusPayload payload = new AccountDeletionStatusPayload();
        payload.setDeletionRequestId("req-1");
        payload.setStatus("QUEUED");
        payload.setMessage(null);

        String json = objectMapper.writeValueAsString(payload);

        assertTrue(json.contains("\"event_type\":\"ACCOUNT_DELETION_STATUS\""));
        assertTrue(json.contains("\"deletion_request_id\":\"req-1\""));
        assertFalse(json.contains("message"));
    }

    private static ListenableFuture<SendResult<String, CentralPayload<AccountDeletionStatusPayload>>>
            completedSendResult() {
        RecordMetadata metadata =
                new RecordMetadata(new TopicPartition("account-deletion-notifications", 0), 0, 0, 0, 0, 0);
        SettableListenableFuture<SendResult<String, CentralPayload<AccountDeletionStatusPayload>>> future =
                new SettableListenableFuture<>();
        future.set(new SendResult<>(null, metadata));
        return future;
    }

    private static AccountDeletionRequestedEvent event() {
        AccountDeletionRequestedEvent event = new AccountDeletionRequestedEvent();
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
