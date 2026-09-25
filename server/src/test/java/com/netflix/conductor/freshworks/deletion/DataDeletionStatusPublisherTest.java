package com.netflix.conductor.freshworks.deletion;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.SettableListenableFuture;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.freshworks.boot.messaging.KafkaMessageKey;
import com.freshworks.boot.sdk.kafka.model.CentralData;
import com.freshworks.boot.sdk.kafka.model.CentralPayload;
import com.freshworks.boot.sdk.kafka.service.KafkaPublisher;
import com.netflix.conductor.freshworks.deletion.model.DataDeletionRequest;
import com.netflix.conductor.freshworks.deletion.model.DataDeletionStatusPayload;
import com.netflix.conductor.freshworks.deletion.model.DeletionStatus;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
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
    private static final String SERVICE = "conductor";
    private final DataDeletionStatusPublisher publisher =
            new DataDeletionStatusPublisher(kafkaPublisher, SERVICE);
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void buildsEnvelopeAndPayloadFromRequest() {
        when(kafkaPublisher.publish(any())).thenReturn(completedSendResult());

        publisher.publish(DeletionStatus.SUCCESS, request(), "done", "trace-1");

        @SuppressWarnings("unchecked")
        ArgumentCaptor<CentralPayload<DataDeletionStatusPayload>> captor =
                ArgumentCaptor.forClass(CentralPayload.class);
        verify(kafkaPublisher).publish(captor.capture());

        CentralData<DataDeletionStatusPayload> data = captor.getValue().getData();
        assertEquals("ACCOUNT_DELETION_STATUS", data.getPayloadType());
        assertEquals("2.0", data.getPayloadVersion());
        // envelope account_id is the FreshID account_id - ProducerHelper.getKafkaMessageKey
        // does Long.parseLong on it, so it must be present and numeric
        assertEquals("1022003773479925014", data.getAccountId());
        assertDoesNotThrow(() -> Long.parseLong(data.getAccountId()));
        // region/service echoed from the inbound request, so DefaultKafkaPublisher never falls
        // back to freshworks.boot.kafka.producer.*; pod is not part of this event
        assertEquals("us-east-1", data.getRegion());
        assertEquals("freshidv2", data.getService());
        assertNull(data.getPod());

        DataDeletionStatusPayload payload = data.getPayload();
        assertEquals("ACCOUNT_DELETION_STATUS", payload.getEventType());
        assertEquals("req-1", payload.getDeletionRequestId());
        assertEquals(SERVICE, payload.getServiceName());
        assertNotNull(payload.getActionTimestamp());
        assertEquals("1022003773479925014", payload.getAccountId());
        assertEquals("5001", payload.getProductAccountId());
        assertEquals("SUCCESS", payload.getStatus());
        assertEquals("done", payload.getMessage());
    }

    /**
     * Reproduces a production failure: an ACCOUNT_DELETION_REQUESTED with no product_account_id is
     * reported back as NOT_FOUND, but the envelope was keyed on that same missing value, so
     * Long.parseLong(null) threw inside publish and the status never reached Central.
     */
    @Test
    void publishesStatusWhenProductAccountIdIsMissing() {
        when(kafkaPublisher.publish(any())).thenReturn(completedSendResult());

        String noProductAccountId =
                """
                {"data": {
                    "service": "freshidv2",
                    "region": "us-east-1",
                    "payload": {
                        "deletion_request_id": "req-1",
                        "account_id": "1022003773479925014",
                        "product": "freshservice"}}}
                """;
        DataDeletionRequest request = readRequest(noProductAccountId);

        publisher.publish(DeletionStatus.NOT_FOUND, request, "Missing product_account_id", "trace-1");

        @SuppressWarnings("unchecked")
        ArgumentCaptor<CentralPayload<DataDeletionStatusPayload>> captor =
                ArgumentCaptor.forClass(CentralPayload.class);
        verify(kafkaPublisher).publish(captor.capture());

        CentralData<DataDeletionStatusPayload> data = captor.getValue().getData();
        assertEquals("1022003773479925014", data.getAccountId());
        assertDoesNotThrow(() -> Long.parseLong(data.getAccountId()));
        assertNull(data.getPayload().getProductAccountId());
    }

    @Test
    void serializesPayloadAsSnakeCaseAndDropsNullMessage() throws Exception {
        DataDeletionStatusPayload payload = new DataDeletionStatusPayload();
        payload.setDeletionRequestId("req-1");
        payload.setServiceName("fs-caas");
        payload.setStatus(DeletionStatus.STARTED.name());
        payload.setActionTimestamp(1705312200000L);
        payload.setMessage(null);

        String json = objectMapper.writeValueAsString(payload);

        assertTrue(json.contains("\"event_type\":\"ACCOUNT_DELETION_STATUS\""));
        assertTrue(json.contains("\"deletion_request_id\":\"req-1\""));
        // Central's contract names these service_name and action_timestamp, and the timestamp is
        // epoch millis as a number - not a quoted ISO-8601 string
        assertTrue(json.contains("\"service_name\":\"fs-caas\""));
        assertTrue(json.contains("\"action_timestamp\":1705312200000"));
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

    /** Built by deserialization, the way the listener receives it. */
    private static DataDeletionRequest request() {
        String json =
                """
                {"data": {
                    "service": "freshidv2",
                    "region": "us-east-1",
                    "payload": {
                        "deletion_request_id": "req-1",
                        "organisation_id": "org-1",
                        "bundle_id": "bundle-1",
                        "account_id": "1022003773479925014",
                        "product": "freshservice",
                        "product_id": "prod-1",
                        "product_account_id": "5001"}}}
                """;
        return readRequest(json);
    }

    private static DataDeletionRequest readRequest(String json) {
        try {
            return new ObjectMapper().readValue(json, DataDeletionRequest.class);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

}
