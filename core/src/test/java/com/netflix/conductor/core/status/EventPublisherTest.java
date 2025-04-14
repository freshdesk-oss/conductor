package com.netflix.conductor.core.status;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@ExtendWith(MockitoExtension.class)
class EventPublisherTest {

    @InjectMocks
    private EventPublisher eventPublisher;

    @Mock
    private EventFilterConfig eventFilterConfig;

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(eventPublisher, "isStatusListenerEnabled", true);
        ReflectionTestUtils.setField(eventPublisher, "moduleType", "journey");
    }

    @Test
    void testSendCentralMessageSuccess() throws JsonProcessingException {
        ReflectionTestUtils.setField(eventPublisher, "url", "http://staging-central.freshedge.net/collector");
        ReflectionTestUtils.setField(eventPublisher, "token", "565b0f58441d2f113a7ceaae2ea5744c75de667f72272c55ec5123dfee206a5b5f598b9e9af7935f9e27bbe1d6ee566a");

        ObjectNode payload = objectMapper.createObjectNode();
        payload.put("parent_workflow_id", "");
        payload.put("workflow_id", "workflow-123");
        payload.put("status", "COMPLETED");
        payload.put("reason_for_incompletion", "");

        String response = eventPublisher.sendCentralMessage("100", "journey_conductor_workflow_event", payload);
        JsonNode jsonNode = objectMapper.readTree(response);

        assertEquals("freshservice-central-journey-conductor-event-v2", jsonNode.get("topic").asText());
    }

    @Test
    void testSendCentralMessageFailure() {
        ObjectNode payload = objectMapper.createObjectNode();
        payload.put("parent_workflow_id", "");
        payload.put("workflow_id", "workflow-123");
        payload.put("status", "COMPLETED");
        payload.put("reason_for_incompletion", "");

        String response = eventPublisher.sendCentralMessage("100", "journey_conductor_workflow_event", payload);

        assertNull(response);
    }
}
