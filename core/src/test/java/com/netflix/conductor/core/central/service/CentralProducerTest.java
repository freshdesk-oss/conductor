package com.netflix.conductor.core.central.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.core.central.model.CentralProperties;
import com.netflix.conductor.core.central.exception.CentralRetryableException;
import com.netflix.conductor.core.central.client.HttpClient;
import com.netflix.conductor.core.status.WorkflowEvent;
import kong.unirest.HttpResponse;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.support.RetryTemplate;

import static com.netflix.conductor.core.central.CentralConstants.JOURNEY_CONDUCTOR_WORKFLOW_EVENT;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.anyMap;

import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(MockitoExtension.class)
class CentralProducerTest {
    @InjectMocks
    private CentralProducer centralProducer;

    @Mock
    private CentralProperties centralProperties;

    @Mock
    private RetryTemplate retryTemplate;

    @Mock
    private HttpClient httpClient;

    @Mock
    private HttpResponse<String> httpResponse;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void testPublish() {
        when(centralProperties.getPod()).thenReturn("poduseast");
        when(centralProperties.getRegion()).thenReturn("us-east-1");
        when(centralProperties.getService()).thenReturn("freshservice-v2-dev");
        when(centralProperties.getUrl()).thenReturn("http://mock-central.url");
        when(centralProperties.getToken()).thenReturn("token-123");

        when(retryTemplate.execute(any(),any(),any())).thenAnswer(invocation -> {
            RetryCallback retry = invocation.getArgument(0);
            return retry.doWithRetry(null);
        });

        when(httpClient.post(anyString(), anyMap(), anyString())).thenReturn(httpResponse);
        when(httpResponse.getStatus()).thenReturn(200);
        when(httpResponse.getBody()).thenReturn("success");

        centralProducer.publish("100", objectMapper.valueToTree(WorkflowEvent.builder().build()), JOURNEY_CONDUCTOR_WORKFLOW_EVENT);

        verify(httpClient, times(1)).post(anyString(), anyMap(), anyString());
        verify(retryTemplate, times(1)).execute(any(), any(), any());
    }

    @Test
    void testPublish_Error() {
        when(centralProperties.getPod()).thenReturn("poduseast");
        when(centralProperties.getRegion()).thenReturn("us-east-1");
        when(centralProperties.getService()).thenReturn("freshservice-v2-dev");
        when(centralProperties.getUrl()).thenReturn("http://mock-central.url");
        when(centralProperties.getToken()).thenReturn("token-123");

        when(retryTemplate.execute(any(),any(),any())).thenAnswer(invocation -> {
            RetryCallback retry = invocation.getArgument(0);
            return retry.doWithRetry(null);
        });

        when(httpClient.post(anyString(), anyMap(), anyString())).thenReturn(null);

        JsonNode event = objectMapper.valueToTree(WorkflowEvent.builder().build());

        assertThrows(RuntimeException.class, () -> centralProducer.publish("100", event, JOURNEY_CONDUCTOR_WORKFLOW_EVENT));
    }

    @Test
    void testPublish_5xx_Error() {
        when(centralProperties.getPod()).thenReturn("poduseast");
        when(centralProperties.getRegion()).thenReturn("us-east-1");
        when(centralProperties.getService()).thenReturn("freshservice-v2-dev");
        when(centralProperties.getUrl()).thenReturn("http://mock-central.url");
        when(centralProperties.getToken()).thenReturn("token-123");

        when(retryTemplate.execute(any(),any(),any())).thenAnswer(invocation -> {
            RetryCallback retry = invocation.getArgument(0);
            return retry.doWithRetry(null);
        });

        when(httpClient.post(anyString(), anyMap(), anyString())).thenReturn(httpResponse);
        when(httpResponse.getStatus()).thenReturn(500);

        JsonNode event = objectMapper.valueToTree(WorkflowEvent.builder().build());

        assertThrows(CentralRetryableException.class, () -> centralProducer.publish("100", event, JOURNEY_CONDUCTOR_WORKFLOW_EVENT));
    }

    @Test
    void testPublish_4xx_Error() {
        when(centralProperties.getPod()).thenReturn("poduseast");
        when(centralProperties.getRegion()).thenReturn("us-east-1");
        when(centralProperties.getService()).thenReturn("freshservice-v2-dev");
        when(centralProperties.getUrl()).thenReturn("http://mock-central.url");
        when(centralProperties.getToken()).thenReturn("token-123");

        when(retryTemplate.execute(any(),any(),any())).thenAnswer(invocation -> {
            RetryCallback retry = invocation.getArgument(0);
            return retry.doWithRetry(null);
        });

        when(httpClient.post(anyString(), anyMap(), anyString())).thenReturn(httpResponse);
        when(httpResponse.getStatus()).thenReturn(400);

        JsonNode event = objectMapper.valueToTree(WorkflowEvent.builder().build());

        assertThrows(RuntimeException.class, () -> centralProducer.publish("100", event, JOURNEY_CONDUCTOR_WORKFLOW_EVENT));
    }
}
