package com.netflix.conductor.core.central.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.core.central.exception.CentralRetryableException;
import com.netflix.conductor.core.central.client.HttpClient;
import com.netflix.conductor.core.central.model.CentralData;
import com.netflix.conductor.core.central.model.CentralProperties;
import com.netflix.conductor.metrics.Monitors;
import kong.unirest.HttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Service;


import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static com.netflix.conductor.core.central.CentralConstants.*;

@Service
public class CentralProducer {
    private static final Logger LOGGER = LoggerFactory.getLogger(CentralProducer.class);
    private final CentralProperties centralProperties;
    private final RetryTemplate retryTemplate;
    private final HttpClient httpClient;

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public CentralProducer(
            CentralProperties centralProperties,
            @Qualifier("centralRetryTemplate") RetryTemplate retryTemplate,
            HttpClient httpClient) {
        this.centralProperties = centralProperties;
        this.retryTemplate = retryTemplate;
        this.httpClient = httpClient;
    }

    /***
     * This method forms the central payload with the necessary event properties and payload types.
     * @param accountId
     * @param event
     * @param payloadType
     */
    public void publish(String accountId, JsonNode event, String payloadType) {
        CentralData centralData =
                CentralData.builder()
                        .accountId(accountId)
                        .payload(event)
                        .payloadType(payloadType)
                        .pod(centralProperties.getPod())
                        .region(centralProperties.getRegion())
                        .service(centralProperties.getService())
                        .payloadVersion(PAYLOAD_VERSION)
                        .build();

        sendCentralMessage(centralProperties.getUrl(), centralData);
    }

    /***
     * This method sends the central message to the central service.
     * Retry is configured only in case of 5xx error from the central service.
     * In case of 4xx error, the exception is thrown.
     * @param centralUrl
     * @param centralMessage
     * @return
     */
    public String sendCentralMessage(String centralUrl, CentralData centralMessage) {
        long startTime = System.currentTimeMillis();
        try {
            return retryTemplate.execute(context -> {
                try {
                    String requestBody = objectMapper.writeValueAsString(centralMessage);
                    HttpResponse<String> response = httpClient.post(centralUrl, getHeaders(), requestBody);
                    int statusCategory = getStatus(response.getStatus());

                    if (statusCategory == 2) {
                        return response.getBody(); // Success
                    } else if (statusCategory == 5) {
                        throw new CentralRetryableException("Server error: " + response.getBody()); // Triggers retry
                    } else {
                        LOGGER.error("Non-retryable error. Status: {}, Body: {}", response.getStatus(), response.getBody());
                        return null; // Non-retryable case
                    }
                } catch (JsonProcessingException ex) {
                    LOGGER.error("Failed to serialize central message: {}", ex.getMessage(), ex);
                } catch (Exception ex) {
                    LOGGER.error("Unexpected error while sending central message: {}", ex.getMessage(), ex);
                }
                return null;
            });
        } catch (CentralRetryableException ex) {
            LOGGER.error("Failure after retries: {}", ex.getMessage(), ex);
            return null; // Prevent exception from processing
        } finally {
            long elapsedTime = System.currentTimeMillis() - startTime;
            Monitors.recordStatusListenerEventTime(elapsedTime);
            LOGGER.info("Completed request to central service after {} ms", elapsedTime);
        }
    }

    private Map<String, String> getHeaders() {
        Map<String, String> headers = new HashMap<>();
        headers.put(CONTENT_TYPE, CONTENT_TYPE_JSON);
        headers.put(SERVICE, centralProperties.getToken());
        headers.put(X_REQUEST_ID, UUID.randomUUID().toString());
        return headers;
    }

    private int getStatus(int status) {
        return status / 100;
    }
}
