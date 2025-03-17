package com.netflix.conductor.core.central.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.core.central.exception.CentralException;
import com.netflix.conductor.core.central.exception.CentralRetryableException;
import com.netflix.conductor.core.central.client.HttpClient;
import com.netflix.conductor.core.central.model.CentralData;
import com.netflix.conductor.core.central.CentralProperties;
import kong.unirest.HttpResponse;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Service;


import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static com.netflix.conductor.core.central.CentralConstants.*;

@Service
public class CentralProducer {
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

        publishCentralMessage(centralData);
    }

    public void publishCentralMessage(CentralData centralMessage) {
        sendCentralMessage(centralProperties.getCentralUrl(), centralMessage);
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
        return retryTemplate.execute(context -> {
            HttpResponse<String> response = null;
            try {
                response = httpClient.post(centralUrl, getHeaders(), objectMapper.writeValueAsString(centralMessage));
            } catch (JsonProcessingException ex) {
                throw new CentralException("Error occurred while sending central message. Exception: ", ex);
            }
            if (response == null) {
                throw new CentralException("Central response is null");
            }
            int statusCategory = getStatus(response.getStatus());
            if (statusCategory == 2) {
                return response.getBody();
            } else if (statusCategory == 5) {
                throw new CentralRetryableException("Error occurred while sending central message. response: " + response.getBody());
            }
            throw new CentralException("Something went wrong. message: " + response.getBody());
        });
    }

    private Map<String, String> getHeaders() {
        Map<String, String> headers = new HashMap<>();
        headers.put(CONTENT_TYPE, CONTENT_TYPE_JSON);
        headers.put(SERVICE, centralProperties.getCentralToken());
        headers.put(X_REQUEST_ID, UUID.randomUUID().toString());
        return headers;
    }

    private int getStatus(int status) {
        return status / 100;
    }
}
