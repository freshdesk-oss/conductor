package com.netflix.conductor.core.central.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.core.central.CentralRetryableException;
import com.netflix.conductor.core.central.model.CentralData;
import com.netflix.conductor.core.central.CentralProperties;
import kong.unirest.HttpResponse;
import kong.unirest.Unirest;
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

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public CentralProducer(CentralProperties centralProperties, @Qualifier("centralRetryTemplate") RetryTemplate retryTemplate) {
        this.centralProperties = centralProperties;
        this.retryTemplate = retryTemplate;
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
        SendCentralMessage(centralProperties.getCentralUrl(), centralMessage);
    }

    public String SendCentralMessage(String centralUrl, CentralData centralMessage) {
        return retryTemplate.execute(context -> {
            HttpResponse<String> response = null;
            try {
                response = Unirest.post(centralUrl).headers(getHeaders()).body(objectMapper.writeValueAsString(centralMessage)).asString();
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
            if (response == null) {
                throw new RuntimeException("Central response is null");
            }
            int statusCategory = getStatus(response.getStatus());
            if (statusCategory == 2) {
                return response.getBody();
            } else if (statusCategory == 5) {
                throw new CentralRetryableException("Error in central publisher");
            }
            throw new RuntimeException("Something went wrong");
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
