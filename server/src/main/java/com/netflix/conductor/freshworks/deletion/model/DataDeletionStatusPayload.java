package com.netflix.conductor.freshworks.deletion.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

/**
 * Inner payload of the {@code ACCOUNT_DELETION_STATUS} event published to Central. Serialized with
 * snake_case field names; {@code null} fields (e.g. an absent {@code message}) are omitted via
 * {@code NON_NULL} inclusion so serialization does not depend on the caller's {@code ObjectMapper}.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class DataDeletionStatusPayload {

    public static final String EVENT_TYPE = "ACCOUNT_DELETION_STATUS";

    private String eventType = EVENT_TYPE;
    private String deletionRequestId;
    private String service;
    private String organisationId;
    private String bundleId;
    private String accountId;
    private String product;
    private String productAccountId;
    private String productId;
    private String status;
    private String timestamp;
    private String message;

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getDeletionRequestId() {
        return deletionRequestId;
    }

    public void setDeletionRequestId(String deletionRequestId) {
        this.deletionRequestId = deletionRequestId;
    }

    public String getService() {
        return service;
    }

    public void setService(String service) {
        this.service = service;
    }

    public String getOrganisationId() {
        return organisationId;
    }

    public void setOrganisationId(String organisationId) {
        this.organisationId = organisationId;
    }

    public String getBundleId() {
        return bundleId;
    }

    public void setBundleId(String bundleId) {
        this.bundleId = bundleId;
    }

    public String getAccountId() {
        return accountId;
    }

    public void setAccountId(String accountId) {
        this.accountId = accountId;
    }

    public String getProduct() {
        return product;
    }

    public void setProduct(String product) {
        this.product = product;
    }

    public String getProductAccountId() {
        return productAccountId;
    }

    public void setProductAccountId(String productAccountId) {
        this.productAccountId = productAccountId;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
