package com.netflix.conductor.freshworks.deletion.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Inner payload (the {@code data.payload} node) of the FreshID {@code ACCOUNT_DELETION_REQUESTED}
 * event delivered by Central. Unknown properties are ignored so future contract additions do not
 * break deserialization.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class AccountDeletionRequestedEvent {

    @JsonProperty("event_type")
    private String eventType;

    @JsonProperty("deletion_request_id")
    private String deletionRequestId;

    @JsonProperty("organisation_id")
    private String organisationId;

    @JsonProperty("bundle_id")
    private String bundleId;

    /** FreshID account id — echoed back only; not usable as conductor's numeric shard key. */
    @JsonProperty("account_id")
    private String accountId;

    @JsonProperty("product")
    private String product;

    @JsonProperty("product_id")
    private String productId;

    /** Product account id — this is conductor's {@code correlationId}/{@code shard_id}. */
    @JsonProperty("product_account_id")
    private String productAccountId;

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

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public String getProductAccountId() {
        return productAccountId;
    }

    public void setProductAccountId(String productAccountId) {
        this.productAccountId = productAccountId;
    }
}
