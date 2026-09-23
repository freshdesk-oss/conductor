package com.netflix.conductor.freshworks.deletion.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Inner payload (the {@code data.payload} node) of the FreshID {@code ACCOUNT_DELETION_REQUESTED}
 * event delivered by Central. Unknown properties are ignored so future contract additions do not
 * break deserialization.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DataDeletionRequestPayload {

    @JsonProperty("deletion_request_id")
    private String deletionRequestId;

    @JsonProperty("organisation_id")
    private String organisationId;

    @JsonProperty("bundle_id")
    private String bundleId;

    /** FreshID account id — echoed back in the status event only. */
    @JsonProperty("account_id")
    private String accountId;

    @JsonProperty("product")
    private String product;

    @JsonProperty("product_id")
    private String productId;

    @JsonProperty("product_account_id")
    private String productAccountId;

    public String getDeletionRequestId() {
        return deletionRequestId;
    }

    public String getOrganisationId() {
        return organisationId;
    }

    public String getBundleId() {
        return bundleId;
    }

    public String getAccountId() {
        return accountId;
    }

    public String getProduct() {
        return product;
    }

    public String getProductId() {
        return productId;
    }

    public String getProductAccountId() {
        return productAccountId;
    }
}
