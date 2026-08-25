package com.netflix.conductor.freshworks.deletion.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Routing envelope published to Central's Kafka topics, reproducing the wire shape of
 * {@code com.freshworks.boot.sdk.kafka.model.CentralData}.
 *
 * @param <P> the inner payload type (e.g. {@link AccountDeletionStatusPayload})
 */
public class CentralData<P> {

    @JsonProperty("account_id")
    private String accountId;

    @JsonProperty("region")
    private String region;

    @JsonProperty("organisation_id")
    private String organisationId;

    @JsonProperty("product_id")
    private String productId;

    @JsonProperty("bundle_id")
    private String bundleId;

    @JsonProperty("pod")
    private String pod;

    @JsonProperty("payload_type")
    private String payloadType;

    @JsonProperty("payload_version")
    private String payloadVersion;

    @JsonProperty("payload")
    private P payload;

    public String getAccountId() {
        return accountId;
    }

    public void setAccountId(String accountId) {
        this.accountId = accountId;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getOrganisationId() {
        return organisationId;
    }

    public void setOrganisationId(String organisationId) {
        this.organisationId = organisationId;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public String getBundleId() {
        return bundleId;
    }

    public void setBundleId(String bundleId) {
        this.bundleId = bundleId;
    }

    public String getPod() {
        return pod;
    }

    public void setPod(String pod) {
        this.pod = pod;
    }

    public String getPayloadType() {
        return payloadType;
    }

    public void setPayloadType(String payloadType) {
        this.payloadType = payloadType;
    }

    public String getPayloadVersion() {
        return payloadVersion;
    }

    public void setPayloadVersion(String payloadVersion) {
        this.payloadVersion = payloadVersion;
    }

    public P getPayload() {
        return payload;
    }

    public void setPayload(P payload) {
        this.payload = payload;
    }
}
