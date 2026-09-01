package com.netflix.conductor.freshworks.deletion.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** Configuration for the Central-integrated account data deletion feature. */
@ConfigurationProperties("conductor.account-deletion")
public class AccountDeletionProperties {

    /** Enables the account deletion consume/delete/publish feature. */
    private boolean enabled = false;

    /** This service's FreshID-registered service name, emitted in the status payload. */
    private String service = "conductor";

    /** Deployment pod, forwarded to the status payload. */
    private String pod = "";

    /** Deployment region, forwarded to the status payload. */
    private String region = "";

    /** Kafka topic that {@code ACCOUNT_DELETION_STATUS} events are published to. */
    private String statusTopic = "account-deletion-notifications";

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getService() {
        return service;
    }

    public void setService(String service) {
        this.service = service;
    }

    public String getPod() {
        return pod;
    }

    public void setPod(String pod) {
        this.pod = pod;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getStatusTopic() {
        return statusTopic;
    }

    public void setStatusTopic(String statusTopic) {
        this.statusTopic = statusTopic;
    }
}
