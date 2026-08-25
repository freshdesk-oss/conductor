package com.netflix.conductor.freshworks.deletion.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

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

    /** Number of threads used to run account purges asynchronously. */
    private int purgeThreads = 2;

    /** Max number of enumerate-then-delete passes before a purge is considered incomplete. */
    private int maxPurgePasses = 5;

    /** Kafka topic that {@code ACCOUNT_DELETION_STATUS} events are published to. */
    private String statusTopic = "account-deletion-notifications";

    @NestedConfigurationProperty private final Retry retry = new Retry();

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

    public int getPurgeThreads() {
        return purgeThreads;
    }

    public void setPurgeThreads(int purgeThreads) {
        this.purgeThreads = purgeThreads;
    }

    public int getMaxPurgePasses() {
        return maxPurgePasses;
    }

    public void setMaxPurgePasses(int maxPurgePasses) {
        this.maxPurgePasses = maxPurgePasses;
    }

    public String getStatusTopic() {
        return statusTopic;
    }

    public void setStatusTopic(String statusTopic) {
        this.statusTopic = statusTopic;
    }

    public Retry getRetry() {
        return retry;
    }

    /** Retry behaviour for transient failures while purging account data. */
    public static class Retry {

        private int maxAttempts = 3;

        public int getMaxAttempts() {
            return maxAttempts;
        }

        public void setMaxAttempts(int maxAttempts) {
            this.maxAttempts = maxAttempts;
        }
    }
}
