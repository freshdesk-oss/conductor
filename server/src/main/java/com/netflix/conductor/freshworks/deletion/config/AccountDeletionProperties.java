package com.netflix.conductor.freshworks.deletion.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** Configuration for the Central-integrated account data deletion feature. */
@ConfigurationProperties("conductor.account-deletion")
public class AccountDeletionProperties {

    /** Enables the account deletion consume/delete/publish feature. */
    private boolean enabled = false;

    /** This service's FreshID-registered service name, emitted in the status payload. */
    private String service = "conductor";

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
}
