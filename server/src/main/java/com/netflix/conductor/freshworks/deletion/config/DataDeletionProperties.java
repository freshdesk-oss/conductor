package com.netflix.conductor.freshworks.deletion.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** Configuration for the Central-integrated account data deletion feature. */
@ConfigurationProperties("conductor.data-deletion")
public class DataDeletionProperties {

    /** This service's FreshID-registered service name, emitted in the status payload. */
    private String service = "conductor";

    public String getService() {
        return service;
    }

    public void setService(String service) {
        this.service = service;
    }
}
