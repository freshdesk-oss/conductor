package com.netflix.conductor.core.central.model;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Getter
@Setter
@Component
@ConfigurationProperties(prefix = "conductor.central")
public class CentralProperties {
    private String pod;

    private String region;

    private String service;

    private String url;

    private String token;
}
