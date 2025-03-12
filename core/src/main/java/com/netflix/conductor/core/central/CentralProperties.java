package com.netflix.conductor.core.central;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Getter
@Component
public class CentralProperties {
    @Value("${conductor.central.pod}")
    private String pod;

    @Value("${conductor.central.region}")
    private String region;

    @Value("${conductor.central.service}")
    private String service;

    @Value("${conductor.central.central.url}")
    private String centralUrl;

    @Value("${conductor.central.central.token}")
    private String centralToken;
}
