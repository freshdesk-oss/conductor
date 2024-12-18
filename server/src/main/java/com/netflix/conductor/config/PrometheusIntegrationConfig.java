package com.netflix.conductor.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

import com.netflix.spectator.api.Spectator;
import com.netflix.spectator.micrometer.MicrometerRegistry;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.micrometer.prometheus.PrometheusRenameFilter;

// This class loads all the configurations related to prometheus. 
@Configuration
@ConditionalOnProperty(name = "prometheus.integration.enabled", havingValue = "true", matchIfMissing = false)
public class PrometheusIntegrationConfig
        implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(PrometheusIntegrationConfig.class);
    private PrometheusMeterRegistry prometheusRegistry;

    public PrometheusIntegrationConfig(PrometheusMeterRegistry prometheusRegistry) {
        this.prometheusRegistry = prometheusRegistry;
    }

    @Override
    public void run(String... args) throws Exception {
        log.info("Registered PrometheusRegistry");
        final MicrometerRegistry metricsRegistry = new MicrometerRegistry(prometheusRegistry);
        prometheusRegistry.config().meterFilter(new PrometheusRenameFilter());
        Spectator.globalRegistry().add(metricsRegistry);
    }
}