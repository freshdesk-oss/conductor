package com.netflix.conductor.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Configuration;

import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Spectator;
import com.netflix.spectator.micrometer.MicrometerRegistry;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.micrometer.prometheus.PrometheusRenameFilter;
import io.prometheus.client.CollectorRegistry;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Metrics;

// This class loads all the configurations related to prometheus. 
@Configuration
public class PrometheusIntegrationConfig
        implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(PrometheusIntegrationConfig.class);
    private static PrometheusMeterRegistry prometheusRegistry;

    public PrometheusIntegrationConfig(PrometheusMeterRegistry prometheusRegistry) {
        this.prometheusRegistry = prometheusRegistry;
    }

    @Override
    public void run(String... args) throws Exception {
        setupPrometheusRegistry();
    }

    /**
     * To Register PrometheusRegistry
    */
    private static void setupPrometheusRegistry() {
        log.info("Registered PrometheusRegistry");
        final MicrometerRegistry metricsRegistry = new MicrometerRegistry(prometheusRegistry);
        prometheusRegistry.config().meterFilter(new PrometheusRenameFilter());
        Spectator.globalRegistry().add(metricsRegistry);
    }

}
