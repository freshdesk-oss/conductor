package com.netflix.conductor.freshworks.deletion.config;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import com.freshworks.boot.sdk.kafka.config.CentralKafkaProducerConfig;
import com.freshworks.boot.sdk.kafka.service.KafkaPublisher;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code freshworks-boot-central-kafka-sdk}'s {@code CentralKafkaProducerConfig} auto-configures
 * unconditionally (no {@code @ConditionalOnProperty}) and requires a {@code PayloadVersionFetcher}
 * bean at startup. This verifies {@link DataDeletionPayloadVersionFetcher} alone satisfies that
 * requirement.
 */
class DataDeletionKafkaAutoConfigurationTest {

    private final ApplicationContextRunner contextRunner =
            new ApplicationContextRunner()
                    .withConfiguration(
                            org.springframework.boot.autoconfigure.AutoConfigurations.of(
                                    KafkaAutoConfiguration.class, CentralKafkaProducerConfig.class))
                    .withUserConfiguration(DataDeletionPayloadVersionFetcher.class);

    @Test
    void bootsWithMinimalWiring() {
        contextRunner.run(
                context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(KafkaPublisher.class);
                });
    }
}
