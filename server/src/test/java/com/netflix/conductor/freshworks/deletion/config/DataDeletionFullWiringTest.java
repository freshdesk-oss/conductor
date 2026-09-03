package com.netflix.conductor.freshworks.deletion.config;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import com.freshworks.boot.sdk.kafka.config.CentralKafkaProducerConfig;
import com.freshworks.boot.sdk.kafka.service.KafkaPublisher;
import com.netflix.conductor.freshworks.deletion.DataDeletionStatusPublisher;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the real consumer of the data-deletion Kafka wiring, not just that some {@code
 * KafkaPublisher} bean exists by raw type. {@code CentralKafkaProducerConfig}'s own {@code
 * kafkaPublisher} bean comes from a generic factory method with an unresolved type variable, so it
 * satisfies a raw-type check but can never be autowired into {@link
 * DataDeletionStatusPublisher}'s concretely-typed constructor parameter — that gap is exactly
 * what let the app fail to start in production despite {@link
 * DataDeletionKafkaAutoConfigurationTest} passing.
 */
class DataDeletionFullWiringTest {

    private final ApplicationContextRunner contextRunner =
            new ApplicationContextRunner()
                    .withConfiguration(
                            org.springframework.boot.autoconfigure.AutoConfigurations.of(
                                    KafkaAutoConfiguration.class, CentralKafkaProducerConfig.class))
                    .withUserConfiguration(
                            DataDeletionPayloadVersionFetcher.class,
                            DataDeletionConfiguration.class,
                            KafkaConfig.class,
                            DataDeletionStatusPublisher.class);

    @Test
    void bootsWithDataDeletionWiring() {
        contextRunner.run(
                context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(DataDeletionStatusPublisher.class);
                    // Exactly one KafkaPublisher bean: ours takes the "kafkaPublisher" name, so the
                    // SDK's own generically-unresolved bean of that name backs off instead of
                    // coexisting as a dangling duplicate.
                    assertThat(context).hasSingleBean(KafkaPublisher.class);
                });
    }
}
