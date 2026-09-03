package com.netflix.conductor.freshworks.deletion.config;

import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import java.util.Map;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import com.freshworks.boot.messaging.KafkaMessageKey;
import com.freshworks.boot.sdk.kafka.listeners.ProducerLogListener;
import com.freshworks.boot.sdk.kafka.model.CentralPayload;
import com.freshworks.boot.sdk.kafka.service.DefaultKafkaPublisher;
import com.freshworks.boot.sdk.kafka.service.KafkaPublisher;
import com.freshworks.boot.sdk.kafka.util.MessageKeySerializer;
import com.freshworks.boot.sdk.kafka.util.MessageValueSerializer;
import com.freshworks.boot.sdk.kafka.util.ProducerHelper;
import com.netflix.conductor.freshworks.deletion.model.DataDeletionStatusPayload;

/**
 * Kafka producer wiring for the account deletion feature, gated on the same {@code
 * conductor.data-deletion.enabled=true} flag as {@link DataDeletionConfiguration}.
 *
 * <p>{@code freshworks-boot-central-kafka-sdk}'s own {@code kafkaTemplate}/{@code kafkaPublisher}
 * beans come from generic {@code CentralKafkaProducerConfig} factory methods (<code>{@literal <P>}
 * KafkaTemplate&lt;KafkaMessageKey, CentralPayload&lt;P&gt;&gt;</code>) with an unresolved type
 * variable, so Spring registers them with no resolvable generic signature — they satisfy a
 * raw-type bean check but can never be autowired into a concretely-typed injection point like
 * {@link com.netflix.conductor.freshworks.deletion.DataDeletionStatusPublisher}'s constructor.
 * {@code workflow-service}'s {@code KafkaConfig} hits the same SDK limitation and works around it
 * the same way this does: a dedicated {@code KafkaConfig} building the producer stack with a
 * concrete generic type instead of going through the SDK's generic factory methods.
 *
 * <p>Reuses {@code spring.kafka.producer.*} (already configured for this SDK — see {@code
 * CentralKafkaProducerConfig#kafkaTemplate}, which builds its producer from the exact same {@link
 * KafkaProperties#buildProducerProperties()}) and the SDK's own {@code MessageKeySerializer}/
 * {@code MessageValueSerializer} so the wire format matches what Central expects, unlike {@code
 * workflow-service} which re-derives serializer/broker config from its own {@code @Value}
 * properties.
 */
@Configuration
@ConditionalOnProperty(name = "conductor.data-deletion.enabled", havingValue = "true")
public class KafkaConfig {

    @Bean
    public ProducerFactory<KafkaMessageKey, CentralPayload<DataDeletionStatusPayload>>
            dataDeletionProducerFactory(KafkaProperties kafkaProperties) {
        Map<String, Object> config = kafkaProperties.buildProducerProperties();
        config.put(KEY_SERIALIZER_CLASS_CONFIG, MessageKeySerializer.class);
        config.put(VALUE_SERIALIZER_CLASS_CONFIG, MessageValueSerializer.class);
        return new DefaultKafkaProducerFactory<>(
                config, new MessageKeySerializer(), new MessageValueSerializer<>());
    }

    /**
     * Named {@code kafkaTemplate} to match the SDK's own bean name: since this is a user-defined
     * {@code @Configuration} bean (registered before deferred auto-configuration runs), the SDK's
     * {@code CentralKafkaProducerConfig#kafkaTemplate}'s {@code @ConditionalOnMissingBean(
     * KafkaTemplate.class)} then sees the name/type already taken and backs off.
     */
    @Bean
    public KafkaTemplate<KafkaMessageKey, CentralPayload<DataDeletionStatusPayload>> kafkaTemplate(
            ProducerFactory<KafkaMessageKey, CentralPayload<DataDeletionStatusPayload>>
                    dataDeletionProducerFactory) {
        KafkaTemplate<KafkaMessageKey, CentralPayload<DataDeletionStatusPayload>> kafkaTemplate =
                new KafkaTemplate<>(dataDeletionProducerFactory);
        kafkaTemplate.setProducerListener(new ProducerLogListener<>());
        return kafkaTemplate;
    }

    /**
     * Named {@code kafkaPublisher} to match the SDK's own bean name, for the same reason as {@link
     * #kafkaTemplate}: the SDK's {@code CentralKafkaProducerConfig#kafkaPublisher}'s {@code
     * @ConditionalOnMissingBean(name = "kafkaPublisher")} sees the name already taken and backs
     * off, leaving exactly one (concretely-typed) {@code KafkaPublisher} bean instead of two.
     */
    @Bean
    public KafkaPublisher<KafkaMessageKey, CentralPayload<DataDeletionStatusPayload>> kafkaPublisher(
            KafkaTemplate<KafkaMessageKey, CentralPayload<DataDeletionStatusPayload>> kafkaTemplate,
            ProducerHelper producerHelper) {
        return new DefaultKafkaPublisher<>(kafkaTemplate, producerHelper);
    }
}
