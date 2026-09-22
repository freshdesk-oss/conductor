package com.netflix.conductor.freshworks.deletion.config;

import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import java.util.Map;

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
 * Kafka producer wiring for the data deletion feature.
 */
@Configuration
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

    @Bean
    public KafkaTemplate<KafkaMessageKey, CentralPayload<DataDeletionStatusPayload>> kafkaTemplate(
            ProducerFactory<KafkaMessageKey, CentralPayload<DataDeletionStatusPayload>>
                    dataDeletionProducerFactory) {
        KafkaTemplate<KafkaMessageKey, CentralPayload<DataDeletionStatusPayload>> kafkaTemplate =
                new KafkaTemplate<>(dataDeletionProducerFactory);
        kafkaTemplate.setProducerListener(new ProducerLogListener<>());
        return kafkaTemplate;
    }

    @Bean
    public KafkaPublisher<KafkaMessageKey, CentralPayload<DataDeletionStatusPayload>> kafkaPublisher(
            KafkaTemplate<KafkaMessageKey, CentralPayload<DataDeletionStatusPayload>> kafkaTemplate,
            ProducerHelper producerHelper) {
        return new DefaultKafkaPublisher<>(kafkaTemplate, producerHelper);
    }
}
