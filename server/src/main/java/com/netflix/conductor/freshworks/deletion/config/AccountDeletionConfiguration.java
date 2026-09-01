package com.netflix.conductor.freshworks.deletion.config;

import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import com.freshworks.boot.common.AccountFetcher;
import com.freshworks.boot.common.context.account.IAccount;
import com.netflix.conductor.freshworks.deletion.model.AccountDeletionStatusPayload;
import com.netflix.conductor.freshworks.deletion.model.CentralPayload;

/**
 * Wires the account deletion feature. Everything here (and the feature's components) is gated on
 * {@code conductor.account-deletion.enabled=true}, so the feature is inert by default.
 */
@Configuration
@EnableConfigurationProperties(AccountDeletionProperties.class)
@ConditionalOnProperty(name = "conductor.account-deletion.enabled", havingValue = "true")
public class AccountDeletionConfiguration {

    /**
     * Publishing reuses the {@code spring.kafka.consumer.*} connection (bootstrap servers, SASL)
     * instead of a separate {@code spring.kafka.producer.*} block, since both point at the same
     * Central Kafka cluster.
     */
    @Bean
    public ProducerFactory<String, CentralPayload<AccountDeletionStatusPayload>>
            accountDeletionProducerFactory(KafkaProperties kafkaProperties) {
        Map<String, Object> props = kafkaProperties.buildConsumerProperties();
        props.remove(ConsumerConfig.GROUP_ID_CONFIG);
        props.remove(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);
        props.remove(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
        props.remove(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
        props.remove(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, CentralPayload<AccountDeletionStatusPayload>>
            accountDeletionKafkaTemplate(
                    ProducerFactory<String, CentralPayload<AccountDeletionStatusPayload>>
                            accountDeletionProducerFactory) {
        return new KafkaTemplate<>(accountDeletionProducerFactory);
    }

    /**
     * {@code freshworks-boot-kafka}'s {@code BootKafkaListener} requires an {@code AccountFetcher}
     * bean to exist for every {@code @CentralListener}, even though account-deletion never enables
     * account-context filtering ({@code messageFilterWithAccountContextEnabled=false} is the
     * default). It's still invoked unconditionally per message, so this returns {@code null} rather
     * than throwing.
     */
    @Bean
    @ConditionalOnMissingBean(AccountFetcher.class)
    public AccountFetcher<IAccount> accountDeletionAccountFetcher() {
        return (service, accountId) -> null;
    }
}
