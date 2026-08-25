package com.netflix.conductor.freshworks.deletion;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;

import com.netflix.conductor.freshworks.deletion.config.AccountDeletionProperties;
import com.netflix.conductor.freshworks.deletion.model.AccountDeletionStatusPayload;
import com.netflix.conductor.freshworks.deletion.model.CentralPayload;

/**
 * Publishes events directly to Central's Kafka topic, keyed by {@code productAccountId} so all
 * status events for an account land on the same partition via Kafka's default partitioner.
 */
@Component
@ConditionalOnProperty(name = "conductor.account-deletion.enabled", havingValue = "true")
public class CentralKafkaPublisher {

    private final KafkaTemplate<String, CentralPayload<AccountDeletionStatusPayload>> kafkaTemplate;
    private final AccountDeletionProperties properties;

    public CentralKafkaPublisher(
            KafkaTemplate<String, CentralPayload<AccountDeletionStatusPayload>>
                    accountDeletionKafkaTemplate,
            AccountDeletionProperties properties) {
        this.kafkaTemplate = accountDeletionKafkaTemplate;
        this.properties = properties;
    }

    public ListenableFuture<SendResult<String, CentralPayload<AccountDeletionStatusPayload>>> publish(
            String productAccountId, CentralPayload<AccountDeletionStatusPayload> payload) {
        return kafkaTemplate.send(properties.getStatusTopic(), productAccountId, payload);
    }
}
