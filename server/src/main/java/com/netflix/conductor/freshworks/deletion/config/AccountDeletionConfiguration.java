package com.netflix.conductor.freshworks.deletion.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;

import com.freshworks.boot.common.AccountFetcher;
import com.freshworks.boot.common.context.account.IAccount;
import com.freshworks.boot.messaging.KafkaMessageKey;
import com.freshworks.boot.sdk.kafka.model.CentralPayload;
import com.freshworks.boot.sdk.kafka.service.DefaultKafkaPublisher;
import com.freshworks.boot.sdk.kafka.service.KafkaPublisher;
import com.freshworks.boot.sdk.kafka.util.ProducerHelper;
import com.netflix.conductor.freshworks.deletion.model.AccountDeletionStatusPayload;

/**
 * Wires the account deletion feature. Everything here (and the feature's components) is gated on
 * {@code conductor.account-deletion.enabled=true}, so the feature is inert by default.
 */
@Configuration
@EnableConfigurationProperties(AccountDeletionProperties.class)
@ConditionalOnProperty(name = "conductor.account-deletion.enabled", havingValue = "true")
public class AccountDeletionConfiguration {

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

    /**
     * {@code freshworks-boot-central-kafka-sdk}'s own {@code kafkaPublisher}/{@code kafkaTemplate}
     * beans come from a generic {@code CentralKafkaProducerConfig} factory method (<code>{@literal
     * <P>} KafkaPublisher&lt;KafkaMessageKey, CentralPayload&lt;P&gt;&gt;</code>) with an unresolved
     * type variable, so Spring registers it with no resolvable generic signature. That satisfies
     * {@code hasSingleBean(KafkaPublisher.class)} (raw-type check) but can never be autowired into a
     * concretely-typed injection point like {@link
     * com.netflix.conductor.freshworks.deletion.AccountDeletionStatusPublisher}'s constructor —
     * Spring fails with "no qualifying bean" for the fully-parameterized type (workflow-service's
     * own {@code KafkaConfig} hits the same SDK limitation and works around it the same way: a
     * concretely-typed {@code @Bean} method reusing the SDK's already-configured {@code
     * KafkaTemplate}/{@code ProducerHelper}).
     *
     * <p>Named {@code kafkaPublisher} to match the SDK's own bean name: since this is a
     * user-defined {@code @Configuration} bean (registered before deferred auto-configuration
     * runs), the SDK's {@code CentralKafkaProducerConfig#kafkaPublisher} method's {@code
     * @ConditionalOnMissingBean(name = "kafkaPublisher")} then sees the name already taken and
     * backs off, leaving exactly one (concretely-typed) {@code KafkaPublisher} bean instead of two.
     */
    @Bean
    @SuppressWarnings({"unchecked", "rawtypes"})
    public KafkaPublisher<KafkaMessageKey, CentralPayload<AccountDeletionStatusPayload>> kafkaPublisher(
            KafkaTemplate kafkaTemplate, ProducerHelper producerHelper) {
        return new DefaultKafkaPublisher<>(kafkaTemplate, producerHelper);
    }
}
