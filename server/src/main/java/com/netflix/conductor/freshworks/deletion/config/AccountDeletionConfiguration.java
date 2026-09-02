package com.netflix.conductor.freshworks.deletion.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.freshworks.boot.common.AccountFetcher;
import com.freshworks.boot.common.context.account.IAccount;

/**
 * Wires the account deletion feature. Everything here (and the feature's components) is gated on
 * {@code conductor.account-deletion.enabled=true}, so the feature is inert by default.
 *
 * <p>Outbound {@code ACCOUNT_DELETION_STATUS} publishing needs no {@code KafkaTemplate}/{@code
 * KafkaPublisher} bean here — {@code freshworks-boot-central-kafka-sdk}'s own auto-configuration
 * ({@code CentralKafkaProducerConfig}) supplies those once the dependency is on the classpath.
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
}
