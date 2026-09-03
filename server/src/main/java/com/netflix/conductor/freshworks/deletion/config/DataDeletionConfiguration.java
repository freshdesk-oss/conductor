package com.netflix.conductor.freshworks.deletion.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.freshworks.boot.common.AccountFetcher;
import com.freshworks.boot.common.context.account.IAccount;

/**
 * Wires the account deletion feature, always active. Kafka producer wiring lives separately in
 * {@link KafkaConfig}.
 */
@Configuration
@EnableConfigurationProperties(DataDeletionProperties.class)
public class DataDeletionConfiguration {

    /**
     * {@code freshworks-boot-kafka}'s {@code BootKafkaListener} requires an {@code AccountFetcher}
     * bean to exist for every {@code @CentralListener}, even though data-deletion never enables
     * account-context filtering ({@code messageFilterWithAccountContextEnabled=false} is the
     * default). It's still invoked unconditionally per message, so this returns {@code null} rather
     * than throwing.
     */
    @Bean
    @ConditionalOnMissingBean(AccountFetcher.class)
    public AccountFetcher<IAccount> dataDeletionAccountFetcher() {
        return (service, accountId) -> null;
    }
}
