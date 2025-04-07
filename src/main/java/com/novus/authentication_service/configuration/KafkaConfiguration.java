package com.novus.authentication_service.configuration;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

@Slf4j
@EnableKafka
@Configuration
@RequiredArgsConstructor
public class KafkaConfiguration {

    private final EnvConfiguration envConfiguration;

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        String bootstrapServers = envConfiguration.getKafkaBootstrapServers();
        log.info("Configuring Kafka consumer with bootstrap servers: {}", bootstrapServers);

        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "authentication-service-group");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "authentication-service-client");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 50);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 15000);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 3000);
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1);
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 300);

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(3);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.getContainerProperties().setMissingTopicsFatal(false);
        factory.getContainerProperties().setSyncCommits(true);

        ExponentialBackOff backOff = new ExponentialBackOff(1000L, 2.0);
        backOff.setMaxElapsedTime(10000L);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                (record, exception) -> {
                    log.error("Processing error: topic={}, partition={}, offset={}, exception={}",
                            record.topic(), record.partition(), record.offset(), exception.getMessage(), exception);
                },
                backOff
        );

        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }


    @PostConstruct
    public void logKafkaConfig() {
        try {
            Properties props = new Properties();
            props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, envConfiguration.getKafkaBootstrapServers());
            props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);

            try (AdminClient adminClient = AdminClient.create(props)) {
                Set<String> topics = adminClient.listTopics().names().get();

                log.info("=== CONFIGURATION KAFKA ===");
                log.info("Bootstrap servers: {}", envConfiguration.getKafkaBootstrapServers());
                log.info("Topics disponibles: {}", topics);

                if (topics.contains("authentication-service")) {
                    log.info("Topic 'authentication-service' trouvé !");
                    adminClient.listConsumerGroups().valid().get().forEach(group -> {
                        log.info("Groupe consommateur: {}", group.groupId());
                    });
                } else {
                    log.warn("Topic 'authentication-service' NON TROUVÉ! Topics existants: {}", topics);
                }

                log.info("=== FIN CONFIGURATION KAFKA ===");
            }
        } catch (Exception e) {
            log.error("Erreur lors de la connexion à Kafka", e);
        }
    }

}