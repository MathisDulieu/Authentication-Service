package com.novus.authentication_service.configuration;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@EnableKafka
@Configuration
@RequiredArgsConstructor
public class KafkaConfiguration {

    private final EnvConfiguration envConfiguration;

    private static final String bootstrapServers = "kafka.railway.internal:29092";

    private static final String groupId = "authentication-groupId";

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // Configuration pour aider à résoudre le problème de coordinateur
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 60000); // 60 secondes
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 20000); // 20 secondes
        props.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, 5000); // 5 secondes
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 60000); // 60 secondes
        props.put(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, 1000); // 1 seconde

        log.info("Configuration consumer - Bootstrap servers: {}, Group ID: {}", bootstrapServers, groupId);

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        // Augmenter les timeouts pour les problèmes de réseau
        factory.getContainerProperties().setPollTimeout(5000);

        // Configuration manuelle du coordinateur
        factory.getContainerProperties().setMissingTopicsFatal(false);

        // Amélioration des logs
        factory.getContainerProperties().setLogContainerConfig(true);

        return factory;
    }
}