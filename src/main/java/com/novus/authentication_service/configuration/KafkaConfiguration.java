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
import java.util.UUID;

@Slf4j
@EnableKafka
@Configuration
@RequiredArgsConstructor
public class KafkaConfiguration {

    private final EnvConfiguration envConfiguration;

    private static final String bootstrapServers = "kafka.railway.internal:29092";

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        props.put(ConsumerConfig.GROUP_ID_CONFIG, "diagnostic-" + UUID.randomUUID());

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);

        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        log.info("Configuration consumer - Bootstrap servers: {}, Group ID: {}", bootstrapServers, props.get(ConsumerConfig.GROUP_ID_CONFIG));

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());

        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        factory.getContainerProperties().setSyncCommits(true);

        factory.getContainerProperties().setPollTimeout(5000);

        log.info("Kafka Listener configuré avec AckMode.MANUAL_IMMEDIATE et timeout 5000ms");

        return factory;
    }
}