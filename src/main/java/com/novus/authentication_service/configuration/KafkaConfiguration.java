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
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 3000);

        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "authentication-service-client");
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1);
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(1);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.getContainerProperties().setMissingTopicsFatal(false);
        factory.getContainerProperties().setSyncCommits(true);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                (record, exception) -> log.error("Error in consumer: topic={}, partition={}, offset={}, exception={}",
                        record.topic(), record.partition(), record.offset(), exception.getMessage()),
                new FixedBackOff(1000L, 3)
        );
        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }

    @PostConstruct
    public void logKafkaConfig() {
        try {
            Properties props = new Properties();
            props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, envConfiguration.getKafkaBootstrapServers());

            AdminClient adminClient = AdminClient.create(props);
            Set<String> topics = adminClient.listTopics().names().get();

            log.info("Kafka configuration initialized");
            log.info("Bootstrap servers: {}", envConfiguration.getKafkaBootstrapServers());
            log.info("Available topics: {}", topics);

            adminClient.close();
        } catch (Exception e) {
            log.error("Error connecting to Kafka", e);
        }
    }
}