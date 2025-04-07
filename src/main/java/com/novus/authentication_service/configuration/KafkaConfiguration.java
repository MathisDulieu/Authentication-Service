//package com.novus.authentication_service.configuration;
//
//import jakarta.annotation.PostConstruct;
//import lombok.RequiredArgsConstructor;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.kafka.clients.admin.AdminClient;
//import org.apache.kafka.clients.admin.AdminClientConfig;
//import org.apache.kafka.clients.admin.NewPartitions;
//import org.apache.kafka.clients.admin.NewTopic;
//import org.apache.kafka.clients.consumer.ConsumerConfig;
//import org.apache.kafka.common.serialization.StringDeserializer;
//import org.springframework.boot.ApplicationRunner;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.Configuration;
//import org.springframework.kafka.annotation.EnableKafka;
//import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
//import org.springframework.kafka.core.ConsumerFactory;
//import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
//import org.springframework.kafka.listener.ContainerProperties;
//import org.springframework.kafka.listener.DefaultErrorHandler;
//import org.springframework.util.backoff.ExponentialBackOff;
//import org.springframework.util.backoff.FixedBackOff;
//
//import java.util.*;
//
//@Slf4j
//@EnableKafka
//@Configuration
//@RequiredArgsConstructor
//public class KafkaConfiguration {
//
//    private final EnvConfiguration envConfiguration;
//
//    @Bean
//    public ConsumerFactory<String, String> consumerFactory() {
//        String bootstrapServers = envConfiguration.getKafkaBootstrapServers();
//        log.info("Configuring Kafka consumer with bootstrap servers: {}", bootstrapServers);
//
//        Map<String, Object> props = new HashMap<>();
//        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
//        props.put(ConsumerConfig.GROUP_ID_CONFIG, "authentication-service-group");
//        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "authentication-service-client");
//        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
//        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
//        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
//        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 50);
//        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);  // Augmenter à 30s
//        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10000);  // Augmenter à 10s
//        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1);
//        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);
//        props.put(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, 1000);  // Attendre 1s entre les tentatives
//        props.put(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, 1000);  // Attendre 1s avant de reconnecter
//        props.put(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, 10000);  // Max 10s
//        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);  // 5 min
//
//        return new DefaultKafkaConsumerFactory<>(props);
//    }
//
//    @Bean
//    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
//        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
//        factory.setConsumerFactory(consumerFactory());
//        factory.setConcurrency(1);
//        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
//        factory.getContainerProperties().setMissingTopicsFatal(false);
//        factory.getContainerProperties().setSyncCommits(true);
//
//        ExponentialBackOff backOff = new ExponentialBackOff(2000L, 2.0);
//        backOff.setMaxElapsedTime(60000L);
//
//        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
//                (record, exception) -> {
//                    log.error("Processing error: topic={}, partition={}, offset={}, exception={}",
//                            record.topic(), record.partition(), record.offset(), exception.getMessage(), exception);
//                },
//                backOff
//        );
//
//        errorHandler.addRetryableExceptions(org.apache.kafka.common.errors.CoordinatorNotAvailableException.class);
//
//        factory.setCommonErrorHandler(errorHandler);
//        return factory;
//    }
//
//    @Bean
//    public ApplicationRunner kafkaInit(EnvConfiguration envConfiguration) {
//        return args -> {
//            try {
//                Properties props = new Properties();
//                props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, envConfiguration.getKafkaBootstrapServers());
//                props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
//
//                try (AdminClient adminClient = AdminClient.create(props)) {
//                    // Vérifier si le topic existe
//                    Set<String> topics = adminClient.listTopics().names().get();
//                    log.info("Topics disponibles: {}", topics);
//
//                    // Forcer la création du topic s'il n'existe pas
//                    if (!topics.contains("authentication-service")) {
//                        log.info("Création du topic authentication-service");
//                        adminClient.createTopics(Collections.singletonList(
//                                new NewTopic("authentication-service", 3, (short) 1)
//                        )).all().get();
//                    } else {
//                        // Vérifier les partitions
//                        adminClient.describeTopics(Collections.singletonList("authentication-service"))
//                                .allTopicNames().get().forEach((name, desc) -> {
//                                    log.info("Topic {} a {} partitions", name, desc.partitions().size());
//                                    if (desc.partitions().size() < 3) {
//                                        try {
//                                            log.info("Mise à jour du nombre de partitions à 3");
//                                            Map<String, NewPartitions> newPartitions = new HashMap<>();
//                                            newPartitions.put("authentication-service", NewPartitions.increaseTo(3));
//                                            adminClient.createPartitions(newPartitions).all().get();
//                                        } catch (Exception e) {
//                                            log.error("Échec de la mise à jour des partitions: {}", e.getMessage());
//                                        }
//                                    }
//                                });
//                    }
//
//                    // Essayer d'initialiser le groupe de consommateurs
//                    log.info("Initialisation du groupe de consommateurs");
//                    // Cette opération forcera Kafka à créer le coordinateur de groupe
//                    adminClient.listConsumerGroups().valid().get().forEach(group -> {
//                        log.info("Groupe existant: {}", group.groupId());
//                    });
//                }
//            } catch (Exception e) {
//                log.error("Erreur lors de l'initialisation Kafka: {}", e.getMessage(), e);
//            }
//        };
//    }
//
//
//    @PostConstruct
//    public void logKafkaConfig() {
//        try {
//            Properties props = new Properties();
//            props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, envConfiguration.getKafkaBootstrapServers());
//            props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);
//
//            try (AdminClient adminClient = AdminClient.create(props)) {
//                Set<String> topics = adminClient.listTopics().names().get();
//
//                log.info("=== CONFIGURATION KAFKA ===");
//                log.info("Bootstrap servers: {}", envConfiguration.getKafkaBootstrapServers());
//                log.info("Topics disponibles: {}", topics);
//
//                if (topics.contains("authentication-service")) {
//                    log.info("Topic 'authentication-service' trouvé !");
//                    adminClient.listConsumerGroups().valid().get().forEach(group -> {
//                        log.info("Groupe consommateur: {}", group.groupId());
//                    });
//                } else {
//                    log.warn("Topic 'authentication-service' NON TROUVÉ! Topics existants: {}", topics);
//                }
//
//                log.info("=== FIN CONFIGURATION KAFKA ===");
//            }
//        } catch (Exception e) {
//            log.error("Erreur lors de la connexion à Kafka", e);
//        }
//    }
//
//}
package com.novus.authentication_service.configuration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

@EnableKafka
@Configuration
public class KafkaConfiguration {

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka.railway.internal:29092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "authentication-service-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        return factory;
    }
}