package com.novus.authentication_service;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
public class KafkaSanityCheck {

    private static final Logger logger = LoggerFactory.getLogger(KafkaSanityCheck.class);

    private static final String bootstrapServers = "kafka.railway.internal:29092";

    @EventListener
    public void onApplicationEvent(ContextRefreshedEvent event) {
        logger.info("Vérification de la connexion Kafka au démarrage");

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);

        try (AdminClient adminClient = AdminClient.create(props)) {
            ListTopicsResult topics = adminClient.listTopics();
            Set<String> topicNames = topics.names().get(30, TimeUnit.SECONDS);

            logger.info("Connexion Kafka établie avec succès!");
            logger.info("Topics disponibles: {}", topicNames);

            boolean hasAuthenticationTopic = topicNames.contains("authentication-service");
            if (hasAuthenticationTopic) {
                logger.info("Le topic 'authentication-service' existe et est accessible");
            } else {
                logger.warn("Le topic 'authentication-service' n'existe pas ou n'est pas accessible!");
            }

        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            logger.error("Échec de la connexion à Kafka: {}", e.getMessage(), e);
            logger.error("Bootstrap servers: {}", bootstrapServers);
            logger.error("Vérifiez la configuration Kafka et les paramètres réseau");
        }
    }
}
