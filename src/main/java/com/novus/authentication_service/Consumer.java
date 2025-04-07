package com.novus.authentication_service;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class Consumer {

    private static final Logger logger = LoggerFactory.getLogger(Consumer.class);

    @PostConstruct
    public void init() {
        logger.info("Initialisation du consumer Kafka - prêt à recevoir des messages");
    }

    @KafkaListener(
            topics = "authentication-service",
            groupId = "authentication-groupId",
            containerFactory = "kafkaListenerContainerFactory",
            autoStartup = "true"
    )
    public void listen(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset
    ) {
        logger.info("=======================================================");
        logger.info("MESSAGE REÇU - Détails:");
        logger.info("Topic: {}", topic);
        logger.info("Partition: {}", partition);
        logger.info("Offset: {}", offset);
        logger.info("Clé: {}", key);
        logger.info("Contenu: {}", message);
        logger.info("=======================================================");

        try {
            processMessage(key, message);
            logger.info("Message traité avec succès - Offset: {}", offset);
        } catch (Exception e) {
            logger.error("ERREUR lors du traitement du message: {}", e.getMessage(), e);
        }
    }

    private void processMessage(String key, String message) {
        logger.info("Début du traitement du message - Clé: {}", key);

        // Votre logique de traitement ici

        logger.info("Fin du traitement du message - Clé: {}", key);
    }
}
