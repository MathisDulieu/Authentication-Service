package com.novus.authentication_service;

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

    @KafkaListener(
            topics = "authentication-service",
            groupId = "authentication-groupId",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void listen(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment
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

            // Acquittement manuel du message
            acknowledgment.acknowledge();
            logger.info("Message acquitté avec succès - Offset: {}", offset);
        } catch (Exception e) {
            logger.error("ERREUR lors du traitement du message: {}", e.getMessage(), e);
            // Vous pourriez décider de ne pas acquitter le message en cas d'erreur
            // pour qu'il soit relivré, selon votre stratégie de gestion des erreurs
        }
    }

    private void processMessage(String key, String message) {
        logger.info("Début du traitement du message - Clé: {}", key);

        // Votre logique de traitement ici

        logger.info("Fin du traitement du message - Clé: {}", key);
    }
}
