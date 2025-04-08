package com.novus.authentication_service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class KafkaListeners {

    private static final Logger logger = LoggerFactory.getLogger(KafkaListeners.class);

    @KafkaListener(
            topics = "authenticationTopic",
            groupId = "groupId",
            containerFactory = "kafkaListenerContainerFactory"
    )
    void listener(String data, Acknowledgment acknowledgment) {
        try {
            logger.info("Message reçu: {}", data);
            System.out.println("Data received: " + data);

            acknowledgment.acknowledge();

            logger.info("Message traité avec succès et confirmé");
        } catch (Exception e) {
            logger.error("Erreur lors du traitement du message: {}", e.getMessage(), e);
        }
    }
}
