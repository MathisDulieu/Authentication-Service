package com.novus.authentication_service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaListeners {
    private static final Logger logger = LoggerFactory.getLogger(KafkaListeners.class);

    @KafkaListener(topics = "authenticationTopic", groupId = "authenticationTopic-group")
    void listener(String data) {
        try {
            logger.info("Message reçu: {}", data);
            System.out.println("Data received: " + data);
            logger.info("Message traité avec succès");
        } catch (Exception e) {
            logger.error("Erreur lors du traitement du message: {}", e.getMessage(), e);
        }
    }
}