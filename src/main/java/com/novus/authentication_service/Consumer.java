package com.novus.authentication_service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.novus.shared_models.common.Kafka.KafkaMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
public class Consumer {
    private static final Logger logger = LoggerFactory.getLogger(Consumer.class);

    @Autowired
    private ObjectMapper objectMapper;

    @KafkaListener(topics = "authentication-service", groupId = "${spring.kafka.consumer.group-id}")
    public void consumeAuthenticationEvents(
            @Payload String messageJson,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment) {

        try {
            logger.info("Message JSON reçu du topic authentication-service [partition: {}, offset: {}]",
                    partition, offset);

            KafkaMessage kafkaMessage = objectMapper.readValue(messageJson, KafkaMessage.class);

            logger.info("Message désérialisé: {}", kafkaMessage);

            if (kafkaMessage.getRequest() != null) {
                String operation = kafkaMessage.getRequest().get("operation");
                if (operation != null) {
                    switch (operation) {
                        case "login":
                            // Traitement login
                            break;
                        case "register":
                            // Traitement register
                            break;
                        default:
                            logger.warn("Opération inconnue: {}", operation);
                    }
                }
            }

            acknowledgment.acknowledge();

        } catch (Exception e) {
            logger.error("Erreur lors du traitement du message: {}", e.getMessage(), e);
            acknowledgment.acknowledge();
        }
    }
}