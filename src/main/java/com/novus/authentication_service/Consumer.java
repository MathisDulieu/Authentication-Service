package com.novus.authentication_service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.novus.shared_models.common.Kafka.KafkaMessage;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.Map;

@Slf4j
@Service
public class Consumer {

    @Autowired
    private ObjectMapper objectMapper;

    @KafkaListener(topics = "authentication-service", groupId = "${spring.kafka.consumer.group-id}")
    public void consumeAuthenticationEvents(
            @Payload KafkaMessage kafkaMessage,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment) {

        try {
            log.info("Événement reçu du topic authentication-service [partition: {}, offset: {}]: {}",
                    partition, offset, kafkaMessage);

            Map<String, String> request = kafkaMessage.getRequest();
            if (request != null) {
                String operation = request.get("operation");
                switch (operation) {
                    case "login":
                        break;
                    case "register":
                        break;
                    case "validateToken":
                        break;
                    default:
                        log.warn("Opération non reconnue: {}", operation);
                }
            }

            acknowledgment.acknowledge();

        } catch (Exception e) {
            log.error("Erreur lors du traitement de l'événement: {}", e.getMessage(), e);
        }
    }
}