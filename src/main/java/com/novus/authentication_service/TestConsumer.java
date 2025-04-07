package com.novus.authentication_service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class TestConsumer {

    @KafkaListener(
            topics = "authentication-service",
            groupId = "authentication-service-group"
    )
    public void listen(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            @Header(KafkaHeaders.RECEIVED_PARTITION) Integer partition
    ) {
        log.info("Received message: topic={}, partition={}, key={}", topic, partition, key);
        log.info("Message content: {}", message);

        // Traiter le message ici
        processMessage(key, message);
    }

    private void processMessage(String key, String message) {
        // Simple exemple de traitement selon la clé
        switch (key) {
            case "register":
                log.info("Processing registration message");
                break;
            case "login":
                log.info("Processing login message");
                break;
            default:
                log.info("Processing unknown message type: {}", key);
                break;
        }
    }
}