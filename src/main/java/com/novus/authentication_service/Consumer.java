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

    @KafkaListener(topics = "authentication-service", groupId = "authentication-groupId")
    public void listen(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment
    ) {
        logger.info("Message reçu - Topic: {}, Partition: {}, Offset: {}, Clé: {}, Message: {}",
                topic, partition, offset, key, message);

        try {
            processMessage(key, message);

            acknowledgment.acknowledge();
            logger.info("Message traité et acquitté");
        } catch (Exception e) {
            logger.error("Erreur lors du traitement du message: {}", e.getMessage(), e);
        }
    }

    private void processMessage(String key, String message) {
        logger.info("Traitement du message - Clé: {}, Contenu: {}", key, message);
    }

}
