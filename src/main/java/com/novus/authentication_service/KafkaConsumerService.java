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
public class KafkaConsumerService {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerService.class);

    @Autowired
    private ObjectMapper objectMapper;

    @KafkaListener(topics = "authentication-service", groupId = "${spring.kafka.consumer.group-id}")
    public void consumeNavigationEvents(
            @Payload KafkaMessage kafkaMessage,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment) {

        try {
            logger.info("Événement reçu du topic navigation-service [partition: {}, offset: {}]: {}",
                    partition, offset, kafkaMessage);

            acknowledgment.acknowledge();
        } catch (Exception e) {
            logger.error("Erreur lors du traitement de l'événement: {}", e.getMessage(), e);
            acknowledgment.acknowledge();
        }
    }
}