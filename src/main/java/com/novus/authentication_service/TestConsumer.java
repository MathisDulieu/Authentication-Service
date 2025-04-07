package com.novus.authentication_service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class TestConsumer {

    private static final Logger log = LoggerFactory.getLogger(TestConsumer.class);

    @KafkaListener(topics = "authentication-service", groupId = "test-group-explicit")
    public void listen(String message) {
        log.info("TEST CONSUMER RECEIVED MESSAGE: {}", message);
    }
}