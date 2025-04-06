package com.novus.authentication_service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.novus.authentication_service.services.LoginService;
import com.novus.authentication_service.services.PasswordService;
import com.novus.authentication_service.services.RegistrationService;
import com.novus.shared_models.common.Kafka.KafkaMessage;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Service
@EnableScheduling
@RequiredArgsConstructor
public class Consumer {

    private final KafkaConsumer<String, String> kafkaConsumer;
    private final ObjectMapper objectMapper;
    private final LoginService loginService;
    private final PasswordService passwordService;
    private final RegistrationService registrationService;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private Thread consumerThread;

    @PostConstruct
    public void startConsumer() {
        log.info("Starting Kafka consumer thread");
        running.set(true);
        consumerThread = new Thread(() -> {
            try {
                log.info("Consumer thread initializing");
                log.info("Consumer subscriptions: {}", kafkaConsumer.subscription());
                consume();
            } catch (Exception e) {
                log.error("Error in consumer thread initialization: {}", e.getMessage(), e);
            }
        });
        consumerThread.setName("kafka-consumer-thread");
        consumerThread.start();
        log.info("🚀 Authentication Service Kafka Consumer started on thread: {}", consumerThread.getName());
    }

    @PreDestroy
    public void stopConsumer() {
        running.set(false);
        if (consumerThread != null) {
            consumerThread.interrupt();
        }
        kafkaConsumer.close();
        log.info("🛑 Authentication Service Kafka Consumer stopped");
    }

    public void consume() {
        log.info("Consumer thread started, beginning to poll for messages");
        try {
            while (running.get()) {
                log.debug("Polling for messages...");
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
                log.debug("Poll completed. Records received: {}", records.count());

                for (ConsumerRecord<String, String> record : records) {
                    log.info("Received message: topic={}, partition={}, offset={}, key={}",
                            record.topic(), record.partition(), record.offset(), record.key());
                    processMessage(record.key(), record.value());
                }

                if (records.count() > 0) {
                    log.debug("Committing offsets...");
                    kafkaConsumer.commitSync();
                    log.debug("Offsets committed");
                }
            }
        } catch (Exception e) {
            log.error("Error in Kafka consumer: {}", e.getMessage(), e);
        }
        log.info("Consumer thread stopped");
    }

    @Scheduled(fixedRate = 60000)
    public void checkConsumerHealth() {
        if (consumerThread != null && !consumerThread.isAlive()) {
            log.warn("Consumer thread is not alive! Restarting...");
            stopConsumer();
            startConsumer();
        } else {
            log.info("Consumer thread is healthy. Subscribed to: {}", kafkaConsumer.subscription());
        }
    }

    private void processMessage(String key, String value) {
        try {
            log.info("Processing message with key: {}", key);
            KafkaMessage kafkaMessage = objectMapper.readValue(value, KafkaMessage.class);

            switch (key) {
                case "register":
                    registrationService.processRegister(kafkaMessage);
                    break;
                case "confirmEmail":
                    registrationService.processConfirmEmail(kafkaMessage);
                    break;
                case "resendRegisterConfirmationEmail":
                    registrationService.processResendRegisterConfirmationEmail(kafkaMessage);
                    break;
                case "login":
                    loginService.processLogin(kafkaMessage);
                    break;
                case "googleLogin":
                    loginService.processGoogleLogin(kafkaMessage);
                    break;
                case "sendForgotPasswordEmail":
                    passwordService.processSendForgotPasswordEmail(kafkaMessage);
                    break;
                case "resetPassword":
                    passwordService.processResetPassword(kafkaMessage);
                    break;
                default:
                    log.warn("Unknown message key: {}", key);
                    break;
            }
        } catch (Exception e) {
            log.error("Error processing message with key {}: {}", key, e.getMessage(), e);
        }
    }

}
