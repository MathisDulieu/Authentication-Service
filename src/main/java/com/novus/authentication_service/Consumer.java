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
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Service
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
        running.set(true);
        consumerThread = new Thread(this::consume);
        consumerThread.start();
        log.info("🚀 Authentication Service Kafka Consumer started");
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

    @Async("consumerTaskExecutor")
    public void consume() {
        try {
            while (running.get()) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    processMessage(record.key(), record.value());
                }
                kafkaConsumer.commitSync();
            }
        } catch (Exception e) {
            log.error("Error in Kafka consumer: {}", e.getMessage(), e);
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
