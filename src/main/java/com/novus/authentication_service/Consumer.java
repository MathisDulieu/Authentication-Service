package com.novus.authentication_service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.novus.authentication_service.services.LoginService;
import com.novus.authentication_service.services.PasswordService;
import com.novus.authentication_service.services.RegistrationService;
import com.novus.shared_models.common.Kafka.KafkaMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class Consumer {

    @Autowired
    private ObjectMapper objectMapper;

    private final LoginService loginService;
    private final PasswordService passwordService;
    private final RegistrationService registrationService;

    @KafkaListener(topics = "authentication-service", groupId = "${spring.kafka.consumer.group-id}")
    public void consumeAuthenticationEvents(
            @Payload String messageJson,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment) {

        try {
            log.info("JSON message received from authentication-service topic [key: {}, partition: {}, offset: {}]", key, partition, offset);

            KafkaMessage kafkaMessage = objectMapper.readValue(messageJson, KafkaMessage.class);

            handleOperation(key, kafkaMessage);

            acknowledgment.acknowledge();
        } catch (Exception e) {
            log.error("Error processing message: {}", e.getMessage(), e);
            acknowledgment.acknowledge();
        }
    }

    private void handleOperation(String operationKey, KafkaMessage kafkaMessage) {
        log.info("Processing operation: {}", operationKey);

        switch (operationKey) {
            case "register":
                registrationService.processRegister(kafkaMessage);
                break;
            case "login":
                loginService.processLogin(kafkaMessage);
                break;
            case "confirmEmail":
                registrationService.processConfirmEmail(kafkaMessage);
                break;
            case "resendRegisterConfirmationEmail":
                registrationService.processResendRegisterConfirmationEmail(kafkaMessage);
                break;
            case "sendForgotPasswordEmail":
                passwordService.processSendForgotPasswordEmail(kafkaMessage);
                break;
            case "resetPassword":
                passwordService.processResetPassword(kafkaMessage);
                break;
            case "googleLogin":
                loginService.processLogin(kafkaMessage);
                break;
            default:
                log.warn("Unknown operation: {}", operationKey);
        }
    }
}