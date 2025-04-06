package com.novus.authentication_service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.novus.authentication_service.services.LoginService;
import com.novus.authentication_service.services.PasswordService;
import com.novus.authentication_service.services.RegistrationService;
import com.novus.shared_models.common.Kafka.KafkaMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class Consumer {

    private final ObjectMapper objectMapper;
    private final LoginService loginService;
    private final PasswordService passwordService;
    private final RegistrationService registrationService;

    @KafkaListener(
            topics = "authentication-service",
            groupId = "authentication-service-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(@Payload String message, @Header(KafkaHeaders.RECEIVED_KEY) String key) {
        try {
            log.info("Received message with key: {}", key);
            KafkaMessage kafkaMessage = objectMapper.readValue(message, KafkaMessage.class);

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
