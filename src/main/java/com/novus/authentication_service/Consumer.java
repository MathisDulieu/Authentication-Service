package com.novus.authentication_service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.novus.authentication_service.services.LoginService;
import com.novus.authentication_service.services.PasswordService;
import com.novus.authentication_service.services.RegistrationService;
import com.novus.shared_models.common.Kafka.KafkaMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.logging.Logger;

@Slf4j
@Service
@RequiredArgsConstructor
public class Consumer {

    private final ObjectMapper objectMapper;
    private final LoginService loginService;
    private final PasswordService passwordService;
    private final RegistrationService registrationService;

    @KafkaListener(
            topics = "${kafka.topics.authentication-service:authentication-service}",
            groupId = "${kafka.consumer.group-id:authentication-service-group}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeAuthenticationMessages(
            @Payload String value,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) Integer partition,
            @Header(KafkaHeaders.RECEIVED_TIMESTAMP) Long timestamp,
            Acknowledgment acknowledgment
    ) {
        try {
            log.info("Received message: topic={}, partition={}, key={}, timestamp={}",
                    topic, partition, key, timestamp);

            KafkaMessage kafkaMessage = objectMapper.readValue(value, KafkaMessage.class);

            log.info("Message deserialized successfully for key: {}", key);
            processMessage(key, kafkaMessage);

            // Acknowledge the message after successful processing
            acknowledgment.acknowledge();
            log.info("Message processed and acknowledged: key={}", key);

        } catch (Exception e) {
            log.error("Error processing message with key {}: {}", key, e.getMessage(), e);
            // Still acknowledge the message to prevent retries that will likely fail again
            // You might want to implement a dead letter queue pattern for failed messages
            acknowledgment.acknowledge();
            log.info("Message with error acknowledged (will not retry): key={}", key);
        }
    }

    private void processMessage(String key, KafkaMessage kafkaMessage) {
        log.info("Processing message with key: {}", key);

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
    }

}
