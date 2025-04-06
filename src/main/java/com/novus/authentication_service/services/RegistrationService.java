package com.novus.authentication_service.services;

import com.novus.shared_models.common.Kafka.KafkaMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class RegistrationService {

    public void processRegister(KafkaMessage kafkaMessage) {
        log.info("Processing register request");

        Map<String, String> request = kafkaMessage.getRequest();
        String username = request.get("username");
        String email = request.get("email");
        String encodedPassword = request.get("password");

        log.info("User registered successfully: {}", email);
    }

    public void processConfirmEmail(KafkaMessage kafkaMessage) {
        log.info("Processing email confirmation");

        Map<String, String> request = kafkaMessage.getRequest();
        String userId = request.get("userId");
        String email = request.get("email");

        log.info("Email confirmed successfully for user ID: {}", userId);
    }

    public void processResendRegisterConfirmationEmail(KafkaMessage kafkaMessage) {
        log.info("Processing resend confirmation email request");

        Map<String, String> request = kafkaMessage.getRequest();
        String userId = request.get("userId");
        String email = request.get("email");

        log.info("Confirmation email resent to: {}", email);
    }

}
