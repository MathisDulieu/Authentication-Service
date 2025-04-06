package com.novus.authentication_service.services;

import com.novus.shared_models.common.Kafka.KafkaMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class PasswordService {

    public void processSendForgotPasswordEmail(KafkaMessage kafkaMessage) {
        log.info("Processing forgot password request");

        Map<String, String> request = kafkaMessage.getRequest();
        String userId = request.get("userId");
        String email = request.get("email");

        log.info("Password reset email sent to: {}", email);
    }

    public void processResetPassword(KafkaMessage kafkaMessage) {
        log.info("Processing password reset");

        Map<String, String> request = kafkaMessage.getRequest();
        String userId = request.get("userId");

        log.info("Password reset successfully for user ID: {}", userId);
    }

}
