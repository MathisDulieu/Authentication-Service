package com.novus.authentication_service.services;

import com.novus.shared_models.common.Kafka.KafkaMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class LoginService {

    public void processLogin(KafkaMessage kafkaMessage) {
        log.info("Processing login request");

        Map<String, String> request = kafkaMessage.getRequest();
        String email = request.get("email");
        String userId = request.get("userId");

        log.info("User logged in successfully: {}", email);
    }

    public void processGoogleLogin(KafkaMessage kafkaMessage) {
        log.info("Processing Google login request");

        Map<String, String> request = kafkaMessage.getRequest();
        String userId = request.get("userId");

        log.info("User logged in successfully via Google, userId: {}", userId);
    }

}
