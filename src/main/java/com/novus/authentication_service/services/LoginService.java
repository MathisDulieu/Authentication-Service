package com.novus.authentication_service.services;

import com.novus.authentication_service.configuration.DateConfiguration;
import com.novus.authentication_service.dao.UserDaoUtils;
import com.novus.authentication_service.utils.LogUtils;
import com.novus.shared_models.common.Kafka.KafkaMessage;
import com.novus.shared_models.common.Log.HttpMethod;
import com.novus.shared_models.common.Log.LogLevel;
import com.novus.shared_models.common.User.User;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;
import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class LoginService {

    private final UserDaoUtils userDaoUtils;
    private final LogUtils logUtils;
    private final DateConfiguration dateConfiguration;

    public void processLogin(KafkaMessage kafkaMessage) {
        Map<String, String> request = kafkaMessage.getRequest();
        String email = request.get("email");
        String userId = request.get("userId");
        log.info("Starting to process login request for user with email: {} and ID: {}", email, userId);

        try {
            Optional<User> optionalUser = userDaoUtils.findById(userId);

            if (optionalUser.isEmpty()) {
                logUtils.buildAndSaveLog(
                        LogLevel.ERROR,
                        "LOGIN_FAILED",
                        kafkaMessage.getIpAddress(),
                        "User not found during login process: " + email,
                        HttpMethod.POST,
                        "/auth/login",
                        "authentication-service",
                        null,
                        userId
                );
                throw new RuntimeException("User not found during login process with ID: " + userId);
            }

            User user = optionalUser.get();

            logUtils.buildAndSaveLog(
                    LogLevel.INFO,
                    "LOGIN_SUCCESS",
                    kafkaMessage.getIpAddress(),
                    "User logged in successfully: " + email,
                    HttpMethod.POST,
                    "/auth/login",
                    "authentication-service",
                    null,
                    userId
            );

            user.setLastLoginDate(dateConfiguration.newDate());
            user.setLastActivityDate(dateConfiguration.newDate());
            userDaoUtils.save(user);
            log.info("Login successfully processed for user with email: {} and ID: {}", email, userId);
        } catch (Exception e) {
            log.error("Error occurred while processing login request: {}", e.getMessage());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            String stackTrace = sw.toString();

            logUtils.buildAndSaveLog(
                    LogLevel.ERROR,
                    "LOGIN_PROCESS_ERROR",
                    kafkaMessage.getIpAddress(),
                    "Error processing login for user: " + email + ", error: " + e.getMessage(),
                    HttpMethod.POST,
                    "/auth/login",
                    "authentication-service",
                    stackTrace,
                    userId
            );

            throw new RuntimeException("Failed to process login: " + e.getMessage(), e);
        }
    }

    public void processGoogleLogin(KafkaMessage kafkaMessage) {
        Map<String, String> request = kafkaMessage.getRequest();
        String userId = request.get("userId");
        log.info("Starting to process Google login request for user with ID: {}", userId);

        try {
            Optional<User> optionalUser = userDaoUtils.findById(userId);

            if (optionalUser.isEmpty()) {
                log.error("User with ID: {} not found during Google login process", userId);
                logUtils.buildAndSaveLog(
                        LogLevel.ERROR,
                        "GOOGLE_LOGIN_FAILED",
                        kafkaMessage.getIpAddress(),
                        "User not found during Google login process with ID: " + userId,
                        HttpMethod.POST,
                        "/oauth/google-login",
                        "authentication-service",
                        null,
                        userId
                );
                throw new RuntimeException("User not found during Google login process with ID: " + userId);
            }

            User user = optionalUser.get();

            logUtils.buildAndSaveLog(
                    LogLevel.INFO,
                    "GOOGLE_LOGIN_SUCCESS",
                    kafkaMessage.getIpAddress(),
                    "User logged in successfully via Google: " + user.getEmail(),
                    HttpMethod.POST,
                    "/oauth/google-login",
                    "authentication-service",
                    null,
                    userId
            );

            user.setLastLoginDate(dateConfiguration.newDate());
            user.setLastActivityDate(dateConfiguration.newDate());
            userDaoUtils.save(user);
            log.info("Google login successfully processed for user with ID: {} and email: {}", userId, user.getEmail());
        } catch (Exception e) {
            log.error("Error occurred while processing Google login request: {}", e.getMessage());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            String stackTrace = sw.toString();

            logUtils.buildAndSaveLog(
                    LogLevel.ERROR,
                    "GOOGLE_LOGIN_PROCESS_ERROR",
                    kafkaMessage.getIpAddress(),
                    "Error processing Google login for user ID: " + userId + ", error: " + e.getMessage(),
                    HttpMethod.POST,
                    "/oauth/google-login",
                    "authentication-service",
                    stackTrace,
                    userId
            );
            throw new RuntimeException("Failed to process Google login: " + e.getMessage(), e);
        }
    }

}
