package com.novus.authentication_service.services;

import com.novus.authentication_service.UuidProvider;
import com.novus.authentication_service.configuration.EnvConfiguration;
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
import java.util.Date;
import java.util.Map;
import java.util.Optional;

import static com.novus.authentication_service.services.EmailService.getEmailSignature;

@Slf4j
@Service
@RequiredArgsConstructor
public class RegistrationService {

    private final UserDaoUtils userDaoUtils;
    private final LogUtils logUtils;
    private final UuidProvider uuidProvider;
    private final EmailService emailService;
    private final JwtTokenService jwtTokenService;
    private final EnvConfiguration envConfiguration;

    public void processRegister(KafkaMessage kafkaMessage) {
        Map<String, String> request = kafkaMessage.getRequest();
        String username = request.get("username");
        String email = request.get("email");
        String encodedPassword = request.get("password");

        try {
            User user = User.builder()
                    .id(uuidProvider.generateUuid())
                    .email(email)
                    .password(encodedPassword)
                    .username(username)
                    .build();

            userDaoUtils.save(user);

            logUtils.buildAndSaveLog(
                    LogLevel.INFO,
                    "REGISTER_SUCCESS",
                    kafkaMessage.getIpAddress(),
                    "User successfully registered: " + email,
                    HttpMethod.POST,
                    "/auth/register",
                    "authentication-service",
                    null,
                    user.getId()
            );

            String emailConfirmationToken = jwtTokenService.generateEmailConfirmationToken(user.getId());

            emailService.sendEmail(user.getEmail(), "Confirm your Supmap account", getRegisterEmailBody(emailConfirmationToken, username));

            logUtils.buildAndSaveLog(
                    LogLevel.INFO,
                    "CONFIRMATION_EMAIL_SENT",
                    kafkaMessage.getIpAddress(),
                    "Confirmation email sent to: " + email,
                    HttpMethod.POST,
                    "/auth/register",
                    "authentication-service",
                    null,
                    user.getId()
            );

        } catch (Exception e) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            String stackTrace = sw.toString();

            logUtils.buildAndSaveLog(
                    LogLevel.ERROR,
                    "REGISTER_FAILED",
                    kafkaMessage.getIpAddress(),
                    "Registration failed for " + email + ": " + e.getMessage(),
                    HttpMethod.POST,
                    "/auth/register",
                    "authentication-service",
                    stackTrace,
                    null
            );

            throw new RuntimeException("Failed to process registration: " + e.getMessage(), e);
        }
    }

    public void processConfirmEmail(KafkaMessage kafkaMessage) {
        Map<String, String> request = kafkaMessage.getRequest();
        String userId = request.get("userId");
        String email = request.get("email");

        Optional<User> optionalUser = userDaoUtils.findById(userId);

        if (optionalUser.isEmpty()) {
            logUtils.buildAndSaveLog(
                    LogLevel.ERROR,
                    "EMAIL_CONFIRMATION_FAILED",
                    kafkaMessage.getIpAddress(),
                    "User not found for email confirmation: " + email,
                    HttpMethod.GET,
                    "/auth/confirm-email",
                    "authentication-service",
                    null,
                    userId
            );
            throw new RuntimeException("User not found for email confirmation with ID: " + userId);
        }

        User user = optionalUser.get();

        user.setValidEmail(true);
        user.setUpdatedAt(new Date());
        user.setLastActivityDate(new Date());
        userDaoUtils.save(user);

        logUtils.buildAndSaveLog(
                LogLevel.INFO,
                "EMAIL_CONFIRMATION_SUCCESS",
                kafkaMessage.getIpAddress(),
                "Email confirmed successfully for user: " + email,
                HttpMethod.GET,
                "/auth/confirm-email",
                "authentication-service",
                null,
                userId
        );
    }

    public void processResendRegisterConfirmationEmail(KafkaMessage kafkaMessage) {
        Map<String, String> request = kafkaMessage.getRequest();
        String userId = request.get("userId");
        String email = request.get("email");

        try {
            Optional<User> optionalUser = userDaoUtils.findById(userId);

            if (optionalUser.isEmpty()) {
                logUtils.buildAndSaveLog(
                        LogLevel.ERROR,
                        "RESEND_CONFIRMATION_FAILED",
                        kafkaMessage.getIpAddress(),
                        "User not found for resending confirmation email: " + email,
                        HttpMethod.POST,
                        "/auth/resend-confirmation",
                        "authentication-service",
                        null,
                        userId
                );
                throw new RuntimeException("User not found for resending confirmation email with ID: " + userId);
            }

            User user = optionalUser.get();

            String emailConfirmationToken = jwtTokenService.generateEmailConfirmationToken(userId);

            emailService.sendEmail(email, "Confirm your Supmap account", getRegisterEmailBody(emailConfirmationToken, user.getUsername()));

            logUtils.buildAndSaveLog(
                    LogLevel.INFO,
                    "CONFIRMATION_EMAIL_RESENT",
                    kafkaMessage.getIpAddress(),
                    "Confirmation email resent successfully to: " + email,
                    HttpMethod.POST,
                    "/auth/resend-confirmation",
                    "authentication-service",
                    null,
                    userId
            );

            log.info("Confirmation email resent successfully to: {}", email);

        } catch (Exception e) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            String stackTrace = sw.toString();

            logUtils.buildAndSaveLog(
                    LogLevel.ERROR,
                    "RESEND_CONFIRMATION_ERROR",
                    kafkaMessage.getIpAddress(),
                    "Error resending confirmation email to: " + email + ", error: " + e.getMessage(),
                    HttpMethod.POST,
                    "/auth/resend-confirmation",
                    "authentication-service",
                    stackTrace,
                    userId
            );

            throw new RuntimeException("Failed to resend confirmation email: " + e.getMessage(), e);
        }
    }

    private String getRegisterEmailBody(String emailConfirmationToken, String username) {
        String confirmationLink = envConfiguration.getMailRegisterConfirmationLink() + emailConfirmationToken;

        return "<html>"
                + "<body>"
                + "<h2>Bienvenue " + username + " !</h2>"
                + "<p>Merci de vous être inscrit sur notre application.</p>"
                + "<p>Pour activer votre compte, veuillez cliquer sur le lien suivant :</p>"
                + "<p><a href=\"" + confirmationLink + "\">Confirmer mon email</a></p>"
                + "<p>Si vous n'avez pas créé de compte, veuillez ignorer cet email.</p>"
                + getEmailSignature()
                + "</body>"
                + "</html>";
    }

}
