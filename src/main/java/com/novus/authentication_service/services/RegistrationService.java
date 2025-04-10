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

            emailService.sendEmail(user.getEmail(), "Confirm your Supmap account", getAccountRegistrationEmail(emailConfirmationToken, username));

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

            user.setLastActivityDate(new Date());
            userDaoUtils.save(user);

            String emailConfirmationToken = jwtTokenService.generateEmailConfirmationToken(userId);

            emailService.sendEmail(email, "Confirm your Supmap account", getAccountRegistrationEmail(emailConfirmationToken, user.getUsername()));

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

    public String getAccountRegistrationEmail(String emailConfirmationToken, String username) {
        String confirmationLink = envConfiguration.getMailRegisterConfirmationLink() + emailConfirmationToken;

        return "<!DOCTYPE html>\n" +
                "<html lang=\"en\">\n" +
                "<head>\n" +
                "    <meta charset=\"UTF-8\">\n" +
                "    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">\n" +
                "    <title>Welcome to SupMap!</title>\n" +
                "</head>\n" +
                "<body style=\"font-family: Arial, sans-serif; line-height: 1.6; color: #333; max-width: 600px; margin: 0 auto; padding: 20px;\">\n" +
                "    <div style=\"text-align: center; margin-bottom: 20px;\">\n" +
                "        <img src=\"https://i.ibb.co/NLf7Xgw/supmap-without-text.png\" alt=\"SupMap Logo\" style=\"max-width: 150px; height: auto;\">\n" +
                "    </div>\n" +
                "    <div style=\"background-color: #f9f9f9; padding: 20px; border-radius: 5px; border-left: 4px solid #4285f4;\">\n" +
                "        <h2 style=\"color: #4285f4; margin-top: 0;\">Welcome " + username + "!</h2>\n" +
                "        <p>Thank you for signing up for SupMap. We are thrilled to have you on board!</p>\n" +
                "        <p>To complete your registration and activate your account, please click the button below:</p>\n" +
                "        <div style=\"text-align: center; margin: 30px 0;\">\n" +
                "            <a href=\"" + confirmationLink + "\" style=\"background-color: #4285f4; color: white; padding: 12px 24px; text-decoration: none; border-radius: 4px; font-weight: bold; display: inline-block;\">Confirm my email</a>\n" +
                "        </div>\n" +
                "        <p>If the button does not work, you can also copy and paste the following link into your browser:</p>\n" +
                "        <p style=\"background-color: #f0f0f0; padding: 10px; border-radius: 5px; word-break: break-all;\"><a href=\"" + confirmationLink + "\" style=\"color: #4285f4; text-decoration: none;\">" + confirmationLink + "</a></p>\n" +
                "        <p><strong>Important:</strong> This link will expire in 48 hours for security reasons.</p>\n" +
                "        <p>With SupMap, you will be able to:</p>\n" +
                "        <ul style=\"background-color: #fff; padding: 15px; border-radius: 5px; margin: 15px 0; border: 1px solid #ddd;\">\n" +
                "            <li>Simplify your route management</li>\n" +
                "            <li>Optimize your mapping projects</li>\n" +
                "            <li>Access exclusive features</li>\n" +
                "            <li>Collaborate with your team</li>\n" +
                "        </ul>\n" +
                "        <p>If you did not create an account on SupMap, please ignore this email.</p>\n" +
                "    </div>\n" +
                "    <div style=\"margin-top: 30px; font-size: 14px; color: #666; border-top: 1px solid #ddd; padding-top: 20px;\">\n" +
                "        <p>Best regards,<br>\n" +
                "        The SupMap Team</p>\n" +
                "        <div style=\"margin-top: 15px;\">\n" +
                "            <p>SupMap - Simplify your routes and projects.</p>\n" +
                "            <p>📞 Support: <a href=\"tel:+33614129625\" style=\"color: #4285f4; text-decoration: none;\">+33 6 14 12 96 25</a><br>\n" +
                "            📩 Email: <a href=\"mailto:supmap.application@gmail.com\" style=\"color: #4285f4; text-decoration: none;\">supmap.application@gmail.com</a><br>\n" +
                "            🌐 Website: <a href=\"https://supmap-application.com\" style=\"color: #4285f4; text-decoration: none;\">https://supmap-application.com</a><br>\n" +
                "            📱 Available on iOS and Android!</p>\n" +
                "        </div>\n" +
                "    </div>\n" +
                "</body>\n" +
                "</html>";
    }

}
