package com.novus.authentication_service.services;

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
public class PasswordService {

    private final LogUtils logUtils;
    private final UserDaoUtils userDaoUtils;
    private final EmailService emailService;
    private final JwtTokenService jwtTokenService;
    private final EnvConfiguration envConfiguration;

    public void processSendForgotPasswordEmail(KafkaMessage kafkaMessage) {
        Map<String, String> request = kafkaMessage.getRequest();
        String userId = request.get("userId");
        String email = request.get("email");

        try {
            Optional<User> optionalUser = userDaoUtils.findById(userId);

            if (optionalUser.isEmpty()) {
                logUtils.buildAndSaveLog(
                        LogLevel.ERROR,
                        "FORGOT_PASSWORD_FAILED",
                        kafkaMessage.getIpAddress(),
                        "User not found when sending password reset email: " + email,
                        HttpMethod.POST,
                        "/auth/forgot-password",
                        "authentication-service",
                        null,
                        userId
                );
                throw new RuntimeException("User not found when sending password reset email with ID: " + userId);
            }

            User user = optionalUser.get();

            String passwordResetToken = jwtTokenService.generatePasswordResetToken(userId);

            emailService.sendEmail(email, "Reset your Supmap password", getPasswordResetEmailBody(passwordResetToken, user.getUsername()));

            logUtils.buildAndSaveLog(
                    LogLevel.INFO,
                    "PASSWORD_RESET_EMAIL_SENT",
                    kafkaMessage.getIpAddress(),
                    "Password reset email sent to: " + email,
                    HttpMethod.POST,
                    "/auth/forgot-password",
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
                    "FORGOT_PASSWORD_ERROR",
                    kafkaMessage.getIpAddress(),
                    "Error sending password reset email to: " + email + ", error: " + e.getMessage(),
                    HttpMethod.POST,
                    "/auth/forgot-password",
                    "authentication-service",
                    stackTrace,
                    userId
            );

            throw new RuntimeException("Failed to send password reset email: " + e.getMessage(), e);
        }
    }

    public void processResetPassword(KafkaMessage kafkaMessage) {
        Map<String, String> request = kafkaMessage.getRequest();
        String userId = request.get("userId");
        String newPassword = request.get("newPassword");

        try {
            Optional<User> optionalUser = userDaoUtils.findById(userId);

            if (optionalUser.isEmpty()) {
                logUtils.buildAndSaveLog(
                        LogLevel.ERROR,
                        "PASSWORD_RESET_FAILED",
                        kafkaMessage.getIpAddress(),
                        "User not found when resetting password with ID: " + userId,
                        HttpMethod.POST,
                        "/auth/reset-password",
                        "authentication-service",
                        null,
                        userId
                );
                throw new RuntimeException("User not found when resetting password with ID: " + userId);
            }

            User user = optionalUser.get();

            user.setPassword(newPassword);
            user.setUpdatedAt(new Date());
            user.setLastActivityDate(new Date());

            userDaoUtils.save(user);

            logUtils.buildAndSaveLog(
                    LogLevel.INFO,
                    "PASSWORD_RESET_SUCCESS",
                    kafkaMessage.getIpAddress(),
                    "Password reset successfully for user: " + user.getEmail(),
                    HttpMethod.POST,
                    "/auth/reset-password",
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
                    "PASSWORD_RESET_ERROR",
                    kafkaMessage.getIpAddress(),
                    "Error resetting password for user ID: " + userId + ", error: " + e.getMessage(),
                    HttpMethod.POST,
                    "/auth/reset-password",
                    "authentication-service",
                    stackTrace,
                    userId
            );

            throw new RuntimeException("Failed to reset password: " + e.getMessage(), e);
        }
    }

    private String getPasswordResetEmailBody(String passwordResetToken, String username) {
        String resetLink = envConfiguration.getResetPasswordLink() + passwordResetToken;

        return "<html>"
                + "<body>"
                + "<h2>Hello " + username + ",</h2>"
                + "<p>To reset your password, please click the link below:</p>"
                + "<p><a href=\"" + resetLink + "\">Reset my password</a></p>"
                + "<p>If you did not request this action, please ignore this email.</p>"
                + getEmailSignature()
                + "</body>"
                + "</html>";
    }

}
