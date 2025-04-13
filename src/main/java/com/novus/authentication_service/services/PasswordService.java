package com.novus.authentication_service.services;

import com.novus.authentication_service.configuration.DateConfiguration;
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
import java.util.Map;
import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class PasswordService {

    private final LogUtils logUtils;
    private final UserDaoUtils userDaoUtils;
    private final EmailService emailService;
    private final JwtTokenService jwtTokenService;
    private final EnvConfiguration envConfiguration;
    private final DateConfiguration dateConfiguration;

    public void processSendForgotPasswordEmail(KafkaMessage kafkaMessage) {
        Map<String, String> request = kafkaMessage.getRequest();
        String userId = request.get("userId");
        String email = request.get("email");
        log.info("Starting to process forgot password request for user with email: {} and ID: {}", email, userId);

        try {
            Optional<User> optionalUser = userDaoUtils.findById(userId);

            if (optionalUser.isEmpty()) {
                log.error("User with ID: {} and email: {} not found when sending password reset email", userId, email);
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

            user.setLastActivityDate(dateConfiguration.newDate());
            userDaoUtils.save(user);

            String passwordResetToken = jwtTokenService.generatePasswordResetToken(userId);

            emailService.sendEmail(email, "Reset your Supmap password", buildPasswordResetEmail(passwordResetToken, user.getUsername()));

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
            log.info("Password reset email successfully sent to: {}", email);

        } catch (Exception e) {
            log.error("Error occurred while processing forgot password request: {}", e.getMessage());
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
        log.info("Starting to process reset password request for user with ID: {}", userId);

        try {
            Optional<User> optionalUser = userDaoUtils.findById(userId);

            if (optionalUser.isEmpty()) {
                log.error("User with ID: {} not found when resetting password", userId);
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
            user.setUpdatedAt(dateConfiguration.newDate());
            user.setLastActivityDate(dateConfiguration.newDate());

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
            log.info("Password successfully reset for user with ID: {} and email: {}", userId, user.getEmail());
        } catch (Exception e) {
            log.error("Error occurred while processing reset password request: {}", e.getMessage());
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

    public String buildPasswordResetEmail(String passwordResetToken, String username) {
        String resetLink = envConfiguration.getResetPasswordLink() + passwordResetToken;

        return "<!DOCTYPE html>\n" +
                "<html lang=\"en\">\n" +
                "<head>\n" +
                "    <meta charset=\"UTF-8\">\n" +
                "    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">\n" +
                "    <title>Reset Your Password</title>\n" +
                "</head>\n" +
                "<body style=\"font-family: Arial, sans-serif; line-height: 1.6; color: #333; max-width: 600px; margin: 0 auto; padding: 20px;\">\n" +
                "    <div style=\"background-color: #f9f9f9; padding: 20px; border-radius: 5px; border-left: 4px solid #4285f4;\">\n" +
                "        <h2 style=\"color: #4285f4; margin-top: 0;\">Reset Your Password</h2>\n" +
                "        <p>Hello " + username + ",</p>\n" +
                "        <p>We received a request to reset your password for your SupMap account. To complete this process, please click on the button below:</p>\n" +
                "        <div style=\"text-align: center; margin: 30px 0;\">\n" +
                "            <a href=\"" + resetLink + "\" style=\"background-color: #4285f4; color: white; padding: 12px 24px; text-decoration: none; border-radius: 4px; font-weight: bold; display: inline-block;\">Reset My Password</a>\n" +
                "        </div>\n" +
                "        <p>If the button doesn't work, you can also copy and paste the following link into your browser:</p>\n" +
                "        <p style=\"background-color: #f0f0f0; padding: 10px; border-radius: 5px; word-break: break-all;\"><a href=\"" + resetLink + "\" style=\"color: #4285f4; text-decoration: none;\">" + resetLink + "</a></p>\n" +
                "        <p><strong>Important:</strong> This link will expire in 24 hours for security reasons.</p>\n" +
                "        <p>If you did not request a password reset, please ignore this email or contact our support team if you have concerns about your account security.</p>\n" +
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
