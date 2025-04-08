package com.novus.authentication_service.services;

import jakarta.mail.MessagingException;
import jakarta.mail.internet.MimeMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.mail.MailException;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.stereotype.Service;

import java.io.UnsupportedEncodingException;

@Service
@Slf4j
@RequiredArgsConstructor
public class EmailService {

    @Autowired
    private JavaMailSender javaMailSender;

    private static final String APP_EMAIL = System.getenv("APP_EMAIL");
    private static final String MAIL_MODIFIED_USERNAME = System.getenv("MAIL_MODIFIED_USERNAME");

    public void sendEmail(String to, String subject, String body) {
        try {
            MimeMessage mimeMessage = javaMailSender.createMimeMessage();
            MimeMessageHelper helper = new MimeMessageHelper(mimeMessage, false, "utf-8");

            helper.setFrom(APP_EMAIL, MAIL_MODIFIED_USERNAME);
            helper.setTo(to);
            helper.setSubject(subject);
            helper.setText(body, true);

            javaMailSender.send(mimeMessage);
            log.info("Email sent successfully to " + to);
        } catch (MailException | MessagingException e) {
            log.error("Error while sending email: " + e.getMessage());
            throw new RuntimeException("Error sending email: " + e.getMessage(), e);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    public static String getEmailSignature() {
        return "<br>"
                + "<p>Cordialement,<br>"
                + "L'équipe SupMap.</p>"
                + "<p>SupMap - Simplifiez vos itinéraires et vos projets.</p>"
                + "<p>📞 Assistance : <a href=\"tel:+33614129625\">+33 6 14 12 96 25</a><br>"
                + "📩 Email : <a href=\"mailto:supmap.application@gmail.com\">supmap.application@gmail.com</a><br>"
                + "🌐 Site web : <a href=\"https://supmap-application.com\">https://supmap-application.com</a><br>"
                + "📱 Disponible sur iOS et Android !</p>";
    }

}
