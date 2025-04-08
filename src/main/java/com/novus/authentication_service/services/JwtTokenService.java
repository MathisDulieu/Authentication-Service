package com.novus.authentication_service.services;

import com.novus.authentication_service.configuration.EnvConfiguration;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.security.Keys;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.Key;
import java.time.Instant;
import java.util.Date;

@Component
@RequiredArgsConstructor
public class JwtTokenService {

    private final EnvConfiguration envConfiguration;
    private static final long TOKEN_EXPIRATION_TIME = 172_800_000;

    public String generateEmailConfirmationToken(String userId) {
        Instant now = Instant.now();
        Date expiryDate = Date.from(now.plusMillis(TOKEN_EXPIRATION_TIME));

        return Jwts.builder()
                .setSubject(userId)
                .claim("type", "email_confirmation")
                .setIssuedAt(Date.from(now))
                .setExpiration(expiryDate)
                .signWith(getSigningKey(), SignatureAlgorithm.HS512)
                .compact();
    }

    private Key getSigningKey() {
        String secretString = envConfiguration.getJwtSecret();
        byte[] keyBytes = secretString.getBytes(StandardCharsets.UTF_8);
        return Keys.hmacShaKeyFor(keyBytes);
    }

    public String generatePasswordResetToken(String userId) {
        Instant now = Instant.now();
        Date expiryDate = Date.from(now.plusMillis(15 * 60 * 1000));

        return Jwts.builder()
                .setSubject(userId)
                .claim("type", "password_reset")
                .setIssuedAt(Date.from(now))
                .setExpiration(expiryDate)
                .signWith(getSigningKey(), SignatureAlgorithm.HS512)
                .compact();
    }
}
