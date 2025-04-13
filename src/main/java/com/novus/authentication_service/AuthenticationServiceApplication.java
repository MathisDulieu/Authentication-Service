package com.novus.authentication_service;

import com.novus.authentication_service.configuration.DateConfiguration;
import com.novus.authentication_service.configuration.EnvConfiguration;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import java.util.TimeZone;

@Slf4j
@SpringBootApplication(scanBasePackages = "com.novus.authentication_service")
@EnableConfigurationProperties(EnvConfiguration.class)
public class AuthenticationServiceApplication {

	private final DateConfiguration dateConfiguration = new DateConfiguration();

	public static void main(String[] args) {
		SpringApplication.run(AuthenticationServiceApplication.class, args);
	}

	@PostConstruct
	void setLocalTimeZone() {
		TimeZone.setDefault(TimeZone.getTimeZone("Europe/Paris"));
		log.info("Authentication Service running in Paris timezone, started at: {}", dateConfiguration.newDate());
	}

	@Configuration
	@Profile("test")
	@ComponentScan(lazyInit = true)
	static class ConfigForShorterBootTimeForTests {
	}

}
