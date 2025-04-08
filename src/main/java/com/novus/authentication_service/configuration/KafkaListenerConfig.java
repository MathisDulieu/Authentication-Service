package com.novus.authentication_service.configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;

@Configuration
public class KafkaListenerConfig {

    private static final Logger logger = LoggerFactory.getLogger(KafkaListenerConfig.class);
    private static final String TOPIC = "authenticationTopic";

    @Autowired
    private ConsumerFactory<String, String> consumerFactory;

    @Bean
    public KafkaMessageListenerContainer<String, String> listenerContainer() {
        ContainerProperties containerProps = new ContainerProperties(TOPIC);
        containerProps.setGroupId("groupId");

        containerProps.setMessageListener((MessageListener<String, String>) record -> {
            logger.info("Message reçu explicitement: {}", record.value());
            System.out.println("Received message: " + record.value());
        });

        KafkaMessageListenerContainer<String, String> container = new KafkaMessageListenerContainer<>(
                consumerFactory, containerProps);
        container.setAutoStartup(true);

        return container;
    }
}