package com.novus.authentication_service;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaListeners {

    @KafkaListener(
            topics = "authenticationTopic",
            groupId = "groupId"
    )
    void listener(String data) {
        System.out.println("Date received : " + data);
    }

}
