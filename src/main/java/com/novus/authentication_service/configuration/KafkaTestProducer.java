package com.novus.authentication_service.configuration;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Component
public class KafkaTestProducer implements CommandLineRunner {

    private static final Logger logger = LoggerFactory.getLogger(KafkaTestProducer.class);

    @Override
    public void run(String... args) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka.railway.internal:29092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            String message = "Test message " + System.currentTimeMillis();
            ProducerRecord<String, String> record = new ProducerRecord<>("authenticationTopic", message);

            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    logger.error("Erreur lors de l'envoi du message: {}", exception.getMessage(), exception);
                } else {
                    logger.info("Message envoyé: topic={}, partition={}, offset={}",
                            metadata.topic(), metadata.partition(), metadata.offset());
                }
            });

            producer.flush();
            logger.info("Message de test envoyé au topic authenticationTopic");
        } catch (Exception e) {
            logger.error("Erreur dans le producer de test: {}", e.getMessage(), e);
        }
    }
}
