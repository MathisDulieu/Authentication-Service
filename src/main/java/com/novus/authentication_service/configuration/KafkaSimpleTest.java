package com.novus.authentication_service.configuration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

@Component
@Order(1)
public class KafkaSimpleTest implements CommandLineRunner {

    private static final Logger logger = LoggerFactory.getLogger(KafkaSimpleTest.class);

    @Override
    public void run(String... args) throws Exception {
        String topic = "teste";
        String message = "Test message " + System.currentTimeMillis();
        String groupId = "test-group-" + System.currentTimeMillis();

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka.railway.internal:29092");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka.railway.internal:29092");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try {
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);
                RecordMetadata metadata = producer.send(record).get();

                logger.info("============= TEST KAFKA =============");
                logger.info("Message produit: {}", message);
                logger.info("Topic: {}, Partition: {}, Offset: {}",
                        metadata.topic(), metadata.partition(), metadata.offset());
            }

            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
                consumer.subscribe(List.of(topic));

                boolean messageFound = false;
                for (int i = 0; i < 5 && !messageFound; i++) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Message consommé: {}", record.value());
                        if (record.value().equals(message)) {
                            logger.info("Message de test trouvé avec succès!");
                            messageFound = true;
                        }
                    }
                }

                if (!messageFound) {
                    logger.warn("Le message de test n'a pas été trouvé après plusieurs tentatives");
                }
            }

            try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
                String authMessage = "Auth test message " + System.currentTimeMillis();
                ProducerRecord<String, String> record = new ProducerRecord<>("authenticationTopic", authMessage);

                RecordMetadata metadata = producer.send(record).get();
                logger.info("Message produit sur authenticationTopic: {}", authMessage);
                logger.info("Topic: {}, Partition: {}, Offset: {}",
                        metadata.topic(), metadata.partition(), metadata.offset());
            }

            logger.info("============= FIN TEST KAFKA =============");
        } catch (Exception e) {
            logger.error("Erreur lors du test Kafka: {}", e.getMessage(), e);
        }
    }
}
