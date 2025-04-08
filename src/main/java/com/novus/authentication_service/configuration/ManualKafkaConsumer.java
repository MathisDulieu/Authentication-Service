package com.novus.authentication_service.configuration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
public class ManualKafkaConsumer implements CommandLineRunner {

    private static final Logger logger = LoggerFactory.getLogger(ManualKafkaConsumer.class);
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    @Override
    public void run(String... args) {
        executorService.submit(() -> {
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka.railway.internal:29092");
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "authenticationTopic-group");
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);

            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
                consumer.subscribe(List.of("authenticationTopic"));
                logger.info("Consumer souscrit manuellement au topic: authenticationTopic");

                consumer.poll(Duration.ofMillis(1000));
                consumer.commitSync();
                logger.info("Premier poll et commit effectués pour forcer l'enregistrement du consumer group");

                while (!Thread.currentThread().isInterrupted()) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Message reçu manuellement - Topic: {}, Partition: {}, Offset: {}, Valeur: {}",
                                record.topic(), record.partition(), record.offset(), record.value());
                    }
                }
            } catch (Exception e) {
                logger.error("Erreur dans le consumer manuel: {}", e.getMessage(), e);
            }
        });
    }
}