package com.novus.authentication_service.configuration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.*;

@Component
public class KafkaExplicitConsumerTest implements CommandLineRunner {

    private static final Logger logger = LoggerFactory.getLogger(KafkaExplicitConsumerTest.class);

    @Override
    public void run(String... args) throws Exception {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka.railway.internal:29092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "explicit-test-group-" + System.currentTimeMillis());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // Lire depuis le début

        logger.info("=========== TEST EXPLICIT CONSUMER ===========");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of("teste"));
            logger.info("Consumer explicite abonné au topic: teste");

            consumer.poll(Duration.ofMillis(0));
            consumer.commitSync();

            Set<TopicPartition> assignment = new HashSet<>();
            while (assignment.isEmpty()) {
                consumer.poll(Duration.ofMillis(100));
                assignment = consumer.assignment();
            }

            Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(assignment);
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(assignment);

            logger.info("Offsets du topic:");
            for (TopicPartition partition : assignment) {
                long beginning = beginningOffsets.get(partition);
                long end = endOffsets.get(partition);
                logger.info("Partition {}: Début={}, Fin={}, Messages={}",
                        partition.partition(), beginning, end, end - beginning);

                consumer.seek(partition, beginning);
            }

            int messageCount = 0;
            boolean keepReading = true;

            logger.info("Lecture des messages du topic:");
            while (keepReading) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                if (records.isEmpty()) {
                    if (messageCount > 0) {
                        keepReading = false;
                    }
                } else {
                    for (ConsumerRecord<String, String> record : records) {
                        messageCount++;
                        logger.info("Message #{}: Partition={}, Offset={}, Value={}",
                                messageCount, record.partition(), record.offset(), record.value());
                    }
                }

                if (messageCount >= 10) {
                    keepReading = false;
                }
            }

            if (messageCount == 0) {
                logger.warn("Aucun message trouvé dans le topic!");
            } else {
                logger.info("Total de {} messages lus du topic", messageCount);
            }
        } catch (Exception e) {
            logger.error("Erreur dans le consumer explicite: {}", e.getMessage(), e);
        }

        logger.info("===========================================");
    }
}
