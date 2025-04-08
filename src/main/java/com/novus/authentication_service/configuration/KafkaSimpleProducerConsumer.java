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
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

@Component
public class KafkaSimpleProducerConsumer implements CommandLineRunner {

    private static final Logger logger = LoggerFactory.getLogger(KafkaSimpleProducerConsumer.class);

    @Override
    public void run(String... args) throws Exception {
        String topic = "teste";
        String groupId = "teste-group";

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka.railway.internal:29092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka.railway.internal:29092");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        logger.info("=========== TEST SIMPLE PRODUCER/CONSUMER ===========");

        String testMessage = "Simple test " + System.currentTimeMillis();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, testMessage);
            RecordMetadata metadata = producer.send(record).get();
            logger.info("Message produit: {} (topic={}, partition={}, offset={})",
                    testMessage, metadata.topic(), metadata.partition(), metadata.offset());
        }

        Thread.sleep(1000);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Arrays.asList(topic));
            logger.info("Consumer abonné au topic: {}", topic);

            boolean messageFound = false;
            for (int i = 0; i < 5 && !messageFound; i++) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                logger.info("Poll #{}: {} records reçus", i+1, records.count());

                for (ConsumerRecord<String, String> record : records) {
                    logger.info("Message reçu: {} (partition={}, offset={})",
                            record.value(), record.partition(), record.offset());

                    if (record.value().equals(testMessage)) {
                        logger.info("Message de test trouvé!");
                        messageFound = true;
                    }
                }
            }

            if (!messageFound) {
                logger.warn("Le message de test n'a pas été trouvé!");
            }
        }

        logger.info("===========================================");
    }
}