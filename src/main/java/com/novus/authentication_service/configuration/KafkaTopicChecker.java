package com.novus.authentication_service.configuration;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

@Component
public class KafkaTopicChecker implements CommandLineRunner {

    private static final Logger logger = LoggerFactory.getLogger(KafkaTopicChecker.class);

    @Override
    public void run(String... args) throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka.railway.internal:29092");

        try (AdminClient adminClient = AdminClient.create(props)) {
            ListTopicsResult topics = adminClient.listTopics();
            Set<String> topicNames = topics.names().get();

            if (topicNames.contains("authenticationTopic")) {
                logger.info("Le topic 'authenticationTopic' existe");

                DescribeTopicsResult topicInfo = adminClient.describeTopics(Collections.singletonList("authenticationTopic"));
                TopicDescription desc = topicInfo.all().get().get("authenticationTopic");

                logger.info("Nombre de partitions: {}", desc.partitions().size());

                try {
                    for (TopicPartitionInfo partition : desc.partitions()) {
                        int partitionId = partition.partition();

                        Properties consumerProps = new Properties();
                        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka.railway.internal:29092");
                        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "topic-checker-" + System.currentTimeMillis());

                        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
                            TopicPartition tp = new TopicPartition("authenticationTopic", partitionId);
                            consumer.assign(Collections.singletonList(tp));

                            Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(Collections.singletonList(tp));
                            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(Collections.singletonList(tp));

                            long beginOffset = beginningOffsets.get(tp);
                            long endOffset = endOffsets.get(tp);
                            long messageCount = endOffset - beginOffset;

                            logger.info("Partition {}: nombre de messages = {}", partitionId, messageCount);
                        }
                    }
                } catch (Exception e) {
                    logger.error("Erreur lors du comptage des messages: {}", e.getMessage(), e);
                }
            } else {
                logger.warn("Le topic 'authenticationTopic' n'existe PAS!");
                logger.info("Topics disponibles: {}", topicNames);
            }

            if (topicNames.contains("teste")) {
                logger.info("Le topic 'teste' existe");
            } else {
                logger.warn("Le topic 'teste' n'existe PAS!");
            }
        } catch (Exception e) {
            logger.error("Erreur lors de la vérification du topic: {}", e.getMessage(), e);
        }
    }
}
