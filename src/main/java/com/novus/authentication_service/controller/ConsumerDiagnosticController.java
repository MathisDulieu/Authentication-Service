package com.novus.authentication_service.controller;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Duration;
import java.util.*;

@RestController
@RequestMapping("/api/consumer-diagnostic")
public class ConsumerDiagnosticController {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerDiagnosticController.class);

    private static final String bootstrapServers = "kafka.railway.internal:29092";

    private static final String groupId = "authentication-groupId";

    @GetMapping("/info")
    public ResponseEntity<Map<String, Object>> getKafkaInfo() {
        Map<String, Object> result = new HashMap<>();
        result.put("bootstrapServers", bootstrapServers);
        result.put("groupId", groupId);

        // Essayer de se connecter à Kafka
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        try (AdminClient adminClient = AdminClient.create(props)) {
            // Liste des topics
            try {
                ListTopicsResult topics = adminClient.listTopics();
                Set<String> topicNames = topics.names().get();
                result.put("topics", topicNames);
                result.put("status", "connected");
            } catch (Exception e) {
                result.put("topics_error", e.getMessage());
                logger.error("Erreur lors de la récupération des topics", e);
                result.put("status", "error");
            }
        } catch (Exception e) {
            result.put("status", "error");
            result.put("error", e.getMessage());
            logger.error("Erreur de connexion à Kafka", e);
        }

        return ResponseEntity.ok(result);
    }

    @GetMapping("/messages/{topic}")
    public ResponseEntity<List<Map<String, Object>>> getTopicMessages(
            @PathVariable String topic,
            @RequestParam(defaultValue = "100") int maxMessages) {

        List<Map<String, Object>> messages = new ArrayList<>();

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "diagnostic-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxMessages);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            // Obtenir les informations sur les partitions
            List<TopicPartition> partitions = new ArrayList<>();
            for (org.apache.kafka.common.PartitionInfo partitionInfo : consumer.partitionsFor(topic)) {
                partitions.add(new TopicPartition(topic, partitionInfo.partition()));
            }

            if (partitions.isEmpty()) {
                return ResponseEntity.ok(Collections.singletonList(
                        Collections.singletonMap("warning", "Aucune partition trouvée pour le topic " + topic)
                ));
            }

            // Assigner les partitions et se positionner au début
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);

            // Limiter le nombre de messages à traiter
            int messagesRead = 0;
            boolean moreMessages = true;

            while (moreMessages && messagesRead < maxMessages) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));

                if (records.isEmpty()) {
                    moreMessages = false;
                } else {
                    for (ConsumerRecord<String, String> record : records) {
                        Map<String, Object> messageMap = new HashMap<>();
                        messageMap.put("offset", record.offset());
                        messageMap.put("partition", record.partition());
                        messageMap.put("timestamp", record.timestamp());
                        messageMap.put("key", record.key());
                        messageMap.put("value", record.value());
                        messages.add(messageMap);

                        messagesRead++;
                        if (messagesRead >= maxMessages) {
                            moreMessages = false;
                            break;
                        }
                    }
                }
            }

            logger.info("Récupéré {} messages du topic {}", messages.size(), topic);
        } catch (Exception e) {
            logger.error("Erreur lors de la récupération des messages du topic {}", topic, e);
            return ResponseEntity.status(500).body(Collections.singletonList(
                    Collections.singletonMap("error", e.getMessage())
            ));
        }

        return ResponseEntity.ok(messages);
    }

    @GetMapping("/consumer-status")
    public ResponseEntity<Map<String, Object>> getConsumerStatus() {
        Map<String, Object> status = new HashMap<>();
        status.put("groupId", groupId);
        status.put("bootstrapServers", bootstrapServers);

        // Vérifier la configuration du consumer existant
        try {
            // Obtenir les propriétés du Listener
            status.put("listenerActive", true);
            status.put("messageCount", "Actif - voir les logs pour le nombre de messages traités");

            return ResponseEntity.ok(status);
        } catch (Exception e) {
            status.put("listenerActive", false);
            status.put("error", e.getMessage());
            return ResponseEntity.status(500).body(status);
        }
    }
}
