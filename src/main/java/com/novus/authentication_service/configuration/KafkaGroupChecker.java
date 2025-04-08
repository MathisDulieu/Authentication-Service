package com.novus.authentication_service.configuration;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class KafkaGroupChecker implements CommandLineRunner {

    private static final Logger logger = LoggerFactory.getLogger(KafkaGroupChecker.class);

    @Override
    public void run(String... args) throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka.railway.internal:29092");

        try (AdminClient adminClient = AdminClient.create(props)) {
            ListConsumerGroupsResult groups = adminClient.listConsumerGroups();
            Collection<ConsumerGroupListing> groupListings = groups.all().get();

            logger.info("CONSUMER GROUPS DISPONIBLES:");
            for (ConsumerGroupListing group : groupListings) {
                logger.info(" - {}", group.groupId());

                // Obtenir les détails pour ce groupe
                DescribeConsumerGroupsResult groupDetails =
                        adminClient.describeConsumerGroups(Collections.singletonList(group.groupId()));
                ConsumerGroupDescription description =
                        groupDetails.describedGroups().get(group.groupId()).get();

                logger.info("   État: {}", description.state());
                logger.info("   Membres: {}", description.members().size());

                ListConsumerGroupOffsetsResult offsetsResult =
                        adminClient.listConsumerGroupOffsets(group.groupId());
                Map<TopicPartition, OffsetAndMetadata> offsets =
                        offsetsResult.partitionsToOffsetAndMetadata().get();

                for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
                    if (entry.getKey().topic().equals("authenticationTopic")) {
                        logger.info("   Topic: {}, Partition: {}, Offset: {}",
                                entry.getKey().topic(),
                                entry.getKey().partition(),
                                entry.getValue().offset());
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Erreur lors de la vérification des consumer groups: {}", e.getMessage(), e);
        }
    }
}
