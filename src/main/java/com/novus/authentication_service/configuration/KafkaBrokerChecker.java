package com.novus.authentication_service.configuration;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.common.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Properties;
import java.util.Set;

@Component
public class KafkaBrokerChecker implements CommandLineRunner {

    private static final Logger logger = LoggerFactory.getLogger(KafkaBrokerChecker.class);

    @Override
    public void run(String... args) throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka.railway.internal:29092");

        try (AdminClient adminClient = AdminClient.create(props)) {
            // Tenter de récupérer les métadonnées du cluster
            DescribeClusterResult cluster = adminClient.describeCluster();
            String clusterId = cluster.clusterId().get();
            Collection<Node> nodes = cluster.nodes().get();

            logger.info("Connexion réussie au broker Kafka");
            logger.info("Cluster ID: {}", clusterId);
            logger.info("Nombre de noeuds: {}", nodes.size());

            for (Node node : nodes) {
                logger.info("Noeud: id={}, host={}, port={}", node.id(), node.host(), node.port());
            }

            ListTopicsResult topics = adminClient.listTopics();
            Set<String> topicNames = topics.names().get();

            logger.info("Topics disponibles:");
            for (String topic : topicNames) {
                logger.info(" - {}", topic);
            }
        } catch (Exception e) {
            logger.error("Erreur de connexion au broker Kafka: {}", e.getMessage(), e);
        }
    }
}