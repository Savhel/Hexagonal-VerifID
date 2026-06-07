package com.projects.config.kernel;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Propriétés Kafka pour l'intégration asynchrone avec le file-core du Kernel iwm.
 *
 * <p>VerifID consomme les événements {@code FILE_ANALYSIS_REQUESTED} émis par iwm pour les
 * documents KYC, et publie en retour {@code FILE_ANALYSIS_COMPLETED} sur le même topic métier.
 */
@Component
@ConfigurationProperties(prefix = "kernel.kafka")
public class KernelKafkaProperties {

    /** Activation de l'intégration Kafka (consumer + producer). */
    private boolean enabled = false;

    /** Brokers Kafka (ex: localhost:9092). */
    private String bootstrapServers = "localhost:9092";

    /** Topic métier d'iwm (émission et réception des événements business). */
    private String topic = "iwm.events.business";

    /** Group id du consumer VerifID. */
    private String groupId = "verifid-file-analysis";

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }

    public String getBootstrapServers() { return bootstrapServers; }
    public void setBootstrapServers(String bootstrapServers) { this.bootstrapServers = bootstrapServers; }

    public String getTopic() { return topic; }
    public void setTopic(String topic) { this.topic = topic; }

    public String getGroupId() { return groupId; }
    public void setGroupId(String groupId) { this.groupId = groupId; }
}
