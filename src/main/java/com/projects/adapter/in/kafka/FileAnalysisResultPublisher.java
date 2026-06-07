package com.projects.adapter.in.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.projects.adapter.in.web.dto.DocumentAnalysisResponse;
import com.projects.config.kernel.KernelKafkaProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Publie le verdict d'analyse vers iwm via Kafka, au format {@code OutboxEvent} attendu par le
 * {@code KafkaBusinessEventConsumerListener} du Kernel.
 *
 * <p>eventType = {@code FILE_ANALYSIS_COMPLETED}, aggregateType = {@code STORED_FILE},
 * aggregateId = fileId, payload = {@code {fileId, verdict, reason, extracted...}}.
 */
@Component
@Slf4j
@ConditionalOnProperty(prefix = "kernel.kafka", name = "enabled", havingValue = "true")
public class FileAnalysisResultPublisher {

    private static final String EVENT_COMPLETED = "FILE_ANALYSIS_COMPLETED";
    private static final String AGGREGATE_TYPE = "STORED_FILE";

    private final KafkaTemplate<String, String> kernelKafkaTemplate;
    private final KernelKafkaProperties props;
    private final ObjectMapper objectMapper;

    public FileAnalysisResultPublisher(KafkaTemplate<String, String> kernelKafkaTemplate,
                                       KernelKafkaProperties props,
                                       ObjectMapper objectMapper) {
        this.kernelKafkaTemplate = kernelKafkaTemplate;
        this.props = props;
        this.objectMapper = objectMapper;
    }

    public Mono<Void> publishCompleted(UUID tenantId, UUID orgId, UUID fileId,
                                       boolean accepted, String reason, DocumentAnalysisResponse details) {
        return Mono.fromRunnable(() -> {
            try {
                String json = objectMapper.writeValueAsString(buildEvent(tenantId, orgId, fileId, accepted, reason, details));
                kernelKafkaTemplate.send(props.getTopic(), fileId.toString(), json);
                log.info("[verifid] Verdict publié — fileId={} verdict={}", fileId, accepted ? "ACCEPTED" : "REJECTED");
            } catch (Exception e) {
                log.error("[verifid] Échec publication verdict fileId={} : {}", fileId, e.getMessage(), e);
            }
        });
    }

    private Map<String, Object> buildEvent(UUID tenantId, UUID orgId, UUID fileId,
                                           boolean accepted, String reason, DocumentAnalysisResponse details) {
        Instant now = Instant.now();

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("fileId", fileId.toString());
        payload.put("verdict", accepted ? "ACCEPTED" : "REJECTED");
        payload.put("reason", reason == null ? "" : reason);
        if (details != null) {
            payload.put("documentType", details.getDocumentType());
            payload.put("documentNumber", details.getDocumentNumber());
            payload.put("holderName", details.getHolderName());
            payload.put("confidenceScore", details.getConfidenceScore());
        }

        // Enveloppe OutboxEvent attendue par iwm (JsonOutboxEvent).
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("id", UUID.randomUUID().toString());
        event.put("tenantId", tenantId.toString());
        event.put("createdAt", now.toString());
        event.put("updatedAt", now.toString());
        event.put("organizationId", orgId == null ? null : orgId.toString());
        event.put("eventType", EVENT_COMPLETED);
        event.put("aggregateType", AGGREGATE_TYPE);
        event.put("aggregateId", fileId.toString());
        event.put("occurredAt", now.toString());
        event.put("payload", payload);
        event.put("status", "PUBLISHED");
        event.put("attemptCount", 1);
        event.put("publishedAt", now.toString());
        return event;
    }
}
