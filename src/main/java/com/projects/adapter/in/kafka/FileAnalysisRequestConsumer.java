package com.projects.adapter.in.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.projects.application.port.in.document.AnalyzeDocumentUseCase;
import com.projects.application.port.out.FileStoragePort;
import com.projects.config.kernel.KernelKafkaProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.UUID;

/**
 * Consume les événements {@code FILE_ANALYSIS_REQUESTED} émis par le file-core d'iwm pour les
 * documents KYC, télécharge le binaire depuis le Kernel, lance l'analyse OCR+IA, puis publie le
 * verdict {@code FILE_ANALYSIS_COMPLETED}.
 *
 * <p>Format wire : {@code OutboxEvent} JSON d'iwm. On lit {@code eventType}, {@code tenantId},
 * {@code organizationId}, {@code aggregateId} (= fileId) et {@code payload.{fileId,fileName,documentType}}.
 */
@Component
@Slf4j
@ConditionalOnProperty(prefix = "kernel.kafka", name = "enabled", havingValue = "true")
public class FileAnalysisRequestConsumer {

    private static final String EVENT_REQUESTED = "FILE_ANALYSIS_REQUESTED";

    private final FileStoragePort fileStoragePort;
    private final AnalyzeDocumentUseCase analyzeDocumentUseCase;
    private final FileAnalysisResultPublisher resultPublisher;
    private final ObjectMapper objectMapper;

    public FileAnalysisRequestConsumer(FileStoragePort fileStoragePort,
                                       AnalyzeDocumentUseCase analyzeDocumentUseCase,
                                       FileAnalysisResultPublisher resultPublisher,
                                       ObjectMapper objectMapper) {
        this.fileStoragePort = fileStoragePort;
        this.analyzeDocumentUseCase = analyzeDocumentUseCase;
        this.resultPublisher = resultPublisher;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(
            topics = "#{@kernelKafkaProperties.topic}",
            groupId = "#{@kernelKafkaProperties.groupId}",
            containerFactory = "kernelKafkaListenerContainerFactory")
    public void onMessage(ConsumerRecord<String, String> record, Acknowledgment ack) {
        JsonNode event;
        try {
            event = objectMapper.readTree(record.value());
        } catch (Exception e) {
            log.error("[verifid] Événement Kafka illisible, ignoré (offset={}) : {}", record.offset(), e.getMessage());
            ack.acknowledge();
            return;
        }

        String eventType = text(event, "eventType");
        if (!EVENT_REQUESTED.equals(eventType)) {
            ack.acknowledge();
            return;
        }

        JsonNode payload = event.path("payload");
        UUID fileId = parseUuid(firstNonBlank(text(payload, "fileId"), text(event, "aggregateId")));
        UUID tenantId = parseUuid(text(event, "tenantId"));
        UUID orgId = parseUuid(text(event, "organizationId"));
        String fileName = firstNonBlank(text(payload, "fileName"), fileId == null ? "document" : fileId.toString());

        if (fileId == null || tenantId == null) {
            log.warn("[verifid] FILE_ANALYSIS_REQUESTED sans fileId/tenantId, ignoré");
            ack.acknowledge();
            return;
        }

        log.info("[verifid] Analyse demandée — fileId={} tenant={} documentType={}",
                fileId, tenantId, text(payload, "documentType"));

        process(fileId, tenantId, orgId, fileName)
                .doOnSuccess(v -> ack.acknowledge())
                .doOnError(e -> log.error("[verifid] Échec analyse fileId={} : {}", fileId, e.getMessage(), e))
                // On acquitte même en cas d'erreur d'analyse : le verdict REJECTED a été publié.
                .onErrorResume(e -> Mono.fromRunnable(ack::acknowledge))
                .subscribe();
    }

    private Mono<Void> process(UUID fileId, UUID tenantId, UUID orgId, String fileName) {
        return fileStoragePort.downloadContent(fileId, tenantId, orgId)
                .flatMap(bytes -> analyzeDocumentUseCase.analyzeStoredDocument(bytes, fileName, null))
                .flatMap(result -> resultPublisher.publishCompleted(
                        tenantId, orgId, fileId,
                        Boolean.TRUE.equals(result.getIsValid()),
                        result.getValidationMessage(),
                        result))
                .onErrorResume(e -> resultPublisher.publishCompleted(
                        tenantId, orgId, fileId, false,
                        "Analyse impossible : " + e.getMessage(), null));
    }

    private static String text(JsonNode node, String field) {
        JsonNode v = node.get(field);
        return v == null || v.isNull() ? null : v.asText();
    }

    private static String firstNonBlank(String a, String b) {
        return a != null && !a.isBlank() ? a : b;
    }

    private static UUID parseUuid(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        try {
            return UUID.fromString(raw.trim());
        } catch (IllegalArgumentException e) {
            return null;
        }
    }
}
