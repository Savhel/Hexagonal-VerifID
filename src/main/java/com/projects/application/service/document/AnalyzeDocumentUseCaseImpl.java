package com.projects.application.service.document;

import com.projects.application.port.in.document.AnalyzeDocumentUseCase;
import com.projects.application.port.out.AiAnalysisServicePort;
import com.projects.application.port.out.FileStoragePort;
import com.projects.application.port.out.OcrServicePort;
import com.projects.application.port.out.VerificationLogRepositoryPort;
import com.projects.domain.model.VerificationLog;
import com.projects.adapter.in.web.dto.DocumentAnalysisResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Application use case — Document analysis orchestration.
 * Orchestrates OCR → AI extraction → validation → logging.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class AnalyzeDocumentUseCaseImpl implements AnalyzeDocumentUseCase {

    private final OcrServicePort ocrService;
    private final AiAnalysisServicePort aiService;
    private final VerificationLogRepositoryPort verificationLogRepository;
    private final FileStoragePort fileStoragePort;
    private final Validator validator;
    private final ObjectMapper objectMapper;

    private static final DateTimeFormatter[] DATE_FORMATTERS = {
        DateTimeFormatter.ofPattern("dd.MM.yyyy"), DateTimeFormatter.ofPattern("dd/MM/yyyy"),
        DateTimeFormatter.ofPattern("dd-MM-yyyy"), DateTimeFormatter.ofPattern("yyyy-MM-dd"),
        DateTimeFormatter.ofPattern("d.MM.yyyy"),  DateTimeFormatter.ofPattern("d/MM/yyyy")
    };

    @Override
    public Mono<DocumentAnalysisResponse> analyzeDocument(byte[] frontBytes, byte[] backBytes,
                                                           String frontFilename, Long platformId) {
        boolean isPdf = frontFilename != null && frontFilename.toLowerCase().endsWith(".pdf");

        return analyzeAndLog(frontBytes, backBytes, isPdf, platformId)
            .flatMap(response -> {
                Mono<DocumentAnalysisResponse> resultMono = Mono.just(response);
                if (Boolean.TRUE.equals(response.getIsValid())) {
                    // Upload file to file_core if valid (chemin synchrone REST historique)
                    Flux<org.springframework.core.io.buffer.DataBuffer> contentFlux = Flux.just(new DefaultDataBufferFactory().wrap(frontBytes));
                    return fileStoragePort.storeFile(frontFilename, isPdf ? "application/pdf" : "image/jpeg", contentFlux, null, null, null)
                            .then(resultMono)
                            .onErrorResume(e -> {
                                log.error("Failed to upload document to file_core: {}", e.getMessage());
                                return resultMono;
                            });
                }
                return resultMono;
            });
    }

    @Override
    public Mono<DocumentAnalysisResponse> analyzeStoredDocument(byte[] frontBytes, String frontFilename,
                                                                Long platformId) {
        boolean isPdf = frontFilename != null && frontFilename.toLowerCase().endsWith(".pdf");
        // Le fichier est déjà stocké dans le Kernel (mode asynchrone) → pas de ré-upload.
        return analyzeAndLog(frontBytes, null, isPdf, platformId);
    }

    /** OCR → IA → validation → enregistrement du log. Commun aux modes synchrone et asynchrone. */
    private Mono<DocumentAnalysisResponse> analyzeAndLog(byte[] frontBytes, byte[] backBytes, boolean isPdf,
                                                         Long platformId) {
        Mono<String> frontOcr = ocrService.extractText(frontBytes, isPdf);
        Mono<String> backOcr = backBytes != null && backBytes.length > 0
            ? ocrService.extractText(backBytes, isPdf)
            : Mono.just("");

        return Mono.zip(frontOcr, backOcr)
            .flatMap(tuple -> {
                String combined = tuple.getT1() + "\n" + tuple.getT2();
                return aiService.extractDocumentData(combined)
                    .map(geminiFields -> buildAnalysisResponse(tuple.getT1(), tuple.getT2(), geminiFields, combined));
            })
            .flatMap(response -> logVerification(response, platformId).thenReturn(response));
    }

    private DocumentAnalysisResponse buildAnalysisResponse(String front, String back,
                                                             Map<String, String> geminiFields,
                                                             String rawCombined) {
        log.info("=== Starting Gemini Document Analysis ===");
        Map<String, String> fields = new HashMap<>(geminiFields);
        String docType = fields.getOrDefault("documentType", "UNKNOWN");
        String issuingCountry = fields.getOrDefault("issuingCountry", "UNKNOWN");

        fields.entrySet().removeIf(e -> e.getValue() == null ||
            e.getValue().equalsIgnoreCase("null") || e.getValue().isBlank());

        LocalDate birthDate  = parseDate(fields.get("dateOfBirth"));
        LocalDate issueDate  = parseDate(fields.get("issueDate"));
        LocalDate expiryDate = parseDate(fields.get("expiryDate"));

        String holderName = buildHolderName(fields);
        boolean namesValid   = fields.get("surname") != null && fields.get("givenNames") != null;
        boolean isExpired    = expiryDate != null && expiryDate.isBefore(LocalDate.now());
        boolean hasDocNumber = fields.get("documentNumber") != null;
        boolean nomencValid  = validateNomenclature(issuingCountry, docType, fields.get("documentNumber"));

        boolean valid = !isExpired && namesValid && !docType.equals("UNKNOWN") && hasDocNumber && nomencValid;

        StringBuilder msg = new StringBuilder();
        if (valid) { msg.append("Document valide (Analyse Gemini)"); }
        else {
            if (docType.equals("UNKNOWN"))      msg.append("Type inconnu. ");
            if (!namesValid)                    msg.append("Noms manquants. ");
            if (!hasDocNumber)                  msg.append("Numéro manquant. ");
            else if (!nomencValid)              msg.append("Format du numéro invalide pour ").append(issuingCountry).append(". ");
            if (isExpired)                      msg.append("Document expiré. ");
            if (msg.length() == 0)              msg.append("Document non conforme.");
        }

        double confidence = 0.9;
        if (!namesValid || !hasDocNumber) confidence = 0.5;
        else if (!nomencValid) confidence = 0.6;

        DocumentAnalysisResponse response = DocumentAnalysisResponse.builder()
            .documentType(docType).issuingCountry(issuingCountry)
            .documentNumber(fields.get("documentNumber")).holderName(holderName)
            .dateOfBirth(birthDate).issueDate(issueDate).expirationDate(expiryDate)
            .isValid(valid).validationMessage(msg.toString().trim())
            .confidenceScore(confidence).hasUncertainty(confidence < 0.6)
            .additionalFields(buildAdditionalFields(fields)).rawExtractedText(rawCombined)
            .build();

        Set<ConstraintViolation<DocumentAnalysisResponse>> violations = validator.validate(response);
        if (!violations.isEmpty()) {
            String summary = violations.stream().map(ConstraintViolation::getMessage)
                .distinct().collect(Collectors.joining(", "));
            response.setValidationMessage(response.getValidationMessage() + " (Format: " + summary + ")");
        }
        return response;
    }

    private Mono<Void> logVerification(DocumentAnalysisResponse response, Long platformId) {
        String additionalFieldsJson = null;
        if (response.getAdditionalFields() != null && !response.getAdditionalFields().isEmpty()) {
            try { additionalFieldsJson = objectMapper.writeValueAsString(response.getAdditionalFields()); }
            catch (JsonProcessingException e) { log.error("Failed to serialize additional fields", e); }
        }
        String status = Boolean.TRUE.equals(response.getIsValid()) ? "ACCEPTED" : "REJECTED";
        String reason = Boolean.TRUE.equals(response.getIsValid()) ? null : response.getValidationMessage();

        VerificationLog logEntry = VerificationLog.builder()
            .platformId(platformId).date(LocalDateTime.now())
            .docType(response.getDocumentType()).status(status).reason(reason)
            .confidence(response.getConfidenceScore()).processingTimeMs(1500)
            .documentNumber(response.getDocumentNumber()).holderName(response.getHolderName())
            .dateOfBirth(response.getDateOfBirth() != null ? response.getDateOfBirth().toString() : null)
            .issueDate(response.getIssueDate() != null ? response.getIssueDate().toString() : null)
            .expiryDate(response.getExpirationDate() != null ? response.getExpirationDate().toString() : null)
            .additionalFields(additionalFieldsJson)
            .build();

        return verificationLogRepository.save(logEntry).then();
    }

    private String buildHolderName(Map<String, String> fields) {
        String s = fields.get("surname"), g = fields.get("givenNames");
        if (s != null && g != null) return s.trim() + " " + g.trim();
        return s != null ? s.trim() : (g != null ? g.trim() : "INCONNU");
    }

    private Map<String, String> buildAdditionalFields(Map<String, String> fields) {
        Set<String> topLevel = Set.of("surname", "givenNames", "documentNumber", "dateOfBirth", "issueDate",
            "expiryDate", "expirationDate", "documentType", "issuingCountry");
        Map<String, String> add = new LinkedHashMap<>();
        fields.forEach((k, v) -> { if (v != null && !v.isEmpty() && !topLevel.contains(k)) add.put(k, v); });
        return add;
    }

    private LocalDate parseDate(String dateStr) {
        if (dateStr == null) return null;
        String clean = dateStr.replaceAll("[^\\d./-]", "").trim();
        for (DateTimeFormatter fmt : DATE_FORMATTERS) {
            try { return LocalDate.parse(clean, fmt); } catch (Exception ignored) {}
        }
        return null;
    }

    private boolean validateNomenclature(String country, String docType, String documentNumber) {
        if (documentNumber == null || documentNumber.isBlank()) return false;
        if (country == null || country.equalsIgnoreCase("UNKNOWN")) return documentNumber.length() >= 5;
        String nc = country.toLowerCase().trim();
        String nn = documentNumber.replaceAll("[^a-zA-Z0-9]", "");
        if (nc.contains("gabon")) return nn.length() == 14;
        if (nc.contains("cameroun") || nc.contains("cameroon")) {
            if ("ID_CARD".equals(docType)) return nn.length() >= 9 && nn.length() <= 17;
            if ("PASSPORT".equals(docType)) return nn.length() >= 7;
            return nn.length() >= 5;
        }
        if (nc.contains("tchad") || nc.contains("chad")) return nn.length() >= 5 && nn.length() <= 20;
        if (nc.contains("congo")) return nn.length() >= 5 && nn.length() <= 25;
        if (nc.contains("centrafrique") || nc.contains("central african")) return nn.length() >= 5 && nn.length() <= 20;
        return documentNumber.length() >= 5 && documentNumber.length() <= 30;
    }
}
