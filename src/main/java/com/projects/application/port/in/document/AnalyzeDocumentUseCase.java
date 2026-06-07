package com.projects.application.port.in.document;

import com.projects.adapter.in.web.dto.DocumentAnalysisResponse;
import reactor.core.publisher.Mono;

/**
 * Inbound port — Document analysis use case.
 */
public interface AnalyzeDocumentUseCase {
    Mono<DocumentAnalysisResponse> analyzeDocument(byte[] frontBytes, byte[] backBytes,
                                                    String frontFilename, Long platformId);

    /**
     * Analyse asynchrone d'un document déjà stocké dans le file-core du Kernel : OCR → IA →
     * validation → log. Ne ré-uploade PAS le fichier (il existe déjà côté Kernel). Utilisé par le
     * consumer Kafka FILE_ANALYSIS_REQUESTED.
     */
    Mono<DocumentAnalysisResponse> analyzeStoredDocument(byte[] frontBytes, String frontFilename, Long platformId);
}
