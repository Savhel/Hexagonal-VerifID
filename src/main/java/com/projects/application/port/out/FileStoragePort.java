package com.projects.application.port.out;

import org.springframework.core.io.buffer.DataBuffer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.UUID;

/**
 * Port sortant pour le stockage de fichiers.
 */
public interface FileStoragePort {
    Mono<StoredFileDTO> storeFile(String fileName, String contentType, Flux<DataBuffer> content, UUID tenantId, UUID orgId, String bearerToken);

    /**
     * Télécharge le contenu binaire d'un fichier (y compris en quarantaine PENDING) depuis le
     * file-core du Kernel, via l'endpoint service-à-service GET /api/files/{id}/content.
     */
    Mono<byte[]> downloadContent(UUID fileId, UUID tenantId, UUID orgId);

    record StoredFileDTO(UUID id, UUID organizationId, UUID uploadedByUserId, String fileName, String contentType, long size) {}
}
