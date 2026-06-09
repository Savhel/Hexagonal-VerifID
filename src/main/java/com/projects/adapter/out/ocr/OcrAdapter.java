package com.projects.adapter.out.ocr;

import com.projects.application.port.out.OcrServicePort;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

/**
 * Infrastructure adapter — implements OcrServicePort using the external parsing API.
 */
@Component
@Slf4j
public class OcrAdapter implements OcrServicePort {

    private final WebClient webClient;

    @Value("${parsing.api.url:https://parsing.invalid/api}")
    private String parsingApiUrl;

    @Value("${parsing.api.token:}")
    private String parsingApiToken;

    public OcrAdapter(WebClient.Builder webClientBuilder) {
        this.webClient = webClientBuilder.build();
    }

    @Override
    public Mono<String> extractText(byte[] content, boolean isPdf) {
        log.info("Performing in-memory OCR. Content size: {} bytes", content.length);
        String base64File = Base64.getEncoder().encodeToString(content);

        Map<String, Object> payload = new HashMap<>();
        payload.put("file", base64File);
        payload.put("fileType", isPdf ? 0 : 1);

        return webClient.post()
            .uri(parsingApiUrl)
            .header("Authorization", "token " + parsingApiToken)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(payload)
            .retrieve()
            .onStatus(status -> status.isError(), response ->
                response.bodyToMono(String.class).flatMap(body -> {
                    log.error("OCR API Error {}: {}", response.statusCode(), body);
                    return Mono.error(new RuntimeException("OCR API returned " + response.statusCode() + ": " + body));
                }))
            .bodyToMono(Map.class)
            .timeout(java.time.Duration.ofSeconds(300))
            .map(resp -> {
                try {
                    var result = (Map<String, Object>) resp.get("result");
                    var layouts = (java.util.List<?>) result.get("layoutParsingResults");
                    if (layouts == null || layouts.isEmpty()) return "";
                    var first = (Map<String, Object>) layouts.get(0);
                    var markdown = (Map<String, Object>) first.get("markdown");
                    return (String) markdown.get("text");
                } catch (Exception e) {
                    log.error("Failed to parse OCR response: {}", e.getMessage());
                    return "";
                }
            });
    }
}
