package org.demo.infraestructure.http;

import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;

import java.time.Duration;
import java.util.function.Supplier;

@ApplicationScoped
public class ExternalApiService {
    private static final Logger log = LoggerFactory.getLogger(ExternalApiService.class);
    private static final int MAX_RETRIES = 10;
    private static final Duration WAIT_DURATION = Duration.ofSeconds(1);

    @Inject
    @RestClient
    ExternalApiClient apiClient;

    private final Retry retry;

    public ExternalApiService() {
        RetryConfig config = RetryConfig.custom()
            .maxAttempts(MAX_RETRIES)
            .waitDuration(WAIT_DURATION)
            .retryOnResult(response -> response instanceof Response && ((Response) response).getStatus() == 503)
            .build();
        
        this.retry = Retry.of("externalApiRetry", config);
    }

    public boolean callExternalApi(String message) {
        try {
            Supplier<Response> apiCall = () -> apiClient.sendRequest(message);
            Response response = retry.executeSupplier(apiCall);
            
            if (response.getStatus() == 503) {
                log.error("External API still unavailable after {} retries", MAX_RETRIES);
                return false;
            }
            
            return response.getStatus() >= 200 && response.getStatus() < 300;
            
        } catch (Exception e) {
            log.error("Error calling external API", e);
            return false;
        }
    }
}