package org.demo.infraestructure.http;

import org.demo.infraestructure.http.dto.MessageRequest;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;

@ApplicationScoped
public class ExternalApiService {
  private static final Logger log = LoggerFactory.getLogger(ExternalApiService.class);
  private static final int MAX_RETRIES = 10;

  @Inject @RestClient ExternalApiClient apiClient;

  public boolean callExternalApi(String message) {
    int retryCount = 0;
    while (retryCount < MAX_RETRIES) {
      try {
        Response response = apiClient.sendRequest(new MessageRequest(message));

        if (response.getStatus() == 503) {
          log.warn("Received 503 response, retry {}/{}", retryCount + 1, MAX_RETRIES);
          retryCount++;
          Thread.sleep(1000);
          continue;
        }

        return response.getStatus() >= 200 && response.getStatus() < 300;

      } catch (Exception e) {
        log.error("Error calling external API", e);
        return false;
      }
    }

    log.error("External API unavailable (503) after {} retries", MAX_RETRIES);
    return false;
  }
}
