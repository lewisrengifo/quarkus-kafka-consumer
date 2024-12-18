package org.demo.application;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.demo.infraestructure.http.ExternalApiService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@ApplicationScoped
public class MessageProcessor {
  private static final Logger log = LoggerFactory.getLogger(MessageProcessor.class);

  private final AtomicBoolean isProcessing = new AtomicBoolean(true);
  private final AtomicInteger processedCount = new AtomicInteger(0);

  @Inject ExternalApiService externalApiService;

  public ProcessingResult processMessage(String message) {
    if (!isProcessing.get()) {
      log.warn("Message processor is paused. Skipping message processing.");
      return ProcessingResult.RETRY_LATER;
    }

    log.info("Processing message: {}", message);
    boolean result = externalApiService.callExternalApi(message);

    if (result) {
      processedCount.incrementAndGet();
      log.info("Successfully processed message. Total processed: {}", processedCount.get());
      return ProcessingResult.SUCCESS;
    }

    return ProcessingResult.RETRY_LATER;
  }

  public void pause() {
    isProcessing.set(false);
    log.info("Message processor paused");
  }

  public void resume() {
    isProcessing.set(true);
    log.info("Message processor resumed");
  }

  public boolean isProcessing() {
    return isProcessing.get();
  }

  public int getProcessedCount() {
    return processedCount.get();
  }
}
