package org.demo.infraestructure.kafka;

import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.demo.application.MessageProcessor;
import org.demo.application.ProcessingResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

@ApplicationScoped
public class KafkaConsumerService {
    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerService.class);
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(100);

    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicBoolean paused = new AtomicBoolean(false);
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    @Inject
    Consumer<String, String> consumer;

    @Inject
    MessageProcessor messageProcessor;

    void onStart(@Observes StartupEvent ev) {
        log.info("Starting Kafka consumer");
        executorService.submit(this::consumeMessages);
    }

    void onStop(@Observes ShutdownEvent ev) {
        log.info("Stopping Kafka consumer");
        running.set(false);
        executorService.shutdown();
    }

    @Scheduled(every = "100s")
    void checkAndResumeConsumer() {
        if (paused.get()) {
            log.info("Attempting to resume consumer after pause period");
            resumeConsumer();
        }
    }

    private void consumeMessages() {
        try {
            while (running.get()) {
                if (!paused.get()) {
                    ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT);
                    for (ConsumerRecord<String, String> record : records) {
                        ProcessingResult result = messageProcessor.processMessage(record.value());
                        switch (result) {
                            case SUCCESS:
                                consumer.commitSync();
                                break;
                            case RETRY_LATER:
                                pauseConsumer();
                                return;
                            case FAILURE:
                                log.error("Failed to process message: {}", record.value());
                                pauseConsumer();
                                return;
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("Error in consumer loop", e);
        } finally {
            consumer.close();
        }
    }

    private void pauseConsumer() {
        if (!paused.get()) {
            Set<TopicPartition> assignments = consumer.assignment();
            consumer.pause(assignments);
            paused.set(true);
            log.info("Consumer paused due to processing failures");
        }
    }

    private void resumeConsumer() {
        if (paused.get()) {
            Set<TopicPartition> assignments = consumer.assignment();
            consumer.resume(assignments);
            paused.set(false);
            log.info("Consumer resumed");
        }
    }

    public boolean isPaused() {
        return paused.get();
    }
}