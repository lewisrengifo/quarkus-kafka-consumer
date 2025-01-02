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
import java.time.LocalDateTime;
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
    private final AtomicBoolean needsReopening = new AtomicBoolean(false);

    @Inject
    Consumer<String, String> consumer;

    @Inject
    KafkaConfig kafkaConfig;

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

    @Scheduled(every = "60s")
    void checkAndReopenConsumer() {
        if (needsReopening.get()) {
            log.info("Attempting to reopen consumer");
            reopenConsumer();
            restartConsumerThread();
            needsReopening.set(false);
        }
    }

    private void consumeMessages() {
        try {
            while (running.get()) {
                if (consumer != null) {

                    ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT);;
                    for (ConsumerRecord<String, String> record : records) {
                        long startTime = System.currentTimeMillis();
                        ProcessingResult result = messageProcessor.processMessage(record.value());
                        long endTime = System.currentTimeMillis();
                        long duration = endTime - startTime;
                        log.info("The process took: {}", duration);
                        switch (result) {
                            case SUCCESS:
                                consumer.commitSync();
                                break;
                            case RETRY_LATER:
                                closeConsumer();
                                needsReopening.set(true);
                                return;
                            case FAILURE:
                                log.error("Failed to process message: {}", record.value());

                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("Error in consumer loop", e);
            needsReopening.set(true);
        }
    }

    private void closeConsumer() {
        if (consumer != null) {
            try {
                consumer.close();
                consumer = null;
                log.info("Consumer closed successfully");
            } catch (Exception e) {
                log.error("Error closing consumer", e);
            }
        }
    }

    private void reopenConsumer() {
        try {
            consumer = kafkaConfig.kafkaConsumer();
            log.info("Consumer reopened successfully");
        } catch (Exception e) {
            log.error("Error reopening consumer", e);
            consumer = null;
        }
    }
    private void restartConsumerThread() {
        executorService.submit(this::consumeMessages);
        log.info("Consumer thread restarted");
    }
}