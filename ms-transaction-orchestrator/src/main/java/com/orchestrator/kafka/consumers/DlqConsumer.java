package com.orchestrator.kafka.consumers;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orchestrator.entity.DlqEvent;
import com.orchestrator.metrics.KafkaMetricsService;
import com.orchestrator.repository.DlqEventRepository;
import com.orchestrator.util.TraceUtil;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * ==========================================================
 * DLQ CONSUMER (ORCHESTRATOR - FINAL PRODUCTION VERSION)
 * ==========================================================
 *
 * AUTHOR: Sanjeev Kumar
 *
 * ==========================================================
 * 🎯 WHAT THIS CLASS DOES
 * ==========================================================
 *
 * This consumer listens to Kafka DLQ topics and captures
 * permanently failed messages.
 *
 * Responsibilities:
 *   1. Extract traceId for correlation
 *   2. Capture Kafka metadata (topic, partition, offset)
 *   3. Extract error + business context
 *   4. Persist event into database
 *   5. Emit DLQ metrics
 *
 * ==========================================================
 * 🔍 WHY THIS IS CRITICAL
 * ==========================================================
 *
 * This is the FINAL SAFETY LAYER.
 *
 * ✔ Ensures ZERO message loss
 * ✔ Enables replay mechanism
 * ✔ Helps in debugging production failures
 *
 * ==========================================================
 * 📊 METRICS DESIGN
 * ==========================================================
 *
 * ✔ kafka_consumer_dlq_total
 * ✔ kafka_consumer_processing_seconds
 *
 * NOTE:
 * DLQ messages are already failures → no success/failure split
 *
 * ==========================================================
 * ⚠ DESIGN PRINCIPLES
 * ==========================================================
 *
 * ✔ NEVER throw exception (avoid DLQ of DLQ)
 * ✔ ALWAYS attempt persistence
 * ✔ KEEP processing lightweight
 * ✔ NO business logic here
 *
 * ==========================================================
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class DlqConsumer {

    private final DlqEventRepository repository;
    private final ObjectMapper objectMapper;
    private final KafkaMetricsService metrics;

    private static final String METRIC_TOPIC = "dlq";

    /**
     * ======================================================
     * MAIN DLQ CONSUMER
     * ======================================================
     */
    @KafkaListener(
        topics = {
            "${notification.topic.response}.DLQ",
            "${wallet.topic.response}.DLQ"
        },
        groupId = "orchestrator-dlq-group",
        concurrency = "1"
    )
    public void consumeDlq(ConsumerRecord<String, String> record) {

        // ==================================================
        // STEP 0: TRACE ID SETUP
        // ==================================================
        String traceId = extractTraceId(record);
        TraceUtil.setTraceId(traceId);

        log.error("🚨 DLQ MESSAGE RECEIVED");

        // ==================================================
        // METRIC: DLQ HIT (FINAL FAILURE)
        // ==================================================
        metrics.incrementConsumerDlq(METRIC_TOPIC);

        // ==================================================
        // STEP 1: EXTRACT KAFKA METADATA
        // ==================================================
        String topic = record.topic();
        int partition = record.partition();
        long offset = record.offset();
        String payload = record.value();

        log.error("Topic={} | Partition={} | Offset={}", topic, partition, offset);

        // ==================================================
        // STEP 2: EXTRACT ERROR CONTEXT
        // ==================================================
        String errorMessage = extractError(record);

        // ==================================================
        // STEP 3: EXTRACT BUSINESS CONTEXT
        // ==================================================
        String eventType = extractEventType(payload);
        String transactionId = extractTransactionId(payload);

        try {
            // ==================================================
            // STEP 4: BUILD ENTITY
            // ==================================================
            DlqEvent event = DlqEvent.builder()
                    .originalTopic(topic)
                    .partitionId(partition)
                    .offsetValue(offset)
                    .payload(payload)
                    .errorMessage(errorMessage)
                    .eventType(eventType)
                    .transactionId(transactionId)
                    .createdAt(Instant.now())
                    .replayed(false)
                    .build();

            // ==================================================
            // STEP 5: PERSIST
            // ==================================================
            repository.save(event);

            log.error("✅ DLQ event stored | id={}", event.getId());

        } catch (Exception ex) {

            // ⚠ DO NOT THROW (CRITICAL)
            log.error("❌ FAILED TO PERSIST DLQ EVENT (MANUAL INTERVENTION REQUIRED)", ex);
        } finally {

            // ==================================================
            // STEP 6: CLEAR TRACE CONTEXT
            // ==================================================
            TraceUtil.clear();
        }
    }

    // ==========================================================
    // HELPER METHODS
    // ==========================================================

    /**
     * Extract traceId from Kafka headers
     */
    private String extractTraceId(ConsumerRecord<String, String> record) {
        try {
            if (record.headers().lastHeader("traceId") != null) {
                return new String(
                        record.headers().lastHeader("traceId").value(),
                        StandardCharsets.UTF_8
                );
            }
        } catch (Exception ignored) {}
        return TraceUtil.generateTraceId();
    }

    /**
     * Extract error message from Kafka DLQ headers
     */
    private String extractError(ConsumerRecord<String, String> record) {
        try {
            if (record.headers().lastHeader("kafka_dlt-exception-message") != null) {
                return new String(
                        record.headers()
                                .lastHeader("kafka_dlt-exception-message")
                                .value(),
                        StandardCharsets.UTF_8
                );
            }
        } catch (Exception ignored) {}
        return "UNKNOWN_ERROR";
    }

    /**
     * Extract event type from payload
     */
    private String extractEventType(String payload) {
        try {
            return objectMapper.readTree(payload)
                    .path("eventType")
                    .asText("UNKNOWN");
        } catch (Exception e) {
            return "UNKNOWN";
        }
    }

    /**
     * Extract transactionId from payload
     */
    private String extractTransactionId(String payload) {
        try {
            return objectMapper.readTree(payload)
                    .path("transactionId")
                    .asText(null);
        } catch (Exception e) {
            return null;
        }
    }
}