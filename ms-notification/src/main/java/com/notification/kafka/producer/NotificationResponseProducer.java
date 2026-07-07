package com.notification.kafka.producer;

import java.time.Instant;
import java.util.UUID;

import com.notification.enums.NotificationStatus;
import com.notification.event.NotificationProcessedEvent;
import com.notification.metrics.KafkaMetricsService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * ==========================================================
 * NOTIFICATION RESPONSE PRODUCER (FINAL - PRODUCTION READY)
 * ==========================================================
 *
 * AUTHOR: Sanjeev Kumar
 *
 * ==========================================================
 * 🎯 WHAT THIS CLASS DOES
 * ==========================================================
 *
 * Publishes NotificationProcessedEvent to Orchestrator
 * after notification processing is completed.
 *
 * Responsibilities:
 *   1. Validate input
 *   2. Build response event
 *   3. Publish to Kafka (trace-enabled)
 *   4. Record producer metrics (success/failure/latency)
 *
 * ==========================================================
 * 🔍 ROLE IN SAGA FLOW
 * ==========================================================
 *
 * Notification Service → Kafka → Orchestrator
 *
 * This is the FINAL STEP in notification processing.
 *
 * ==========================================================
 * 🔍 TRACEABILITY DESIGN
 * ==========================================================
 *
 * ✔ Extends BaseKafkaProducer
 * ✔ traceId automatically:
 *     - Retrieved from MDC
 *     - Injected into Kafka headers
 *
 * Enables:
 *   End-to-end tracing (Notification → Orchestrator)
 *
 * NOTE:
 *   ❌ traceId NOT included in payload (clean domain model)
 *
 * ==========================================================
 * 📊 METRICS DESIGN
 * ==========================================================
 *
 * ✔ kafka_producer_success_total
 * ✔ kafka_producer_failure_total
 * ✔ kafka_producer_send_seconds
 *
 * WHY:
 *   - This is critical saga step
 *   - Failures must be immediately visible
 *
 * ==========================================================
 * ⚠ DESIGN PRINCIPLES
 * ==========================================================
 *
 * ✔ Fail-fast (CRITICAL for saga correctness)
 * ✔ No business logic inside producer
 * ✔ Observability-first design
 * ✔ TransactionId used as Kafka key (ordering guarantee)
 *
 * ==========================================================
 */
@Service
public class NotificationResponseProducer extends BaseKafkaProducer {

    private static final Logger log = LoggerFactory.getLogger(NotificationResponseProducer.class);

    private final KafkaMetricsService metrics;
    private final String notificationResponseTopic;

    public NotificationResponseProducer(
            KafkaTemplate<String, Object> kafkaTemplate,
            KafkaMetricsService metrics,
            @Value("${notification.topic.response}") String notificationResponseTopic) {

        super(kafkaTemplate);
        this.metrics = metrics;
        this.notificationResponseTopic = notificationResponseTopic;
    }

    /**
     * ======================================================
     * SEND NOTIFICATION RESPONSE
     * ======================================================
     *
     * Flow:
     *   1. Validate input
     *   2. Build event
     *   3. Send to Kafka
     *   4. Record metrics
     */
    public void sendNotificationResponse(UUID transactionId,
                                         NotificationStatus status,
                                         String message) {

        long startTime = System.currentTimeMillis();

        // ==================================================
        // STEP 1: VALIDATION
        // ==================================================
        if (transactionId == null || status == null) {
            log.error("❌ INVALID NOTIFICATION RESPONSE | txnId={} | status={}", transactionId, status);
            throw new IllegalArgumentException("Invalid notification response");
        }

        if (notificationResponseTopic == null || notificationResponseTopic.isBlank()) {
            throw new IllegalStateException("Kafka topic not configured");
        }

        final String key = transactionId.toString();

        // ==================================================
        // STEP 2: BUILD EVENT
        // ==================================================
        NotificationProcessedEvent event = NotificationProcessedEvent.builder()
                .transactionId(transactionId.toString())
                .status(status)
                .message(message)
                .createdAt(Instant.now())
                .source("NOTIFICATION_SERVICE")
                .version("v1")
                .build();

        log.info("📤 SENDING NOTIFICATION RESPONSE | topic={} | txnId={} | status={}",
                notificationResponseTopic, key, status);

        log.debug("Notification response payload: {}", event);

        try {
            // ==================================================
            // STEP 3: SEND (TRACE ENABLED)
            // ==================================================
            sendWithTrace(notificationResponseTopic, key, event);

            // ==================================================
            // SUCCESS METRIC
            // ==================================================
            metrics.incrementProducerSuccess(notificationResponseTopic);

        } catch (Exception ex) {

            // ==================================================
            // FAILURE METRIC
            // ==================================================
            metrics.incrementProducerFailure(notificationResponseTopic);

            log.error("❌ KAFKA SEND FAILURE | topic={} | txnId={} | status={}",
                    notificationResponseTopic, key, status, ex);

            // 🔥 FAIL FAST (VERY IMPORTANT)
            throw new RuntimeException("Kafka send failed", ex);

        } finally {

            // ==================================================
            // LATENCY METRIC
            // ==================================================
            long duration = System.currentTimeMillis() - startTime;
            metrics.recordProducerTime(notificationResponseTopic, duration);
        }
    }
}