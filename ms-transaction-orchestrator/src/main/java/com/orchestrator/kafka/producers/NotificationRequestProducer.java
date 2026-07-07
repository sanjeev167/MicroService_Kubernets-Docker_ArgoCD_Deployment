package com.orchestrator.kafka.producers;

import com.orchestrator.event.NotificationRequestEvent;
import com.orchestrator.metrics.KafkaMetricsService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * ==========================================================
 * NOTIFICATION REQUEST PRODUCER (ORCHESTRATOR - FINAL)
 * ==========================================================
 *
 * AUTHOR: Sanjeev Kumar
 *
 * ==========================================================
 * 🎯 WHAT THIS CLASS DOES
 * ==========================================================
 *
 * This producer publishes NotificationRequestEvent to Kafka
 * as part of the Saga workflow.
 *
 * Responsibilities:
 *   1. Validate outgoing event
 *   2. Publish event to Kafka (trace-enabled)
 *   3. Record producer metrics (success/failure/latency)
 *
 * ==========================================================
 * 🔍 ROLE IN SAGA FLOW
 * ==========================================================
 *
 * Wallet SUCCESS → Orchestrator → NotificationRequestProducer
 *               → Kafka → Notification Service
 *
 * This is the SECOND step in Saga orchestration.
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
 *   End-to-end tracing across services
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
 *   - Detect Kafka failures
 *   - Measure publishing latency
 *   - Ensure reliability of Saga step execution
 *
 * ==========================================================
 * ⚠ DESIGN PRINCIPLES
 * ==========================================================
 *
 * ✔ Fail-fast (critical for Saga consistency)
 * ✔ No business logic inside producer
 * ✔ Reusable via BaseKafkaProducer
 * ✔ Observability-first design
 *
 * ==========================================================
 */
@Service
public class NotificationRequestProducer extends BaseKafkaProducer {

    private static final Logger log = LoggerFactory.getLogger(NotificationRequestProducer.class);

    private final String notificationRequestTopic;
    private final KafkaMetricsService metrics;

    /**
     * Constructor
     */
    public NotificationRequestProducer(
            KafkaTemplate<String, Object> kafkaTemplate,
            KafkaMetricsService metrics,
            @Value("${notification.topic.request}") String notificationRequestTopic
    ) {
        super(kafkaTemplate);
        this.metrics = metrics;
        this.notificationRequestTopic = notificationRequestTopic;
    }

    /**
     * ======================================================
     * SEND NOTIFICATION REQUEST EVENT
     * ======================================================
     *
     * Flow:
     *   1. Validate event
     *   2. Log request
     *   3. Send to Kafka (trace-enabled)
     *   4. Record metrics
     *
     * @param event NotificationRequestEvent
     */
    public void sendNotificationRequest(NotificationRequestEvent event) {

        long startTime = System.currentTimeMillis();

        // ==================================================
        // STEP 1: VALIDATION
        // ==================================================
        if (event == null ||
            event.getTransactionId() == null || event.getTransactionId().isBlank() ||
            event.getUserId() == null || event.getUserId().isBlank() ||
            event.getMessage() == null || event.getMessage().isBlank()) {

            log.error("❌ INVALID NOTIFICATION EVENT - NOT SENDING | event={}", event);
            throw new IllegalArgumentException("Invalid NotificationRequestEvent");
        }

        final String transactionId = event.getTransactionId();

        // ==================================================
        // STEP 2: LOG
        // ==================================================
        log.info("📤 SENDING NOTIFICATION REQUEST | topic={} | txnId={} | userId={}",
                notificationRequestTopic,
                transactionId,
                event.getUserId());

        log.debug("Notification request payload: {}", event);

        try {
            // ==================================================
            // STEP 3: SEND (TRACE ENABLED)
            // ==================================================
            sendWithTrace(notificationRequestTopic, transactionId, event);

            // ==================================================
            // SUCCESS METRIC
            // ==================================================
            metrics.incrementProducerSuccess(notificationRequestTopic);

        } catch (Exception ex) {

            // ==================================================
            // FAILURE METRIC
            // ==================================================
            metrics.incrementProducerFailure(notificationRequestTopic);

            log.error("❌ KAFKA SEND FAILURE | topic={} | txnId={}",
                    notificationRequestTopic, transactionId, ex);

            // 🔥 FAIL FAST (CRITICAL FOR SAGA)
            throw new RuntimeException("Kafka send failed", ex);

        } finally {

            // ==================================================
            // LATENCY METRIC
            // ==================================================
            long duration = System.currentTimeMillis() - startTime;
            metrics.recordProducerTime(notificationRequestTopic, duration);
        }
    }
}