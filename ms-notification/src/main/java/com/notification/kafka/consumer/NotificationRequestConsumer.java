package com.notification.kafka.consumer;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.notification.event.NotificationRequestEvent;
import com.notification.metrics.KafkaMetricsService;
import com.notification.service.NotificationService;
import com.notification.util.TraceUtil;

import lombok.extern.slf4j.Slf4j;

/**
 * ==========================================================
 * NOTIFICATION REQUEST CONSUMER (PRODUCTION READY)
 * ==========================================================
 *
 * AUTHOR: Sanjeev Kumar
 *
 * ==========================================================
 * 🎯 WHAT THIS CLASS DOES
 * ==========================================================
 *
 * Consumes NotificationRequestEvent from Kafka and triggers
 * notification processing.
 *
 * Responsibilities:
 *   1. Extract traceId for distributed tracing
 *   2. Deserialize incoming message
 *   3. Validate event
 *   4. Execute business logic
 *   5. Record metrics
 *   6. Acknowledge Kafka message
 *
 * ==========================================================
 * 🔍 TRACE FLOW
 * ==========================================================
 *
 * Orchestrator → Kafka (traceId header)
 *              → Notification Consumer extracts traceId
 *              → TraceUtil.setTraceId(traceId)
 *              → Logs automatically include traceId
 *
 * NOTE:
 *   ❌ traceId NOT used in metrics (low cardinality)
 *
 * ==========================================================
 * 📊 METRICS DESIGN
 * ==========================================================
 *
 * ✔ kafka_consumer_success_total
 * ✔ kafka_consumer_failure_total
 * ✔ kafka_consumer_processing_seconds
 *
 * ==========================================================
 * ⚠ ERROR HANDLING STRATEGY
 * ==========================================================
 *
 * DESERIALIZATION ERROR → retry → DLQ
 * VALIDATION ERROR      → retry → DLQ
 * PROCESSING ERROR      → retry → DLQ
 *
 * SUCCESS → ACK
 *
 * ==========================================================
 * ⚠ ACK RULE
 * ==========================================================
 *
 * ✔ ACK only on success
 * ❌ DO NOT ACK on failure
 *
 * ==========================================================
 */
@Service
@Slf4j
public class NotificationRequestConsumer {

    private final NotificationService notificationService;
    private final ObjectMapper objectMapper;
    private final KafkaMetricsService metrics;

    @Value("${notification.topic.request}")
    private String topic;

    public NotificationRequestConsumer(NotificationService notificationService,
                                       ObjectMapper objectMapper,
                                       KafkaMetricsService metrics) {
        this.notificationService = notificationService;
        this.objectMapper = objectMapper;
        this.metrics = metrics;
    }

    @KafkaListener(
        topics = "${notification.topic.request}",
        groupId = "${notification.consumer.group-id}",
        concurrency = "1"
    )
    public void handleNotificationRequest(ConsumerRecord<String, String> record,
                                          Acknowledgment ack,
                                          @Header(name = "traceId", required = false) byte[] traceHeader) {

        // ==================================================
        // STEP 0: TRACE ID SETUP
        // ==================================================
        String traceId = (traceHeader != null)
                ? new String(traceHeader, StandardCharsets.UTF_8)
                : TraceUtil.generateTraceId();

        TraceUtil.setTraceId(traceId);

        log.info("📥 Received NotificationRequestEvent");

        String message = record.value();
        NotificationRequestEvent event;

        try {
            // ==================================================
            // STEP 1: DESERIALIZATION
            // ==================================================
            event = objectMapper.readValue(message, NotificationRequestEvent.class);

        } catch (Exception ex) {
            metrics.incrementConsumerFailure(topic);

            log.error("❌ DESERIALIZATION FAILED → retry → DLQ", ex);

            throw new RuntimeException("Invalid message format", ex);
        }

        // ==================================================
        // STEP 2: VALIDATION
        // ==================================================
        if (event.getTransactionId() == null) {
            metrics.incrementConsumerFailure(topic);

            log.error("❌ INVALID EVENT → transactionId missing");

            throw new RuntimeException("transactionId is required");
        }

        String txnId = event.getTransactionId()+"";

        try {
            // ==================================================
            // STEP 3: BUSINESS PROCESSING (MEASURE THIS ONLY)
            // ==================================================
            long processingStart = System.currentTimeMillis();

            notificationService.processNotification(event);

            long processingTime = System.currentTimeMillis() - processingStart;

            log.info("✅ Notification processed SUCCESS | txnId={}", txnId);

            // ==================================================
            // METRICS + ACK
            // ==================================================
            metrics.incrementConsumerSuccess(topic);
            metrics.recordProcessingTime(topic, processingTime);

            ack.acknowledge();

        } catch (Exception ex) {

            metrics.incrementConsumerFailure(topic);

            log.error("❌ PROCESSING FAILED → retry → DLQ | txnId={}", txnId, ex);

            throw new RuntimeException("Processing failed", ex);

        } finally {
            // ==================================================
            // STEP 4: CLEAN TRACE CONTEXT
            // ==================================================
            TraceUtil.clear();
        }
    }
}