package com.orchestrator.kafka.consumers;

import java.nio.charset.StandardCharsets;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orchestrator.enums.StepStatus;
import com.orchestrator.event.NotificationResponseEvent;
import com.orchestrator.kafka.producers.WalletCompensationProducer;
import com.orchestrator.metrics.KafkaMetricsService;
import com.orchestrator.service.SagaService;
import com.orchestrator.util.TraceUtil;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * ==========================================================
 * NOTIFICATION RESPONSE CONSUMER (ORCHESTRATOR - FINAL)
 * ==========================================================
 *
 * AUTHOR: Sanjeev Kumar
 *
 * ==========================================================
 * 🎯 WHAT THIS CLASS DOES
 * ==========================================================
 *
 * This consumer handles NotificationResponseEvent and completes
 * the Saga workflow OR triggers compensation.
 *
 * Responsibilities:
 *   1. Extract traceId for distributed tracing
 *   2. Deserialize Kafka message → NotificationResponseEvent
 *   3. Execute Saga logic:
 *        ✔ SUCCESS → complete Saga
 *        ✔ FAILURE → trigger compensation (wallet rollback)
 *   4. Record metrics (success/failure/latency)
 *   5. Acknowledge Kafka message safely
 *
 * ==========================================================
 * 🔍 TRACE FLOW
 * ==========================================================
 *
 * Notification Service → Kafka (traceId header)
 *                      → Orchestrator extracts traceId
 *                      → TraceUtil.setTraceId(traceId)
 *                      → Logs automatically include traceId
 *
 * NOTE:
 *   ❌ traceId is NOT used in metrics (low cardinality)
 *
 * ==========================================================
 * 📊 METRICS DESIGN
 * ==========================================================
 *
 * INFRA METRICS:
 *   ✔ kafka_consumer_success_total
 *   ✔ kafka_consumer_failure_total
 *   ✔ kafka_consumer_processing_seconds
 *
 * BUSINESS METRICS:
 *   ✔ Managed inside SagaService
 *
 * ==========================================================
 * ⚠ ERROR HANDLING STRATEGY
 * ==========================================================
 *
 * DESERIALIZATION ERROR → retry → DLQ
 * PROCESSING ERROR      → retry → DLQ
 *
 * BUSINESS FAILURE:
 *   ✔ Compensation triggered
 *   ✔ ACK (NO retry)
 *
 * ==========================================================
 * ⚠ ACK RULE (CRITICAL)
 * ==========================================================
 *
 * ✔ ACK only when:
 *     - Processing successful
 *     - Business failure handled
 *
 * ❌ DO NOT ACK:
 *     - Deserialization failure
 *     - Unexpected exceptions
 *
 * ==========================================================
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class NotificationResponseConsumer {

    private final SagaService sagaService;
    private final WalletCompensationProducer compensationProducer;
    private final ObjectMapper objectMapper;
    private final KafkaMetricsService metrics;

    @Value("${notification.topic.response}")
    private String topic;

    /**
     * ======================================================
     * MAIN KAFKA CONSUMER
     * ======================================================
     */
    @KafkaListener(
            topics = "${notification.topic.response}",
            groupId = "${orchestrator.consumer.notification-response-group}",
            concurrency = "1"
    )
    public void consume(
            String message,
            Acknowledgment ack,
            @Header(name = "traceId", required = false) byte[] traceHeader
    ) {

        // ==================================================
        // STEP 0: TRACE ID SETUP
        // ==================================================
        String traceId = (traceHeader != null)
                ? new String(traceHeader, StandardCharsets.UTF_8)
                : TraceUtil.generateTraceId();

        TraceUtil.setTraceId(traceId);

        log.info("📥 Received NotificationResponseEvent");

        NotificationResponseEvent event;

        try {
            // ==================================================
            // STEP 1: DESERIALIZATION
            // ==================================================
            event = objectMapper.readValue(message, NotificationResponseEvent.class);

        } catch (Exception ex) {
            metrics.incrementConsumerFailure(topic);

            log.error("❌ DESERIALIZATION FAILED → retry → DLQ", ex);

            throw new RuntimeException("Invalid message format", ex);
        }

        String transactionId = event.getTransactionId();

        try {
            // ==================================================
            // STEP 2: BUSINESS PROCESSING (MEASURE ONLY THIS)
            // ==================================================
            long processingStart = System.currentTimeMillis();

            if (event.getStatus() == StepStatus.COMPLETED) {

                sagaService.handleNotificationSuccess(transactionId);

                log.info("✅ Saga COMPLETED | txnId={}", transactionId);

            } else {

                log.error("⚠ Notification FAILED → triggering compensation | txnId={}", transactionId);

                sagaService.handleNotificationFailure(transactionId, event.getErrorMessage());

                // Trigger compensation flow
                compensationProducer.sendWalletCompensationRequest(transactionId);
            }

            long processingTime = System.currentTimeMillis() - processingStart;

            // ==================================================
            // METRICS + ACK (SUCCESS PATH)
            // ==================================================
            metrics.incrementConsumerSuccess(topic);
            metrics.recordProcessingTime(topic, processingTime);

            ack.acknowledge();

        } catch (Exception ex) {

            metrics.incrementConsumerFailure(topic);

            log.error("❌ PROCESSING FAILED → retry → may go to DLQ | txnId={}", transactionId, ex);

            throw new RuntimeException("Processing failed", ex);

        } finally {
            // ==================================================
            // STEP 3: CLEAN TRACE CONTEXT
            // ==================================================
            TraceUtil.clear();
        }
    }
}