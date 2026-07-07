package com.orchestrator.kafka.consumers;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orchestrator.entity.SagaTransaction;
import com.orchestrator.enums.StepStatus;
import com.orchestrator.event.NotificationRequestEvent;
import com.orchestrator.event.WalletResponseEvent;
import com.orchestrator.kafka.producers.NotificationRequestProducer;
import com.orchestrator.metrics.KafkaMetricsService;
import com.orchestrator.service.SagaService;
import com.orchestrator.util.TraceUtil;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * ==========================================================
 * WALLET RESPONSE CONSUMER (ORCHESTRATOR - FINAL)
 * ==========================================================
 *
 * AUTHOR: Sanjeev Kumar
 *
 * ==========================================================
 * 🎯 WHAT THIS CLASS DOES
 * ==========================================================
 *
 * This consumer processes WalletResponseEvent coming from Wallet service
 * and drives the Saga workflow forward.
 *
 * Responsibilities:
 *   1. Extract traceId for distributed tracing
 *   2. Deserialize Kafka message → WalletResponseEvent
 *   3. Validate incoming event
 *   4. Execute Saga logic:
 *        ✔ On SUCCESS → trigger Notification step
 *        ✔ On FAILURE → mark Saga as FAILED
 *   5. Record metrics (success/failure/latency)
 *   6. Acknowledge Kafka message ONLY when safe
 *
 * ==========================================================
 * 🔍 TRACE FLOW
 * ==========================================================
 *
 * Wallet Service → Kafka (traceId in header)
 *                → Orchestrator extracts traceId
 *                → TraceUtil.setTraceId(traceId)
 *                → Logs automatically include traceId (via MDC)
 *
 * WHY:
 *   Enables end-to-end tracking of a single transaction
 *
 * NOTE:
 *   ❌ traceId is NOT used in metrics (avoids high cardinality)
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
 *   ✔ Managed inside SagaService (not here)
 *
 * ==========================================================
 * ⚠ ERROR HANDLING STRATEGY
 * ==========================================================
 *
 * DESERIALIZATION ERROR → retry → DLQ
 * VALIDATION ERROR      → retry → DLQ
 * PROCESSING ERROR      → retry → DLQ
 *
 * BUSINESS FAILURE (wallet failed):
 *   ✔ Saga marked FAILED
 *   ✔ ACK message (NO retry)
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
 *     - Invalid messages
 *     - Unexpected exceptions
 *
 * ==========================================================
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class WalletResponseConsumer {

    private final SagaService sagaService;
    private final NotificationRequestProducer notificationProducer;
    private final ObjectMapper objectMapper;
    private final KafkaMetricsService metrics;

    @Value("${wallet.topic.response}")
    private String topic;

    /**
     * ======================================================
     * MAIN KAFKA CONSUMER
     * ======================================================
     */
    @KafkaListener(
            topics = "${wallet.topic.response}",
            groupId = "${orchestrator.consumer.wallet-response-group}",
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

        log.info("📥 Received WalletResponseEvent");

        WalletResponseEvent event;

        try {
            // ==================================================
            // STEP 1: DESERIALIZATION
            // ==================================================
            event = objectMapper.readValue(message, WalletResponseEvent.class);

        } catch (Exception ex) {
            metrics.incrementConsumerFailure(topic);

            log.error("❌ DESERIALIZATION FAILED → retry → DLQ", ex);

            throw new RuntimeException("Invalid message format", ex);
        }

        // ==================================================
        // STEP 2: VALIDATION
        // ==================================================
        if (event.getTransactionId() == null || event.getStatus() == null) {

            metrics.incrementConsumerFailure(topic);

            log.error("❌ INVALID EVENT → retry → DLQ");

            throw new RuntimeException("Invalid WalletResponseEvent");
        }

        String transactionId = event.getTransactionId();

        try {
            // ==================================================
            // STEP 3: BUSINESS PROCESSING (MEASURE ONLY THIS)
            // ==================================================
            long processingStart = System.currentTimeMillis();

            if (event.getStatus() == StepStatus.COMPLETED) {

                SagaTransaction saga = sagaService.handleWalletSuccess(transactionId);

                log.info("✅ Wallet SUCCESS → triggering Notification | txnId={}", transactionId);

                NotificationRequestEvent notificationEvent =
                        NotificationRequestEvent.builder()
                                .transactionId(transactionId)
                                .userId(saga.getUserId())
                                .message("Wallet transaction successful")
                                .createdAt(Instant.now())
                                .source("ORCHESTRATOR")
                                .version("v1")
                                .build();

                notificationProducer.sendNotificationRequest(notificationEvent);

            } else {

                log.error("⚠ Wallet FAILED → marking Saga FAILED | txnId={}", transactionId);

                sagaService.handleWalletFailure(transactionId, event.getErrorMessage());
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
            // STEP 4: CLEAN TRACE CONTEXT
            // ==================================================
            TraceUtil.clear();
        }
    }
}