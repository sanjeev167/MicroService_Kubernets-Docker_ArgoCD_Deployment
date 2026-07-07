package com.wallet.kafka.consumers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wallet.event.WalletRequestEvent;
import com.wallet.metrics.KafkaMetricsService;
import com.wallet.service.WalletService;
import com.wallet.util.TraceUtil;

import lombok.extern.slf4j.Slf4j;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * ==========================================================
 * WALLET REQUEST CONSUMER (FINAL - PRODUCTION READY)
 * ==========================================================
 *
 * AUTHOR: Sanjeev Kumar
 * DATE:   June 16, 2026
 *
 * ==========================================================
 * 🎯 WHAT THIS CLASS DOES
 * ==========================================================
 *
 * This class consumes wallet requests from Kafka and:
 *
 *   1. Extracts traceId for distributed tracing
 *   2. Converts raw JSON → WalletRequestEvent
 *   3. Validates incoming data
 *   4. Executes business logic (wallet service)
 *   5. Sends metrics (success/failure/latency)
 *   6. Acknowledges Kafka message ONLY on success
 *
 * ==========================================================
 * 🔍 TRACE FLOW (VERY IMPORTANT)
 * ==========================================================
 *
 * Orchestrator → Kafka (traceId in header)
 *              → Wallet Consumer extracts traceId
 *              → TraceUtil.setTraceId(traceId)
 *              → All logs now contain traceId (via MDC)
 *
 * WHY:
 *   Helps track one transaction across all microservices
 *
 * NOTE:
 *   ❌ traceId is NOT used in metrics (avoids high cardinality)
 *
 * ==========================================================
 * 📊 METRICS DESIGN
 * ==========================================================
 *
 * INFRA METRICS (Kafka level):
 *
 *   ✔ kafka_consumer_success_total
 *   ✔ kafka_consumer_failure_total
 *   ✔ kafka_consumer_processing_seconds
 *
 * BUSINESS METRICS:
 *   ✔ Handled inside WalletService (not here)
 *
 * WHY SEPARATION?
 *   - This class → Kafka handling
 *   - Service layer → business logic
 *
 * ==========================================================
 * ⚠ ERROR HANDLING STRATEGY
 * ==========================================================
 *
 * DESERIALIZATION ERROR → retry → DLQ
 * VALIDATION ERROR      → retry → DLQ
 * PROCESSING ERROR      → retry → DLQ
 *
 * SUCCESS               → ACK
 *
 * ==========================================================
 * ⚠ ACK RULE (CRITICAL)
 * ==========================================================
 *
 * ✔ ACK ONLY after successful processing
 * ❌ DO NOT ACK on failure
 * ❌ DO NOT swallow exception
 *
 * ==========================================================
 */
@Service
@Slf4j
public class WalletRequestConsumer {

    private final WalletService walletService;
    private final ObjectMapper objectMapper;
    private final KafkaMetricsService metrics;

    @Value("${wallet.topic.request}")
    private String topic;

    public WalletRequestConsumer(WalletService walletService,
                                 ObjectMapper objectMapper,
                                 KafkaMetricsService metrics) {
        this.walletService = walletService;
        this.objectMapper = objectMapper;
        this.metrics = metrics;
    }

    /**
     * ======================================================
     * MAIN KAFKA CONSUMER METHOD
     * ======================================================
     *
     * Triggered automatically when message arrives on topic.
     */
    @KafkaListener(
            topics = "${wallet.topic.request}",
            groupId = "${wallet.consumer.group-id}"
    )
    public void consume(ConsumerRecord<String, String> record,
                        Acknowledgment ack,
                        @Header(name = "traceId", required = false) byte[] traceHeader) {

        // ==================================================
        // STEP 0: TRACE ID SETUP
        // ==================================================
        String traceId = (traceHeader != null)
                ? new String(traceHeader, StandardCharsets.UTF_8)
                : TraceUtil.generateTraceId();

        TraceUtil.setTraceId(traceId);

        String message = record.value();
        String key = record.key();

        log.info("📥 Received wallet request | key={} | partition={} | offset={}",
                key, record.partition(), record.offset());

        WalletRequestEvent event;

        try {
            // ==================================================
            // STEP 1: DESERIALIZATION (JSON → OBJECT)
            // ==================================================
            event = objectMapper.readValue(message, WalletRequestEvent.class);

        } catch (Exception ex) {
            metrics.incrementConsumerFailure(topic);

            log.error("❌ DESERIALIZATION FAILED → retry → DLQ | key={}", key, ex);

            throw new RuntimeException("Invalid JSON message", ex);
        }

        // ==================================================
        // STEP 2: VALIDATION (BASIC CHECKS)
        // ==================================================
        if (event.getTransactionId() == null ||
                event.getWalletId() == null ||
                event.getUserId() == null ||
                event.getAmount() == null ||
                event.getWalletOperationType() == null) {

            metrics.incrementConsumerFailure(topic);

            log.error("❌ VALIDATION FAILED → retry → DLQ | txnId={}", event.getTransactionId());

            throw new RuntimeException("Invalid event data");
        }

        UUID transactionId = UUID.fromString(event.getTransactionId());

        try {
            // ==================================================
            // STEP 3: BUSINESS PROCESSING (MEASURE ONLY THIS)
            // ==================================================
            long processingStart = System.currentTimeMillis();

            walletService.processFromOrchestrator(event);

            long processingTime = System.currentTimeMillis() - processingStart;

            log.info("✅ Wallet processing SUCCESS | txnId={} | partition={} | offset={}",
                    transactionId, record.partition(), record.offset());

            // ==================================================
            // METRICS (SUCCESS + LATENCY)
            // ==================================================
            metrics.incrementConsumerSuccess(topic);
            metrics.recordProcessingTime(topic, processingTime);

            // ==================================================
            // STEP 4: ACKNOWLEDGE MESSAGE
            // ==================================================
            ack.acknowledge();

        } catch (Exception ex) {
            metrics.incrementConsumerFailure(topic);

            log.error("❌ PROCESSING FAILED → retry → DLQ | txnId={}", transactionId, ex);

            throw new RuntimeException("Processing failed", ex);

        } finally {
            // ==================================================
            // STEP 5: CLEAN TRACE CONTEXT
            // ==================================================
            TraceUtil.clear();
        }
    }
}