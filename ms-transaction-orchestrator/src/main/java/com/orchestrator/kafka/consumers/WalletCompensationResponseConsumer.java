package com.orchestrator.kafka.consumers;

import java.nio.charset.StandardCharsets;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orchestrator.enums.StepStatus;
import com.orchestrator.event.WalletCompensationResponseEvent;
import com.orchestrator.metrics.KafkaMetricsService;
import com.orchestrator.service.SagaService;
import com.orchestrator.util.TraceUtil;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * ==========================================================
 * WALLET COMPENSATION RESPONSE CONSUMER (ORCHESTRATOR - FINAL)
 * ==========================================================
 *
 * AUTHOR: Sanjeev Kumar
 *
 * ==========================================================
 * 🎯 WHAT THIS CLASS DOES
 * ==========================================================
 *
 * This consumer handles WalletCompensationResponseEvent and
 * completes the Saga compensation flow.
 *
 * Responsibilities:
 *   1. Extract traceId for distributed tracing
 *   2. Deserialize Kafka message → WalletCompensationResponseEvent
 *   3. Validate event data
 *   4. Execute compensation logic:
 *        ✔ SUCCESS → mark Saga COMPENSATED
 *        ✔ FAILURE → mark Saga FAILED
 *   5. Record metrics (success/failure/latency)
 *   6. Acknowledge Kafka message safely
 *
 * ==========================================================
 * 🔍 TRACE FLOW
 * ==========================================================
 *
 * Wallet Service → Kafka (traceId header)
 *                → Orchestrator extracts traceId
 *                → TraceUtil.setTraceId(traceId)
 *                → Logs include traceId automatically
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
 * VALIDATION ERROR      → retry → DLQ
 * PROCESSING ERROR      → retry → DLQ
 *
 * BUSINESS OUTCOME:
 *   ✔ Compensation SUCCESS → Saga COMPENSATED → ACK
 *   ✔ Compensation FAILURE → Saga FAILED → ACK (handled case)
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
public class WalletCompensationResponseConsumer {

    private final SagaService sagaService;
    private final ObjectMapper objectMapper;
    private final KafkaMetricsService metrics;

    @Value("${wallet.topic.compensation-response}")
    private String topic;

    /**
     * ======================================================
     * MAIN KAFKA CONSUMER
     * ======================================================
     */
    @KafkaListener(
        topics = "${wallet.topic.compensation-response}",
        groupId = "${orchestrator.consumer.compensation-response-group}",
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

        log.info("📥 Received WalletCompensationResponseEvent");

        WalletCompensationResponseEvent event;

        try {
            // ==================================================
            // STEP 1: DESERIALIZATION
            // ==================================================
            event = objectMapper.readValue(message, WalletCompensationResponseEvent.class);

        } catch (Exception ex) {
            metrics.incrementConsumerFailure(topic);

            log.error("❌ DESERIALIZATION FAILED → retry → DLQ", ex);

            throw new RuntimeException("Invalid message format", ex);
        }

        // ==================================================
        // STEP 2: VALIDATION
        // ==================================================
        if (event.getSagaId() == null ||
            event.getTransactionId() == null ||
            event.getStatus() == null) {

            metrics.incrementConsumerFailure(topic);

            log.error("❌ INVALID EVENT → retry → DLQ");

            throw new RuntimeException("Invalid WalletCompensationResponseEvent");
        }

        String sagaId = event.getSagaId();
        String walletTxnId = event.getTransactionId();

        try {
            // ==================================================
            // STEP 3: BUSINESS PROCESSING (MEASURE ONLY THIS)
            // ==================================================
            long processingStart = System.currentTimeMillis();

            if (event.getStatus() == StepStatus.COMPLETED) {

                sagaService.handleCompensationSuccess(sagaId);

                log.info("✅ Compensation COMPLETED → Saga COMPENSATED | sagaId={} | walletTxnId={}",
                        sagaId, walletTxnId);

            } else {

                sagaService.handleCompensationFailure(sagaId, event.getErrorMessage());

                log.error("⚠ Compensation FAILED → Saga FAILED | sagaId={} | walletTxnId={}",
                        sagaId, walletTxnId);
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

            log.error("❌ PROCESSING FAILED → retry → may go to DLQ | sagaId={} | walletTxnId={}",
                    sagaId, walletTxnId, ex);

            throw new RuntimeException("Processing failed", ex);

        } finally {
            // ==================================================
            // STEP 4: CLEAN TRACE CONTEXT
            // ==================================================
            TraceUtil.clear();
        }
    }
}