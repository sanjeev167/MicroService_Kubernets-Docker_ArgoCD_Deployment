package com.orchestrator.kafka.producers;

import com.orchestrator.event.WalletCompensationRequestEvent;
import com.orchestrator.metrics.KafkaMetricsService;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * ==========================================================
 * WALLET COMPENSATION PRODUCER (ORCHESTRATOR - FINAL)
 * ==========================================================
 *
 * AUTHOR: Sanjeev Kumar
 *
 * ==========================================================
 * 🎯 WHAT THIS CLASS DOES
 * ==========================================================
 *
 * This producer publishes WalletCompensationRequestEvent to Kafka
 * to trigger wallet reversal (refund) when Saga fails.
 *
 * Responsibilities:
 *   1. Validate compensation request
 *   2. Build compensation event
 *   3. Publish event to Kafka (trace-enabled)
 *   4. Record producer metrics (success/failure/latency)
 *
 * ==========================================================
 * 🔍 ROLE IN SAGA FLOW
 * ==========================================================
 *
 * Wallet SUCCESS
 *   ↓
 * Notification FAILED
 *   ↓
 * Orchestrator triggers compensation
 *   ↓
 * WalletCompensationProducer (THIS CLASS)
 *   ↓
 * Kafka
 *   ↓
 * Wallet Service (Reversal)
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
 * NOTE:
 * traceId NOT included in payload (keeps domain clean)
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
 *   - Compensation is CRITICAL path
 *   - Failures must be visible immediately
 *
 * ==========================================================
 * ⚠ DESIGN PRINCIPLES
 * ==========================================================
 *
 * ✔ Fail-fast (VERY CRITICAL for compensation flow)
 * ✔ No business logic inside producer
 * ✔ TransactionId used as Kafka key (ordering guarantee)
 * ✔ Observability-first design
 *
 * ==========================================================
 */
@Service
@Slf4j
public class WalletCompensationProducer extends BaseKafkaProducer {

    private final String compensationRequestTopic;
    private final KafkaMetricsService metrics;

    /**
     * Constructor
     */
    public WalletCompensationProducer(
            KafkaTemplate<String, Object> kafkaTemplate,
            KafkaMetricsService metrics,
            @Value("${wallet.topic.compensation-request}") String compensationRequestTopic
    ) {
        super(kafkaTemplate);
        this.metrics = metrics;
        this.compensationRequestTopic = compensationRequestTopic;
    }

    /**
     * ======================================================
     * SEND COMPENSATION REQUEST
     * ======================================================
     *
     * Flow:
     *   1. Validate input
     *   2. Create event
     *   3. Send to Kafka (trace-enabled)
     *   4. Record metrics
     *
     * @param transactionId unique saga transaction identifier
     */
    public void sendWalletCompensationRequest(String transactionId) {

        long startTime = System.currentTimeMillis();

        // ==================================================
        // STEP 1: VALIDATION
        // ==================================================
        if (transactionId == null || transactionId.isBlank()) {
            log.error("❌ INVALID COMPENSATION REQUEST | transactionId is null/blank");
            throw new IllegalArgumentException("transactionId cannot be null or blank");
        }

        // ==================================================
        // STEP 2: EVENT CREATION
        // ==================================================
        WalletCompensationRequestEvent event =
                WalletCompensationRequestEvent.builder()
                        .transactionId(transactionId)
                        .build();

        // ==================================================
        // STEP 3: LOG
        // ==================================================
        log.info("📤 SENDING COMPENSATION REQUEST | topic={} | txnId={}",
                compensationRequestTopic, transactionId);

        log.debug("Compensation request payload: {}", event);

        try {
            // ==================================================
            // STEP 4: SEND (TRACE ENABLED)
            // ==================================================
            sendWithTrace(compensationRequestTopic, transactionId, event);

            // ==================================================
            // SUCCESS METRIC
            // ==================================================
            metrics.incrementProducerSuccess(compensationRequestTopic);

        } catch (Exception ex) {

            // ==================================================
            // FAILURE METRIC
            // ==================================================
            metrics.incrementProducerFailure(compensationRequestTopic);

            log.error("❌ KAFKA SEND FAILURE | topic={} | txnId={}",
                    compensationRequestTopic, transactionId, ex);

            // 🔥 FAIL FAST (CRITICAL FOR COMPENSATION)
            throw new RuntimeException("Kafka send failed", ex);

        } finally {

            // ==================================================
            // LATENCY METRIC
            // ==================================================
            long duration = System.currentTimeMillis() - startTime;
            metrics.recordProducerTime(compensationRequestTopic, duration);
        }
    }
}