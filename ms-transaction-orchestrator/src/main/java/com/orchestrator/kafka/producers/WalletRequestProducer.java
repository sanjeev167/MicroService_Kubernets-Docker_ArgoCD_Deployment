package com.orchestrator.kafka.producers;

import com.orchestrator.event.WalletRequestEvent;
import com.orchestrator.metrics.KafkaMetricsService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * ==========================================================
 * WALLET REQUEST PRODUCER (ORCHESTRATOR - FINAL)
 * ==========================================================
 *
 * AUTHOR: Sanjeev Kumar
 *
 * ==========================================================
 * 🎯 WHAT THIS CLASS DOES
 * ==========================================================
 *
 * This producer publishes WalletRequestEvent to Kafka and
 * initiates the Saga workflow.
 *
 * Responsibilities:
 *   1. Validate event before sending
 *   2. Publish event to Kafka (trace-enabled)
 *   3. Record producer metrics (success/failure/latency)
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
 * FLOW:
 * Orchestrator → Kafka → Wallet Service
 *
 * ==========================================================
 * 📊 METRICS DESIGN
 * ==========================================================
 *
 * ✔ kafka_producer_success_total
 * ✔ kafka_producer_failure_total
 * ✔ kafka_producer_send_seconds
 *
 * WHY IMPORTANT:
 *   - Detect Kafka outages
 *   - Measure producer latency
 *   - Track reliability of saga initiation
 *
 * ==========================================================
 * ⚠ DESIGN PRINCIPLES
 * ==========================================================
 *
 * ✔ Fail-fast (critical for Saga start)
 * ✔ No business logic inside producer
 * ✔ Reusable via BaseKafkaProducer
 * ✔ Clean separation of concerns
 *
 * ==========================================================
 */
@Service
public class WalletRequestProducer extends BaseKafkaProducer {

    private static final Logger log = LoggerFactory.getLogger(WalletRequestProducer.class);

    private final String requestTopic;
    private final KafkaMetricsService metrics;

    /**
     * Constructor
     */
    public WalletRequestProducer(
            KafkaTemplate<String, Object> kafkaTemplate,
            KafkaMetricsService metrics,
            @Value("${wallet.topic.request}") String requestTopic
    ) {
        super(kafkaTemplate);
        this.metrics = metrics;
        this.requestTopic = requestTopic;
    }

    /**
     * ======================================================
     * SEND WALLET REQUEST EVENT
     * ======================================================
     *
     * Flow:
     *   1. Validate event
     *   2. Log request
     *   3. Send to Kafka (trace-enabled)
     *   4. Record metrics
     */
    public void sendWalletRequest(WalletRequestEvent event) {

        long startTime = System.currentTimeMillis();

        // ==================================================
        // STEP 1: VALIDATION
        // ==================================================
        if (event == null ||
            event.getTransactionId() == null || event.getTransactionId().isBlank() ||
            event.getWalletId() == null || event.getWalletId().isBlank() ||
            event.getAmount() == null ||
            event.getWalletOperationType() == null ||
            event.getUserId() == null || event.getUserId().isBlank()) {

            log.error("❌ INVALID WALLET EVENT - NOT SENDING | event={}", event);
            throw new IllegalArgumentException("Invalid WalletRequestEvent");
        }

        final String transactionId = event.getTransactionId();

        // ==================================================
        // STEP 2: LOG
        // ==================================================
        log.info("📤 SENDING WALLET REQUEST | topic={} | txnId={} | walletId={} | amount={} | operationType={}",
                requestTopic,
                transactionId,
                event.getWalletId(),
                event.getAmount(),
                event.getWalletOperationType());

        log.debug("Wallet request payload: {}", event);

        try {
            // ==================================================
            // STEP 3: SEND (TRACE ENABLED)
            // ==================================================
            sendWithTrace(requestTopic, transactionId, event);

            // ==================================================
            // SUCCESS METRIC
            // ==================================================
            metrics.incrementProducerSuccess(requestTopic);

        } catch (Exception ex) {

            // ==================================================
            // FAILURE METRIC
            // ==================================================
            metrics.incrementProducerFailure(requestTopic);

            log.error("❌ KAFKA SEND FAILURE | topic={} | txnId={}",
                    requestTopic, transactionId, ex);

            // 🔥 FAIL FAST (CRITICAL FOR SAGA)
            throw new RuntimeException("Kafka send failed", ex);

        } finally {

            // ==================================================
            // LATENCY METRIC
            // ==================================================
            long duration = System.currentTimeMillis() - startTime;
            metrics.recordProducerTime(requestTopic, duration);
        }
    }
}