package com.wallet.kafka.producers;

import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.wallet.event.WalletProcessedEvent;
import com.wallet.metrics.KafkaMetricsService;

/**
 * ==========================================================
 * WALLET RESPONSE PRODUCER (PRODUCTION READY - FINAL)
 * ==========================================================
 *
 * AUTHOR: Sanjeev Kumar
 * DATE:   June 16, 2026
 *
 * ==========================================================
 * 🎯 PURPOSE
 * ==========================================================
 *
 * - Publish WalletProcessedEvent to Orchestrator
 * - Maintain Saga continuity
 * - Provide full observability:
 *      ✔ Metrics (Micrometer + Prometheus)
 *      ✔ Logs (ELK)
 *      ✔ TraceId propagation (MDC → Kafka headers)
 *
 * ==========================================================
 * 🔍 TRACEABILITY DESIGN
 * ==========================================================
 *
 * ✔ Extends BaseKafkaProducer
 * ✔ traceId automatically:
 *     - Retrieved from MDC
 *     - Injected into Kafka headers
 * ✔ Enables distributed tracing:
 *     Wallet → Kafka → Orchestrator
 *
 * ==========================================================
 * 📊 OBSERVABILITY DESIGN
 * ==========================================================
 *
 * PRODUCER METRICS:
 * ------------------------------------------
 * ✔ kafka_producer_success_total
 * ✔ kafka_producer_failure_total
 * ✔ kafka_producer_send_seconds
 *
 * NOTE:
 * - Latency measured ONLY for Kafka send operation
 * - Does NOT include validation or enrichment time
 *
 * ==========================================================
 * ⚠ DESIGN PRINCIPLES
 * ==========================================================
 *
 * ✔ Fail-fast on Kafka send failure (critical for Saga)
 * ✔ No business logic inside producer
 * ✔ Clean abstraction via BaseKafkaProducer
 * ✔ Low cardinality metrics (no traceId in tags)
 *
 * ==========================================================
 */
@Service
public class WalletResponseProducer extends BaseKafkaProducer {

    private static final Logger log = LoggerFactory.getLogger(WalletResponseProducer.class);

    private final KafkaMetricsService metrics;
    private final String responseTopic;

    public WalletResponseProducer(
            KafkaTemplate<String, Object> kafkaTemplate,
            KafkaMetricsService metrics,
            @Value("${wallet.topic.response}") String responseTopic) {

        super(kafkaTemplate);
        this.metrics = metrics;
        this.responseTopic = responseTopic;
    }

    /**
     * ======================================================
     * SEND WALLET RESPONSE EVENT
     * ======================================================
     *
     * FLOW:
     *   1. Validate input
     *   2. Enrich event metadata
     *   3. Send to Kafka (trace-enabled)
     *   4. Emit metrics (success/failure + latency)
     *
     * @param event WalletProcessedEvent
     */
    public void sendWalletResponse(WalletProcessedEvent event) {

        // ==================================================
        // STEP 1: VALIDATION
        // ==================================================
        if (event == null ||
            event.getTransactionId() == null ||
            event.getTransactionId().toString().isBlank() ||
            event.getStatus() == null) {

            log.error("❌ INVALID WALLET RESPONSE EVENT | event={}", event);
            throw new IllegalArgumentException("Invalid WalletProcessedEvent");
        }

        if (responseTopic == null || responseTopic.isBlank()) {
            throw new IllegalStateException("Kafka topic not configured");
        }

        // ==================================================
        // STEP 2: ENRICH EVENT
        // ==================================================
        if (event.getCreatedAt() == null) {
            event.setCreatedAt(Instant.now());
        }
        if (event.getSource() == null) {
            event.setSource("WALLET_SERVICE");
        }
        if (event.getVersion() == null) {
            event.setVersion("v1");
        }

        final String transactionId = event.getTransactionId().toString();
        final String status = String.valueOf(event.getStatus());

        log.info("📤 SENDING WALLET RESPONSE | topic={} | txnId={} | status={}",
                responseTopic, transactionId, status);

        log.debug("Wallet response payload: {}", event);

        try {
            // ==================================================
            // STEP 3: KAFKA SEND (MEASURE ONLY SEND LATENCY)
            // ==================================================
            long sendStart = System.currentTimeMillis();

            sendWithTrace(responseTopic, transactionId, event);

            long duration = System.currentTimeMillis() - sendStart;

            // ==================================================
            // METRICS (SUCCESS + LATENCY)
            // ==================================================
            metrics.incrementProducerSuccess(responseTopic);
            metrics.recordProducerTime(responseTopic, duration);

        } catch (Exception ex) {

            // ==================================================
            // FAILURE METRIC
            // ==================================================
            metrics.incrementProducerFailure(responseTopic);

            log.error("❌ KAFKA SEND FAILURE | topic={} | txnId={} | status={}",
                    responseTopic, transactionId, status, ex);

            // ==================================================
            // FAIL FAST → REQUIRED FOR SAGA CONSISTENCY
            // ==================================================
            throw new RuntimeException("Kafka send failed", ex);
        }
    }
}