package com.wallet.kafka.producers;

import com.wallet.util.TraceUtil;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * ==========================================================
 * BASE KAFKA PRODUCER (TRACEABILITY FOUNDATION)
 * ==========================================================
 *
 * AUTHOR: Sanjeev Kumar  
 * DATE:   June 16, 2026
 *
 * PURPOSE:
 * - Provide common Kafka publishing functionality
 * - Centralize traceId propagation logic
 * - Avoid duplication across producers
 *
 * ==========================================================
 * TRACEABILITY DESIGN
 * ==========================================================
 *
 * ✔ Fetch traceId from MDC (via {@link TraceUtil})  
 * ✔ Generate traceId if missing (fallback safety)  
 * ✔ Attach traceId as Kafka header  
 *
 * BENEFITS:
 * - Consistent trace propagation across all producers  
 * - Cleaner producer implementations  
 * - Easier maintenance and extension  
 *
 * ==========================================================
 * USAGE
 * ==========================================================
 *
 * Extend this class in all Kafka producers:
 *
 * Example:
 * <pre>
 * public class WalletRequestProducer extends BaseKafkaProducer {
 *     public void send(WalletRequestEvent event) {
 *         sendWithTrace("wallet-request-topic", event.getTransactionId(), event);
 *     }
 * }
 * </pre>
 *
 * ==========================================================
 */
public abstract class BaseKafkaProducer {

    protected final KafkaTemplate<String, Object> kafkaTemplate;

    protected BaseKafkaProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * SEND MESSAGE WITH TRACE ID
     * --------------------------------------------------
     * - Automatically injects traceId into Kafka header  
     * - Ensures traceability across services  
     *
     * @param topic   Kafka topic name
     * @param key     Kafka partition key (e.g., transactionId)
     * @param payload Event payload to publish
     */
    protected void sendWithTrace(String topic, String key, Object payload) {

        // --------------------------------------------------
        // STEP 1: FETCH / GENERATE TRACE ID
        // --------------------------------------------------
        String traceId = TraceUtil.getTraceId();
        if (traceId == null) {
            traceId = TraceUtil.generateTraceId();
            TraceUtil.setTraceId(traceId);
        }

        // --------------------------------------------------
        // STEP 2: CREATE PRODUCER RECORD
        // --------------------------------------------------
        ProducerRecord<String, Object> record =
                new ProducerRecord<>(topic, key, payload);

        // --------------------------------------------------
        // STEP 3: ADD TRACE ID HEADER
        // --------------------------------------------------
        record.headers().add(new RecordHeader("traceId", traceId.getBytes()));

        // --------------------------------------------------
        // STEP 4: SEND ASYNC
        // --------------------------------------------------
        kafkaTemplate.send(record);
    }
}
