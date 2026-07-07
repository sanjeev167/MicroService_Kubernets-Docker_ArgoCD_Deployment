package com.wallet.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * ==========================================================
 * KAFKA METRICS SERVICE (WALLET SERVICE)
 * ==========================================================
 *
 * AUTHOR: Sanjeev Kumar
 * DATE:   June 16, 2026
 *
 * ==========================================================
 * 🎯 WHAT THIS CLASS DOES
 * ==========================================================
 *
 * This class provides a centralized way to record Kafka-related
 * metrics for the Wallet microservice.
 *
 * It is used by:
 *   ✔ Kafka Consumers (incoming messages)
 *   ✔ Kafka Producers (outgoing messages)
 *
 * WHY THIS EXISTS:
 *   - Avoid duplicate metric code across services
 *   - Ensure consistent naming across microservices
 *   - Keep business logic free from Micrometer dependencies
 *
 * ==========================================================
 * 📊 METRIC TYPES USED
 * ==========================================================
 *
 * 1. COUNTERS (Monotonic increasing values)
 * ------------------------------------------
 * Used to track:
 *   - Success count
 *   - Failure count
 *   - DLQ count
 *
 * Example:
 *   kafka_consumer_success_total
 *
 *
 * 2. TIMERS (Latency measurement)
 * ------------------------------------------
 * Used to track:
 *   - Processing time (consumer)
 *   - Send time (producer)
 *
 * IMPORTANT:
 *   - Values are recorded in milliseconds
 *   - Prometheus converts them to seconds internally
 *
 * ==========================================================
 * 📊 METRIC NAMES (PROMETHEUS STANDARD)
 * ==========================================================
 *
 * CONSUMER:
 *   kafka_consumer_success_total
 *   kafka_consumer_failure_total
 *   kafka_consumer_dlq_total
 *   kafka_consumer_processing_seconds
 *
 * PRODUCER:
 *   kafka_producer_success_total
 *   kafka_producer_failure_total
 *   kafka_producer_send_seconds
 *
 * ==========================================================
 * 🏷 TAGGING STRATEGY (VERY IMPORTANT)
 * ==========================================================
 *
 * All metrics include:
 *
 *   service = wallet
 *   topic   = kafka topic name
 *
 * Example:
 *   kafka_consumer_success_total{service="wallet", topic="wallet-request-topic"}
 *
 * ⚠ DO NOT ADD HIGH CARDINALITY TAGS:
 *   ❌ traceId
 *   ❌ userId
 *   ❌ transactionId
 *
 * WHY:
 *   High cardinality → Prometheus memory + performance issues
 *
 * ==========================================================
 * ⚙ DESIGN PRINCIPLES
 * ==========================================================
 *
 * ✔ Meter caching (avoid re-creating metrics repeatedly)
 * ✔ Thread-safe (ConcurrentHashMap)
 * ✔ Reusable across all Kafka components
 * ✔ Clean abstraction (no Micrometer in business logic)
 * ✔ Production-safe (low overhead)
 *
 * ==========================================================
 * 🧠 HOW TO USE THIS CLASS
 * ==========================================================
 *
 * Consumer Example:
 *   metrics.incrementConsumerSuccess(topic);
 *   metrics.recordProcessingTime(topic, time);
 *
 * Producer Example:
 *   metrics.incrementProducerSuccess(topic);
 *   metrics.recordProducerTime(topic, time);
 *
 * ==========================================================
 */
@Service
@RequiredArgsConstructor
public class KafkaMetricsService {

    private final MeterRegistry meterRegistry;

    // Constant tag for service identification
    private static final String SERVICE = "wallet";

    // ==========================================================
    // INTERNAL CACHE (PERFORMANCE OPTIMIZATION)
    // ==========================================================
    // Prevents re-registering metrics multiple times

    private final Map<String, Counter> counterCache = new ConcurrentHashMap<>();
    private final Map<String, Timer> timerCache = new ConcurrentHashMap<>();

    // ==========================================================
    // INTERNAL HELPER METHODS
    // ==========================================================

    /**
     * Returns a cached or newly created Counter.
     */
    private Counter getCounter(String metricName, String topic) {
        String safeTopic = (topic != null) ? topic : "unknown";
        String key = metricName + "|" + safeTopic;

        return counterCache.computeIfAbsent(key, k ->
                Counter.builder(metricName)
                        .description("Kafka metric: " + metricName)
                        .tag("service", SERVICE)
                        .tag("topic", safeTopic)
                        .register(meterRegistry)
        );
    }

    /**
     * Returns a cached or newly created Timer.
     */
    private Timer getTimer(String metricName, String topic) {
        String safeTopic = (topic != null) ? topic : "unknown";
        String key = metricName + "|" + safeTopic;

        return timerCache.computeIfAbsent(key, k ->
                Timer.builder(metricName)
                        .description("Kafka latency metric: " + metricName)
                        .tag("service", SERVICE)
                        .tag("topic", safeTopic)
                        .register(meterRegistry)
        );
    }

    // ==========================================================
    // CONSUMER METRICS
    // ==========================================================

    /**
     * Increment successful Kafka message processing count.
     */
    public void incrementConsumerSuccess(String topic) {
        getCounter("kafka_consumer_success_total", topic).increment();
    }

    /**
     * Increment failed Kafka message processing count.
     */
    public void incrementConsumerFailure(String topic) {
        getCounter("kafka_consumer_failure_total", topic).increment();
    }

    /**
     * Increment Dead Letter Queue (DLQ) count.
     */
    public void incrementConsumerDlq(String topic) {
        getCounter("kafka_consumer_dlq_total", topic).increment();
    }

    // ==========================================================
    // PRODUCER METRICS
    // ==========================================================

    /**
     * Increment successful Kafka publish count.
     */
    public void incrementProducerSuccess(String topic) {
        getCounter("kafka_producer_success_total", topic).increment();
    }

    /**
     * Increment failed Kafka publish count.
     */
    public void incrementProducerFailure(String topic) {
        getCounter("kafka_producer_failure_total", topic).increment();
    }

    // ==========================================================
    // LATENCY METRICS
    // ==========================================================

    /**
     * Record time taken to process a Kafka message (consumer side).
     *
     * @param topic Kafka topic
     * @param durationMs Processing time in milliseconds
     */
    public void recordProcessingTime(String topic, long durationMs) {
        getTimer("kafka_consumer_processing_seconds", topic)
                .record(durationMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Record time taken to send a Kafka message (producer side).
     *
     * @param topic Kafka topic
     * @param durationMs Send time in milliseconds
     */
    public void recordProducerTime(String topic, long durationMs) {
        getTimer("kafka_producer_send_seconds", topic)
                .record(durationMs, TimeUnit.MILLISECONDS);
    }
}