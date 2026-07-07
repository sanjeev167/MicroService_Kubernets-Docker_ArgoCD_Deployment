package com.orchestrator.metrics;

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
 * KAFKA METRICS SERVICE (ORCHESTRATOR - FINAL STANDARD)
 * ==========================================================
 *
 * AUTHOR: Sanjeev Kumar
 *
 * ==========================================================
 * 🎯 WHAT THIS CLASS DOES
 * ==========================================================
 *
 * Centralized observability layer for ALL Kafka interactions
 * inside the Transaction Orchestrator.
 *
 * This includes:
 *   ✔ Consumers (Saga step handling)
 *   ✔ Producers (Saga step triggering)
 *   ✔ DLQ handling
 *
 * WHY:
 *   - Maintain consistency across microservices
 *   - Avoid duplication of Micrometer code
 *   - Enable Prometheus + Grafana observability
 *
 * ==========================================================
 * 📊 METRIC TYPES
 * ==========================================================
 *
 * 🔹 CONSUMER METRICS
 * ------------------------------------------
 * kafka_consumer_success_total
 * kafka_consumer_failure_total
 * kafka_consumer_dlq_total
 * kafka_dlq_replay_total
 * kafka_consumer_processing_seconds
 *
 * 🔹 PRODUCER METRICS
 * ------------------------------------------
 * kafka_producer_success_total
 * kafka_producer_failure_total
 * kafka_producer_send_seconds
 *
 * ==========================================================
 * 🏷 TAGGING STRATEGY
 * ==========================================================
 *
 * service = orchestrator
 * topic   = kafka topic name
 *
 * Example:
 * kafka_producer_success_total{service="orchestrator", topic="wallet-request-topic"}
 *
 * ⚠ DO NOT ADD:
 *   ❌ traceId
 *   ❌ transactionId
 *   ❌ userId
 *
 * WHY:
 * High cardinality → Prometheus memory issues
 *
 * ==========================================================
 * ⚙ DESIGN PRINCIPLES
 * ==========================================================
 *
 * ✔ Cached meters (performance optimized)
 * ✔ Thread-safe (ConcurrentHashMap)
 * ✔ Prometheus naming compliant
 * ✔ Reusable across entire orchestrator
 *
 * ==========================================================
 * 🧠 HOW TO USE
 * ==========================================================
 *
 * CONSUMER:
 *   metrics.incrementConsumerSuccess(topic);
 *   metrics.incrementConsumerFailure(topic);
 *   metrics.recordProcessingTime(topic, duration);
 *
 * PRODUCER:
 *   metrics.incrementProducerSuccess(topic);
 *   metrics.incrementProducerFailure(topic);
 *   metrics.recordProducerTime(topic, duration);
 *
 * ==========================================================
 */
@Service
@RequiredArgsConstructor
public class KafkaMetricsService {

    private final MeterRegistry meterRegistry;

    private static final String SERVICE = "orchestrator";

    // ==========================================================
    // INTERNAL CACHE
    // ==========================================================
    private final Map<String, Counter> counterCache = new ConcurrentHashMap<>();
    private final Map<String, Timer> timerCache = new ConcurrentHashMap<>();

    // ==========================================================
    // INTERNAL FACTORY METHODS
    // ==========================================================

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

    public void incrementConsumerSuccess(String topic) {
        getCounter("kafka_consumer_success_total", topic).increment();
    }

    public void incrementConsumerFailure(String topic) {
        getCounter("kafka_consumer_failure_total", topic).increment();
    }

    public void incrementConsumerDlq(String topic) {
        getCounter("kafka_consumer_dlq_total", topic).increment();
    }

    public void incrementReplay(String topic) {
        getCounter("kafka_dlq_replay_total", topic).increment();
    }

    public void recordProcessingTime(String topic, long durationMs) {
        getTimer("kafka_consumer_processing_seconds", topic)
                .record(durationMs, TimeUnit.MILLISECONDS);
    }

    // ==========================================================
    // PRODUCER METRICS (REQUIRED FOR ORCHESTRATOR)
    // ==========================================================

    /**
     * Increment successful Kafka publish count
     */
    public void incrementProducerSuccess(String topic) {
        getCounter("kafka_producer_success_total", topic).increment();
    }

    /**
     * Increment failed Kafka publish count
     */
    public void incrementProducerFailure(String topic) {
        getCounter("kafka_producer_failure_total", topic).increment();
    }

    /**
     * Record Kafka send latency (producer side)
     *
     * @param topic Kafka topic
     * @param durationMs Send time in milliseconds
     */
    public void recordProducerTime(String topic, long durationMs) {
        getTimer("kafka_producer_send_seconds", topic)
                .record(durationMs, TimeUnit.MILLISECONDS);
    }
}