package com.orchestrator.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import org.springframework.kafka.support.serializer.JsonSerializer;

@Configuration
public class KafkaProducerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    /**
     * ==========================================================
     * PRODUCER FACTORY
     * ==========================================================
     *
     * Purpose:
     * - Used by all producers including:
     *   ✔ Normal event publishing
     *   ✔ DLQ publishing (via DeadLetterPublishingRecoverer)
     *
     * Design Goals:
     * - Idempotent (no duplicate events)
     * - Reliable delivery
     * - Safe JSON serialization
     * - Compatible with DLQ
     */
    @Bean
    public ProducerFactory<String, Object> producerFactory() {

        Map<String, Object> config = new HashMap<>();

        // --------------------------------------------------
        // BASIC CONFIG
        // --------------------------------------------------
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // --------------------------------------------------
        // SERIALIZATION
        // --------------------------------------------------
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        /**
         * Disable type headers to avoid cross-service class issues.
         * Ensures plain JSON payload compatibility.
         */
        config.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);

        // --------------------------------------------------
        // RELIABILITY (STRONG GUARANTEE)
        // --------------------------------------------------

        /**
         * "all" ensures leader + ISR replication before ACK
         */
        config.put(ProducerConfig.ACKS_CONFIG, "all");

        /**
         * Idempotence guarantees:
         * - No duplicate messages during retries
         */
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        /**
         * Retry for transient failures
         */
        config.put(ProducerConfig.RETRIES_CONFIG, 5);

        /**
         * Must be <= 5 for idempotence
         */
        config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

        // --------------------------------------------------
        // TIMEOUTS (FAIL FAST BUT SAFE)
        // --------------------------------------------------

        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 60000);
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 15000);

        // --------------------------------------------------
        // PERFORMANCE (BALANCED)
        // --------------------------------------------------

        /**
         * Small batching delay for better throughput
         */
        config.put(ProducerConfig.LINGER_MS_CONFIG, 5);

        /**
         * Compression reduces network load
         */
        config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        return new DefaultKafkaProducerFactory<>(config);
    }

    /**
     * ==========================================================
     * KAFKA TEMPLATE
     * ==========================================================
     *
     * Used by:
     * - Event Producers
     * - DLQ Publisher (DeadLetterPublishingRecoverer)
     *
     * Important:
     * - Must remain Object type for generic DLQ handling
     */
    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}