package com.notification.config;

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
     * PRODUCER FACTORY (NOTIFICATION SERVICE)
     * ==========================================================
     *
     * PURPOSE:
     * - Publish notification events
     * - Support DLQ publishing (via DeadLetterPublishingRecoverer)
     *
     * DESIGN PRINCIPLES:
     * ✔ Reliable but not over-engineered (notification is non-critical)
     * ✔ Idempotent to avoid duplicates
     * ✔ Compatible with DLQ publishing
     *
     * USED BY:
     * - Notification producers
     * - DLQ mechanism (error handler)
     */
    @Bean
    public ProducerFactory<String, Object> producerFactory() {

        Map<String, Object> config = new HashMap<>();

        // --------------------------------------------------
        // BASIC CONFIG
        // --------------------------------------------------
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        /**
         * Optional but useful for debugging in Kafka logs
         */
        config.put(ProducerConfig.CLIENT_ID_CONFIG, "notification-service");

        // --------------------------------------------------
        // SERIALIZATION
        // --------------------------------------------------
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        /**
         * Disable type headers → ensures cross-service compatibility
         */
        config.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);

        // --------------------------------------------------
        // RELIABILITY (BALANCED)
        // --------------------------------------------------

        /**
         * Wait for leader + ISR acknowledgment
         */
        config.put(ProducerConfig.ACKS_CONFIG, "all");

        /**
         * Limited retry (notification is not mission-critical)
         */
        config.put(ProducerConfig.RETRIES_CONFIG, 3);

        /**
         * Prevent duplicate messages on retry
         */
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        /**
         * Required for idempotence (≤ 5)
         */
        config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

        // --------------------------------------------------
        // TIMEOUTS
        // --------------------------------------------------
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 60000);
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 15000);

        // --------------------------------------------------
        // PERFORMANCE
        // --------------------------------------------------

        /**
         * Small batching for better throughput
         */
        config.put(ProducerConfig.LINGER_MS_CONFIG, 5);

        /**
         * Reduce network usage
         */
        config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        return new DefaultKafkaProducerFactory<>(config);
    }

    /**
     * ==========================================================
     * KAFKA TEMPLATE
     * ==========================================================
     *
     * PURPOSE:
     * - Send notification events
     * - Used internally by DLQ mechanism
     *
     * IMPORTANT:
     * - Must remain Object type for generic DLQ handling
     */
    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}