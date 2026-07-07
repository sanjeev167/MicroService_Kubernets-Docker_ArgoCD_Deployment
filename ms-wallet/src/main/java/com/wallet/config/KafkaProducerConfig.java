package com.wallet.config;

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
     * PRODUCER FACTORY (WALLET SERVICE)
     * ==========================================================
     *
     * PURPOSE:
     * - Publish wallet response events
     * - Support DLQ publishing (via error handler)
     *
     * DESIGN PRINCIPLES:
     * ✔ Strong reliability (wallet is critical)
     * ✔ Idempotent delivery (no duplicate events)
     * ✔ Controlled retry (avoid long blocking)
     *
     * USED BY:
     * - Wallet event producers
     * - DLQ mechanism (DeadLetterPublishingRecoverer)
     *
     * SAGA CONTEXT:
     * - Wallet is part of distributed transaction
     * - Failures should propagate, not be hidden by infinite retry
     */
    @Bean
    public ProducerFactory<String, Object> producerFactory() {

        Map<String, Object> config = new HashMap<>();

        // --------------------------------------------------
        // BASIC CONFIG
        // --------------------------------------------------
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        /**
         * Helps identify producer in Kafka logs
         */
        config.put(ProducerConfig.CLIENT_ID_CONFIG, "wallet-service");

        // --------------------------------------------------
        // SERIALIZATION
        // --------------------------------------------------
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        /**
         * Avoid type headers for cross-service compatibility
         */
        config.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);

        // --------------------------------------------------
        // RELIABILITY (CRITICAL)
        // --------------------------------------------------

        /**
         * Wait for leader + ISR acknowledgment
         */
        config.put(ProducerConfig.ACKS_CONFIG, "all");

        /**
         * Controlled retry (instead of infinite)
         */
        config.put(ProducerConfig.RETRIES_CONFIG, 5);

        /**
         * Idempotent producer (MANDATORY for saga)
         */
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        /**
         * Required for idempotence (≤ 5)
         */
        config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

        // --------------------------------------------------
        // TIMEOUTS
        // --------------------------------------------------

        /**
         * Upper bound for delivery attempts
         */
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000);

        /**
         * Request timeout to broker
         */
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);

        // --------------------------------------------------
        // PERFORMANCE
        // --------------------------------------------------

        /**
         * Small batching for efficiency
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
     * - Send wallet response events
     * - Used by DLQ mechanism internally
     *
     * IMPORTANT:
     * - Must remain Object type for DLQ compatibility
     */
    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}