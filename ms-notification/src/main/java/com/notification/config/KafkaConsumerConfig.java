package com.notification.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;

import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;

import org.springframework.util.backoff.FixedBackOff;

@EnableKafka
@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    /**
     * ==========================================================
     * CONSUMER FACTORY
     * ==========================================================
     *
     * PURPOSE:
     * - Configure Kafka consumer behavior
     * - Ensure manual offset control
     * - Optimize throughput for notification processing
     */
    @Bean
    public ConsumerFactory<String, String> consumerFactory() {

        Map<String, Object> config = new HashMap<>();

        // --------------------------------------------------
        // BASIC CONFIG
        // --------------------------------------------------
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // --------------------------------------------------
        // DESERIALIZATION
        // --------------------------------------------------
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // --------------------------------------------------
        // OFFSET CONTROL
        // --------------------------------------------------
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // --------------------------------------------------
        // PERFORMANCE (Notification is high-throughput)
        // --------------------------------------------------
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 20);
        config.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);

        // --------------------------------------------------
        // HEARTBEAT STABILITY
        // --------------------------------------------------
        config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 15000);
        config.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 5000);

        return new DefaultKafkaConsumerFactory<>(config);
    }

    /**
     * ==========================================================
     * ERROR HANDLER (LIMITED DLQ IMPLEMENTATION)
     * ==========================================================
     *
     * PURPOSE:
     * - Retry failed messages (limited attempts)
     * - Prevent infinite retry loops
     * - Send permanently failing messages to DLQ
     *
     * IMPORTANT:
     * - NO DB persistence here
     * - NO replay logic here
     * - ONLY stability (avoid consumer blockage)
     *
     * DLQ TOPIC:
     * - <original-topic>.DLQ
     */
    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, Object> kafkaTemplate) {

        DeadLetterPublishingRecoverer recoverer =
                new DeadLetterPublishingRecoverer(
                        kafkaTemplate,
                        (record, ex) -> {
                            String dlqTopic = record.topic() + ".DLQ";

                            System.err.println(
                                "🚨 NOTIFICATION → DLQ | topic=" + dlqTopic +
                                " | payload=" + record.value()
                            );

                            return new TopicPartition(dlqTopic, record.partition());
                        }
                );

        /**
         * Retry Strategy:
         * - 2 retries
         * - 1 second gap
         *
         * Total attempts = 1 (initial) + 2 retries
         */
        FixedBackOff backOff = new FixedBackOff(1000L, 2);

        return new DefaultErrorHandler(recoverer, backOff);
    }

    /**
     * ==========================================================
     * LISTENER CONTAINER FACTORY
     * ==========================================================
     *
     * FEATURES:
     * ✔ Manual ACK (controlled processing)
     * ✔ High concurrency (notification throughput)
     * ✔ DLQ enabled (via error handler)
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory,
            DefaultErrorHandler errorHandler) {

        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory);

        // --------------------------------------------------
        // MANUAL ACK
        // --------------------------------------------------
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);

        // --------------------------------------------------
        // HIGH CONCURRENCY (notification is scalable)
        // --------------------------------------------------
        factory.setConcurrency(5);

        // --------------------------------------------------
        // DLQ + RETRY HANDLER
        // --------------------------------------------------
        factory.setCommonErrorHandler(errorHandler);

        return factory;
    }
}