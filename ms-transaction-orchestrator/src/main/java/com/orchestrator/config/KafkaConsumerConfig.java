package com.orchestrator.config;

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
     * Purpose:
     * - Low batch processing
     * - Manual offset control
     * - Stable group rebalancing
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
        // OFFSET CONTROL (MANDATORY FOR SAGA)
        // --------------------------------------------------
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // --------------------------------------------------
        // POLLING CONTROL
        // --------------------------------------------------
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5);
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
     * DLQ ERROR HANDLER
     * ==========================================================
     *
     * Flow:
     * 1. Retry message 3 times
     * 2. If still failing → publish to DLQ
     * 3. Topic naming: <original-topic>.DLQ
     */
    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, Object> kafkaTemplate) {

        DeadLetterPublishingRecoverer recoverer =
                new DeadLetterPublishingRecoverer(
                        kafkaTemplate,
                        (record, ex) -> {
                            String dlqTopic = record.topic() + ".DLQ";

                            System.err.println(
                                "🚨 Sending to DLQ | topic=" + dlqTopic +
                                " | payload=" + record.value()
                            );

                            return new TopicPartition(dlqTopic, record.partition());
                        }
                );

        /**
         * Retry strategy:
         * - 3 retries
         * - 2 seconds gap
         */
        FixedBackOff backOff = new FixedBackOff(2000L, 3);

        return new DefaultErrorHandler(recoverer, backOff);
    }

    /**
     * ==========================================================
     * LISTENER CONTAINER FACTORY
     * ==========================================================
     *
     * Features:
     * ✔ Manual ACK (strict control)
     * ✔ DLQ enabled
     * ✔ Controlled concurrency
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory,
            DefaultErrorHandler errorHandler) {

        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory);

        // --------------------------------------------------
        // MANUAL ACK (REQUIRED FOR SAGA)
        // --------------------------------------------------
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);

        // --------------------------------------------------
        // CONCURRENCY CONTROL
        // --------------------------------------------------
        factory.setConcurrency(1);

        // --------------------------------------------------
        // DLQ + RETRY HANDLER
        // --------------------------------------------------
        factory.setCommonErrorHandler(errorHandler);

        return factory;
    }
}