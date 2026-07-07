package com.orchestrator.entity;

import java.time.Instant;

import jakarta.persistence.*;
import lombok.*;

/**
 * ==========================================================
 * DLQ EVENT ENTITY (FINAL - PRODUCTION GRADE)
 * ==========================================================
 *
 * PURPOSE:
 * - Persist all permanently failed Kafka messages (DLQ)
 * - Provide full observability for debugging and audit
 * - Enable safe, controlled, and traceable replay of failed events
 *
 * TABLE: dlq_event
 *
 * DESIGN PRINCIPLES:
 * ✔ Zero data loss (store complete payload)
 * ✔ Kafka traceability (topic, partition, offset)
 * ✔ Error visibility (exception message)
 * ✔ Fast querying (indexed business fields)
 * ✔ Replay safety (prevent duplicate execution)
 * ✔ Audit trail (created + replay timestamps)
 *
 * SYSTEM FLOW:
 * Kafka Consumer Failure
 *        ↓
 * Retry (DefaultErrorHandler)
 *        ↓
 * DeadLetterPublishingRecoverer
 *        ↓
 * <topic>.DLQ
 *        ↓
 * DlqConsumer
 *        ↓
 * dlq_event table (this entity)
 *
 * OPERATIONAL USAGE:
 * - Debug failed transactions
 * - Filter by transactionId / eventType
 * - Identify failure patterns
 * - Replay failed events safely
 *
 * IMPORTANT:
 * - Payload is stored as raw JSON string
 * - Replay uses original topic + transactionId as key
 */
@Entity
@Table(
    name = "dlq_event",
    indexes = {
        @Index(name = "idx_dlq_txn_id", columnList = "transaction_id"),
        @Index(name = "idx_dlq_event_type", columnList = "event_type"),
        @Index(name = "idx_dlq_replayed", columnList = "replayed")
    }
)
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DlqEvent {

    /**
     * ==========================================================
     * PRIMARY KEY
     * ==========================================================
     */
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private String id;

    /**
     * ==========================================================
     * ORIGINAL KAFKA METADATA
     * ==========================================================
     *
     * Identifies exact Kafka source of failure
     */
    @Column(name = "original_topic", nullable = false)
    private String originalTopic;

    @Column(name = "partition_id", nullable = false)
    private Integer partitionId;

    @Column(name = "offset_value", nullable = false)
    private Long offsetValue;

    /**
     * ==========================================================
     * MESSAGE PAYLOAD
     * ==========================================================
     *
     * Full JSON payload of failed message
     *
     * Used for:
     * - Debugging
     * - Replay
     */
    @Column(columnDefinition = "TEXT", nullable = false)
    private String payload;

    /**
     * ==========================================================
     * ERROR CONTEXT
     * ==========================================================
     *
     * Extracted from Kafka DLQ headers:
     * - kafka_dlt-exception-message
     */
    @Column(name = "error_message", columnDefinition = "TEXT")
    private String errorMessage;

    /**
     * ==========================================================
     * BUSINESS CONTEXT (INDEXED)
     * ==========================================================
     *
     * Extracted from payload for faster querying
     */
    @Column(name = "event_type")
    private String eventType;

    @Column(name = "transaction_id")
    private String transactionId;

    /**
     * ==========================================================
     * AUDIT INFORMATION
     * ==========================================================
     */
    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    /**
     * ==========================================================
     * REPLAY CONTROL
     * ==========================================================
     *
     * false → Not replayed yet
     * true  → Already replayed
     */
    @Column(name = "replayed", nullable = false)
    private Boolean replayed;

    /**
     * ==========================================================
     * REPLAY AUDIT
     * ==========================================================
     *
     * Timestamp when replay was triggered
     *
     * Helps:
     * - Track replay history
     * - Debug repeated failures
     */
    @Column(name = "replayed_at")
    private Instant replayedAt;
}