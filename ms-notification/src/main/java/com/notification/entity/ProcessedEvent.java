package com.notification.entity;



import java.time.LocalDateTime;
import java.util.UUID;

import jakarta.persistence.*;
import lombok.*;

/**
 * ==========================================================
 * Entity: ProcessedEvent
 * ==========================================================
 *
 * 🎯 PURPOSE
 * ----------------------------------------------------------
 * Tracks processed Kafka events for idempotency.
 *
 * ✔ Prevents duplicate processing
 * ✔ Ensures exactly-once behavior at application level
 * ✔ Works across all services (wallet, notification, etc.)
 *
 * ==========================================================
 *
 * 🧱 DB MAPPING (processed_events)
 * ----------------------------------------------------------
 * id              → UUID (PK)
 * transaction_id  → UUID (Saga ID)
 * event_type      → VARCHAR(50)
 * service_name    → VARCHAR(50)
 * processed_at    → TIMESTAMP
 *
 * UNIQUE(transaction_id, event_type, service_name)
 *
 * ==========================================================
 */
@Entity
@Table(
    name = "processed_events",
    uniqueConstraints = {
        @UniqueConstraint(
            name = "processed_events_transaction_id_event_type_service_name_key",
            columnNames = {"transaction_id", "event_type", "service_name"}
        )
    }
)
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ProcessedEvent {

    /**
     * PRIMARY KEY
     */
    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private UUID id;

    /**
     * SAGA ID (Global transaction ID)
     */
    @Column(name = "transaction_id", nullable = false)
    private UUID transactionId;

    /**
     * EVENT TYPE (e.g., WALLET_DEBITED, NOTIFICATION_SENT)
     */
    @Column(name = "event_type", nullable = false, length = 50)
    private String eventType;

    /**
     * SERVICE NAME (e.g., WALLET_SERVICE, NOTIFICATION_SERVICE)
     */
    @Column(name = "service_name", nullable = false, length = 50)
    private String serviceName;

    /**
     * PROCESSED TIMESTAMP
     */
    @Column(name = "processed_at")
    private LocalDateTime processedAt;

    /**
     * AUTO SET TIMESTAMP BEFORE INSERT
     */
    @PrePersist
    public void prePersist() {
        if (processedAt == null) {
            processedAt = LocalDateTime.now();
        }
    }
}