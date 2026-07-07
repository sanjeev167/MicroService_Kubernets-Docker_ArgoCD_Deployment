package com.notification.entity;

import java.time.LocalDateTime;
import java.util.UUID;

import com.notification.enums.NotificationStatus;

import jakarta.persistence.*;
import lombok.*;

/**
 * ==========================================================
 * Entity: NotificationTransaction
 * ==========================================================
 *
 * Stores notification processing records for idempotency.
 * Ensures duplicate Kafka messages do not trigger re-processing.
 *
 * ==========================================================
 */
@Entity
@Table(
    name = "notification",
    indexes = {
        @Index(name = "idx_notification_transaction_id", columnList = "transaction_id")
    }
)
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class NotificationTransaction {

    /**
     * PRIMARY KEY (unique per notification row)
     */
    @Id
    @Column(name = "notification_transaction_id", nullable = false, updatable = false)
    private UUID notificationTransactionId;

    /**
     * SAGA ID (shared across services)
     */
    @Column(name = "transaction_id", nullable = false)
    private UUID transactionId;

    /**
     * USER ID
     */
    @Column(name = "user_id", nullable = false)
    private UUID userId;

    /**
     * MESSAGE CONTENT
     */
    @Column(name = "message")
    private String message;

    /**
     * STATUS
     */
    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 30)
    private NotificationStatus status;

    /**
     * CREATED TIMESTAMP
     */
    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;

    /**
     * AUTO SET TIMESTAMP
     */
    @PrePersist
    public void prePersist() {
        if (createdAt == null) {
            createdAt = LocalDateTime.now();
        }
    }
}