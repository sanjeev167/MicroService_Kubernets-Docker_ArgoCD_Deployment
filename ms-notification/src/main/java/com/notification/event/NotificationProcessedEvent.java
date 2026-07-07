package com.notification.event;

import java.time.Instant;

import com.notification.enums.NotificationStatus;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * ==========================================================
 * EVENT: NotificationProcessedEvent
 * ==========================================================
 *
 * DESCRIPTION
 * ----------------------------------------------------------
 * Event published by the Notification Service after processing
 * a notification request. This event is sent back to the
 * Transaction Orchestrator as part of the Saga workflow.
 *
 * ROLE IN SAGA
 * ----------------------------------------------------------
 * ✔ Represents the outcome of the notification step
 * ✔ Updates Saga state in the Orchestrator
 * ✔ Drives workflow forward to completion or triggers compensation
 *
 * DESIGN PRINCIPLES
 * ----------------------------------------------------------
 * ✔ Global transactionId represented as String (Saga ID)
 * ✔ Strong typing for status values
 * ✔ Idempotent-safe (duplicate events can be ignored)
 * ✔ Extensible metadata for traceability and schema evolution
 *
 * USAGE
 * ----------------------------------------------------------
 * - Produced by Notification Service after sending Email/SMS/Push
 * - Consumed by Orchestrator to mark Saga COMPLETED or COMPENSATING
 * - Ensures observability with metadata fields
 *
 * ==========================================================
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class NotificationProcessedEvent {

    /**
     * Global Transaction ID (Saga ID).
     * Represented as a String for flexibility across services.
     * Must be unique per Saga workflow.
     */
    private String transactionId;

    /**
     * Processing status of the notification step.
     * Possible values:
     * - SUCCESS
     * - FAILED
     */
    private NotificationStatus status;

    /**
     * Optional descriptive message about the outcome.
     * Example: "Email sent successfully" or "SMS gateway unavailable".
     */
    private String message;

    /**
     * Event creation timestamp.
     * Captures when the Notification Service generated this event.
     */
    private Instant createdAt;

    /**
     * Source service identifier.
     * Example: "NOTIFICATION-SERVICE".
     */
    private String source;

    /**
     * Schema version for event evolution.
     * Example: "v1".
     */
    private String version;
}
