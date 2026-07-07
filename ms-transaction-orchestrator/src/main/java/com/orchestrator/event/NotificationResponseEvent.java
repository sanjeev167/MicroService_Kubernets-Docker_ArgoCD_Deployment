package com.orchestrator.event;

import java.time.Instant;

import com.orchestrator.enums.StepStatus;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * ==========================================================
 * EVENT: NotificationResponseEvent
 * SERVICE: ms-transaction-orchestrator
 * ==========================================================
 *
 * DESCRIPTION
 * ----------------------------------------------------------
 * Event published by the Notification Service after processing
 * a notification request. This event is consumed by the
 * Transaction Orchestrator to update Saga state.
 *
 * ROLE IN SAGA
 * ----------------------------------------------------------
 * ✔ Represents the outcome of the notification step
 * ✔ Updates Saga state to COMPLETED or FAILED
 * ✔ Drives workflow forward to completion or triggers compensation
 *
 * DESIGN PRINCIPLES
 * ----------------------------------------------------------
 * ✔ transactionId is the single source of truth across system
 * ✔ Strong typing for step status values
 * ✔ No redundant or unused fields
 * ✔ Fully traceable with metadata
 * ✔ Versioned & extensible for schema evolution
 *
 * EVENT CONTRACT (STRICT)
 * ----------------------------------------------------------
 * REQUIRED:
 * - transactionId → Global correlation ID (String)
 * - status        → Notification step status (COMPLETED/FAILED)
 *
 * OPTIONAL:
 * - errorMessage  → Failure reason (only if status = FAILED)
 * - createdAt     → Event timestamp
 * - source        → Origin system (e.g., NOTIFICATION-SERVICE)
 * - version       → Schema version for evolution
 *
 * VALIDATION RULES (ENFORCED IN CONSUMER)
 * ----------------------------------------------------------
 * - transactionId MUST NOT be null
 * - status MUST NOT be null
 * - errorMessage required only when status = FAILED
 *
 * ==========================================================
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class NotificationResponseEvent {

    /**
     * Global Transaction ID (Saga ID).
     * Represented as a String for flexibility across services.
     * Must be unique per Saga workflow.
     */
    private String transactionId;

    /**
     * Notification step status.
     * Possible values:
     * - COMPLETED
     * - FAILED
     */
    private StepStatus status;

    /**
     * Optional error message.
     * Populated only when status = FAILED.
     */
    private String errorMessage;

    /**
     * Event creation timestamp.
     * Captures when the Notification Service generated this event.
     */
    private Instant createdAt;

    /**
     * Event source system.
     * Example: "NOTIFICATION-SERVICE".
     */
    private String source;

    /**
     * Event schema version.
     * Used for schema evolution and backward compatibility.
     */
    private String version;
}
