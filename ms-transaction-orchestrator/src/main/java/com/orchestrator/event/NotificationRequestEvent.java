package com.orchestrator.event;

import java.time.Instant;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * ==========================================================
 * EVENT: NotificationRequestEvent
 * SERVICE: ms-transaction-orchestrator
 * ==========================================================
 *
 * DESCRIPTION
 * ----------------------------------------------------------
 * Event published by the Transaction Orchestrator to the
 * Notification Service. This event triggers user notification
 * processing (Email, SMS, Push) after wallet success.
 *
 * ROLE IN SAGA
 * ----------------------------------------------------------
 * ✔ Initiates notification step in Saga workflow
 * ✔ Carries required context (transactionId, userId, message)
 * ✔ Ensures correlation with Saga via transactionId
 *
 * DESIGN PRINCIPLES
 * ----------------------------------------------------------
 * ✔ transactionId is the single source of truth across system
 * ✔ No redundant or unused fields
 * ✔ Fully traceable with metadata
 * ✔ Versioned & extensible for schema evolution
 *
 * EVENT CONTRACT (STRICT)
 * ----------------------------------------------------------
 * REQUIRED:
 * - transactionId → Global correlation ID (String)
 * - userId        → Target user identifier
 * - message       → Notification content
 *
 * OPTIONAL:
 * - createdAt     → Event timestamp
 * - source        → Origin system (e.g., ORCHESTRATOR)
 * - version       → Schema version for evolution
 *
 * VALIDATION RULES (ENFORCED IN CONSUMER)
 * ----------------------------------------------------------
 * - transactionId MUST NOT be null
 * - userId MUST NOT be null
 * - message MUST NOT be null or empty
 *
 * ==========================================================
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class NotificationRequestEvent {

    /**
     * Global Transaction ID (Saga ID).
     * Represented as a String for flexibility across services.
     * Must be unique per Saga workflow.
     */
    private String transactionId;

    /**
     * Target user identifier.
     * Represents the recipient of the notification.
     */
    private String userId;

    /**
     * Notification message content.
     * Contains the actual text or payload to be delivered
     * to the user (e.g., SMS, Email, Push).
     */
    private String message;

    /**
     * Event creation timestamp.
     * Optional field for observability and ordering.
     */
    private Instant createdAt;

    /**
     * Event source system.
     * Example: "ORCHESTRATOR".
     */
    private String source;

    /**
     * Event schema version.
     * Used for schema evolution and backward compatibility.
     */
    private String version;
}
