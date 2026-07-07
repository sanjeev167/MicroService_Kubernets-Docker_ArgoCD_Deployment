package com.wallet.event;

import java.time.Instant;

import com.wallet.enums.TransactionStatus;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * ==========================================================
 * EVENT: WalletProcessedEvent
 * ==========================================================
 *
 * DESCRIPTION
 * ----------------------------------------------------------
 * Event published by the Wallet Service after processing a
 * wallet transaction request. This event is sent back to the
 * Transaction Orchestrator as part of the Saga workflow.
 *
 * DESIGN PRINCIPLES
 * ----------------------------------------------------------
 * ✔ Global transactionId (Saga ID) represented as String
 * ✔ Idempotent-safe (duplicate events can be ignored)
 * ✔ Strong typing for status values
 * ✔ Extensible metadata for traceability
 *
 * USAGE
 * ----------------------------------------------------------
 * - Produced by Wallet Service after DEBIT or CREDIT operation
 * - Consumed by Orchestrator to update Saga state
 * - Drives workflow forward to Notification step or triggers
 *   compensation on failure
 *
 * ==========================================================
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class WalletProcessedEvent {

    /**
     * Global Transaction ID (Saga ID).
     * Represented as a String for flexibility across services.
     * Must be unique per Saga workflow.
     */
    private String transactionId;

    /**
     * Processing result of the wallet step.
     * Possible values:
     * - SUCCESS
     * - FAILED
     * - COMPENSATED
     */
    private TransactionStatus status;

    /**
     * Optional descriptive message about the outcome.
     * Example: "Insufficient balance" or "Wallet debit successful".
     */
    private String message;

    /**
     * Event creation timestamp.
     * Captures when the Wallet Service generated this event.
     */
    private Instant createdAt;

    /**
     * Source service identifier.
     * Example: "WALLET-SERVICE".
     */
    private String source;

    /**
     * Schema version for event evolution.
     * Example: "v1".
     */
    private String version;
}
