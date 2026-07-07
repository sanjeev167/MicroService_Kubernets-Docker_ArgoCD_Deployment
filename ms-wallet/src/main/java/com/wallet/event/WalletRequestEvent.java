package com.wallet.event;

import java.math.BigDecimal;
import java.time.Instant;

import com.wallet.enums.WalletOperationType;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * ==========================================================
 * EVENT: WalletRequestEvent
 * ==========================================================
 *
 * DESCRIPTION
 * ----------------------------------------------------------
 * Event published by the Transaction Orchestrator to the
 * Wallet Service. This event initiates wallet processing
 * as part of the Saga workflow.
 *
 * ROLE IN SAGA
 * ----------------------------------------------------------
 * ✔ Initiates wallet debit/credit operation
 * ✔ Carries full transaction context (IDs, amount, type)
 * ✔ Uses single global transactionId (Saga ID)
 *
 * DESIGN PRINCIPLES
 * ----------------------------------------------------------
 * ✔ Global transactionId represented as String (not UUID)
 * ✔ Idempotent-safe (duplicate requests can be ignored)
 * ✔ Strong typing for operation type and amount
 * ✔ Extensible metadata for traceability and evolution
 *
 * USAGE
 * ----------------------------------------------------------
 * - Produced by Orchestrator when Saga reaches Wallet step
 * - Consumed by Wallet Service to perform DEBIT or CREDIT
 * - Drives workflow forward by publishing WalletProcessedEvent
 *
 * ==========================================================
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class WalletRequestEvent {

    /**
     * Global Transaction ID (Saga ID).
     * Represented as a String for flexibility across services.
     * Must be unique per Saga workflow.
     */
    private String transactionId;

    /**
     * Wallet Identifier.
     * Represents the target wallet for debit/credit operation.
     */
    private String walletId;

    /**
     * User Identifier.
     * Represents the owner of the wallet.
     */
    private String userId;

    /**
     * Transaction amount.
     * Must be positive and validated by the Wallet Service.
     */
    private BigDecimal amount;

    /**
     * Operation Type.
     * Possible values:
     * - DEBIT
     * - CREDIT (used for compensation)
     */
    private WalletOperationType walletOperationType;

    /**
     * Event creation timestamp.
     * Captures when the Orchestrator generated this event.
     */
    private Instant createdAt;

    /**
     * Source service identifier.
     * Example: "ORCHESTRATOR-SERVICE".
     */
    private String source;

    /**
     * Schema version for event evolution.
     * Example: "v1".
     */
    private String version;
}
