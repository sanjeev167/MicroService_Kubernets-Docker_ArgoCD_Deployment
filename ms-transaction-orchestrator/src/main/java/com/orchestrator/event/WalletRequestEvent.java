package com.orchestrator.event;

import java.math.BigDecimal;
import java.time.Instant;

import com.orchestrator.enums.WalletOperationType;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * ==========================================================
 * Event: WalletRequestEvent
 * Service: ms-transaction-orchestrator
 * ==========================================================
 *
 * DESCRIPTION
 * ----------------------------------------------------------
 * Event published by Orchestrator to Wallet Service as part
 * of Saga execution.
 *
 * This event represents a wallet operation request:
 * - DEBIT (normal flow)
 * - CREDIT (compensation flow)
 *
 * ROLE IN SAGA
 * ----------------------------------------------------------
 * - Initiates wallet processing
 * - Carries full transaction context
 * - Enables idempotency and tracing
 *
 * DESIGN PRINCIPLES
 * ----------------------------------------------------------
 * - Strong typing (UUID, BigDecimal)
 * - Full context (no partial payloads)
 * - Immutable-style usage via Builder
 * - Backward-compatible (versioned)
 *
 * CRITICAL REQUIREMENTS
 * ----------------------------------------------------------
 * - sagaId MUST be present (Saga correlation)
 * - transactionId MUST be present (idempotency)
 * - walletId MUST be present
 * - amount MUST be present
 * - walletOperationType MUST be present
 *
 * ==========================================================
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class WalletRequestEvent {

   

    /**
     * Unique Transaction Identifier
     * - Used for idempotency and tracking
     */
    private String transactionId;

    /**
     * Wallet Identifier
     */
    private String walletId;

    /**
     * Amount to debit/credit
     */
    private BigDecimal amount;

    /**
     * Type of wallet operation
     * - DEBIT
     * - CREDIT (compensation)
     */
    private WalletOperationType walletOperationType;

    /**
     * Event creation timestamp
     */
    private Instant createdAt;

    /**
     * Source service name (for tracing/debugging)
     */
    private String source;

    /**
     * Schema version (for evolution)
     */
    private String version;
    
    private String userId;
    
   
}