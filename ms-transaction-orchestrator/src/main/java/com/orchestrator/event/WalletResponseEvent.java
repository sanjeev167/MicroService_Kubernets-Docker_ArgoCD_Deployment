package com.orchestrator.event;

import java.time.Instant;

import com.orchestrator.enums.StepStatus;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * ==========================================================
 * Event: WalletResponseEvent (Production-Ready)
 * ==========================================================
 *
 * 🎯 PURPOSE
 * ----------------------------------------------------------
 * Represents the response from Wallet Service after processing
 * a wallet operation (DEBIT / CREDIT).
 *
 * ==========================================================
 *
 * 🧠 RESPONSIBILITIES
 * ----------------------------------------------------------
 * ✔ Update Saga state (wallet step)
 * ✔ Drive next step (Notification)
 * ✔ Handle failure scenarios
 *
 * ==========================================================
 *
 * 📦 PRODUCED BY
 * ----------------------------------------------------------
 * Wallet Service
 *
 * 📦 CONSUMED BY
 * ----------------------------------------------------------
 * Orchestrator (WalletResponseConsumer)
 *
 * ==========================================================
 *
 * 🧠 DESIGN PRINCIPLES
 * ----------------------------------------------------------
 * ✔ Step-level status (NOT business transaction status)
 * ✔ Fully self-contained event
 * ✔ Strong correlation using transactionId + sagaId
 * ✔ Versioned for future compatibility
 *
 * ==========================================================
 *
 * 📊 STATUS MEANING
 * ----------------------------------------------------------
 * COMPLETED → Wallet operation successful
 * FAILED    → Wallet operation failed
 *
 * ==========================================================
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class WalletResponseEvent {

    /**
     * ==========================================================
     * UNIQUE TRANSACTION ID
     * ==========================================================
     *
     * ✔ Used as Kafka key
     * ✔ Identifies specific operation
     */
    private String transactionId;

    /**
     * ==========================================================
     * SAGA CORRELATION ID
     * ==========================================================
     *
     * ✔ Links all steps in the Saga
     */
   // private String sagaId;

    /**
     * ==========================================================
     * STEP STATUS
     * ==========================================================
     *
     * ✔ COMPLETED / FAILED
     */
    private StepStatus status;

    /**
     * ==========================================================
     * ERROR MESSAGE
     * ==========================================================
     *
     * ✔ Populated only when status = FAILED
     */
    private String errorMessage;

    /**
     * ==========================================================
     * EVENT CREATION TIME
     * ==========================================================
     *
     * ✔ Useful for tracing, debugging, retries
     */
    private Instant createdAt;

    /**
     * ==========================================================
     * EVENT SOURCE
     * ==========================================================
     *
     * Example: "WALLET_SERVICE"
     */
    private String source;

    /**
     * ==========================================================
     * EVENT VERSION
     * ==========================================================
     *
     * ✔ Enables backward compatibility
     * Example: "v1"
     */
    private String version;
    
    private String userId;
}