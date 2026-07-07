package com.orchestrator.event;

import java.time.Instant;

import com.orchestrator.enums.StepStatus;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * ==========================================================
 * Event: WalletCompensationResponseEvent (FINAL)
 * ==========================================================
 *
 * 🎯 PURPOSE
 * ----------------------------------------------------------
 * Represents response from Wallet Service after executing
 * compensation (i.e., reversing a previous wallet operation).
 *
 * ==========================================================
 *
 * 🧠 ROLE IN SAGA
 * ----------------------------------------------------------
 * ✔ Final stage of Saga lifecycle
 * ✔ Drives Saga to:
 *      - COMPENSATED (success)
 *      - FAILED (compensation failure)
 *
 * ==========================================================
 *
 * 📦 PRODUCED BY
 * ----------------------------------------------------------
 * Wallet Service
 *
 * 📦 CONSUMED BY
 * ----------------------------------------------------------
 * Orchestrator (WalletCompensationResponseConsumer)
 *
 * ==========================================================
 *
 * 🔑 IDENTIFIERS (CRITICAL DESIGN)
 * ----------------------------------------------------------
 * ✔ sagaId        → GLOBAL (used to fetch saga)
 * ✔ transactionId → LOCAL compensation transaction ID
 *
 * ⚠️ RULE:
 * Orchestrator MUST ALWAYS use sagaId for DB lookup
 *
 * ==========================================================
 *
 * 📊 STATUS
 * ----------------------------------------------------------
 * COMPLETED → Compensation successful → Saga COMPENSATED
 * FAILED    → Compensation failed → Saga FAILED (critical)
 *
 * ==========================================================
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class WalletCompensationResponseEvent {

    /**
     * ==========================================================
     * GLOBAL SAGA ID (MANDATORY)
     * ==========================================================
     *
     * ✔ Used to fetch saga in orchestrator
     * ✔ Must be same across entire workflow
     */
    private String sagaId;

    /**
     * ==========================================================
     * LOCAL COMPENSATION TRANSACTION ID
     * ==========================================================
     *
     * ✔ Unique to this compensation operation
     * ✔ Used for idempotency + logging
     */
    private String transactionId;

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
     * ✔ Populated only when FAILED
     */
    private String errorMessage;

    /**
     * ==========================================================
     * EVENT METADATA (OPTIONAL BUT RECOMMENDED)
     * ==========================================================
     */
    private Instant createdAt;
    private String source;
    private String version;
}