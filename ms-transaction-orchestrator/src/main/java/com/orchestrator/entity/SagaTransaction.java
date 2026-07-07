package com.orchestrator.entity;

import java.time.LocalDateTime;
import java.util.UUID;

import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import com.orchestrator.enums.CompensationStatus;
import com.orchestrator.enums.SagaStatus;
import com.orchestrator.enums.SagaStep;
import com.orchestrator.enums.StepStatus;
import com.orchestrator.enums.WalletOperationType;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * ==========================================================
 * Entity: SagaTransaction (Phase 2 Ready)
 *
 * <p><b>Description:</b></p>
 * Represents the orchestration state of a distributed Saga.
 *
 * Tracks:
 * ✔ Overall saga lifecycle
 * ✔ Step-level execution (Wallet, Notification)
 * ✔ Compensation state (for rollback)
 * ✔ Retry + error audit
 *
 * <p><b>Design Goals:</b></p>
 * <ul>
 *   <li>✔ Strong consistency</li>
 *   <li>✔ Idempotent-safe updates</li>
 *   <li>✔ Compensation-ready</li>
 *   <li>✔ Audit-friendly</li>
 * </ul>
 * ==========================================================
 */
@Entity
@Table(
    name = "saga_transaction",
    indexes = {
        @Index(name = "idx_saga_txn_id", columnList = "transaction_id")
    }
)
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SagaTransaction {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "transaction_id", nullable = false, unique = true)
    private UUID transactionId; // ✅ STRONGLY RECOMMENDED
    
    
    
    @Enumerated(EnumType.STRING)
    @Column(name = "wallet_operation_type", nullable = false)
    private WalletOperationType walletOperationType;

    @Enumerated(EnumType.STRING)
    @Column(name = "saga_status", nullable = false)
    private SagaStatus sagaStatus;

    @Enumerated(EnumType.STRING)
    @Column(name = "current_step", nullable = false)
    private SagaStep currentStep;

    @Enumerated(EnumType.STRING)
    @Column(name = "wallet_status", nullable = false)
    private StepStatus walletStatus;

    @Enumerated(EnumType.STRING)
    @Column(name = "notification_status", nullable = false)
    private StepStatus notificationStatus;

    @Enumerated(EnumType.STRING)
    @Column(name = "compensation_status")
    private CompensationStatus compensationStatus;

    @Column(name = "retry_count")
    private int retryCount;

    @Column(name = "error_message", length = 500)
    private String errorMessage;

    @CreationTimestamp
    @Column(name = "created_at", updatable = false)
    private LocalDateTime createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at")
    private LocalDateTime updatedAt;
    
    @Column(name = "user_id")
    private String userId;
}