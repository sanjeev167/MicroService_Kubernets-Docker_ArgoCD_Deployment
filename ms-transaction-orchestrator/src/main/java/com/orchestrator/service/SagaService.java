package com.orchestrator.service;

import java.util.UUID;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.orchestrator.entity.SagaTransaction;
import com.orchestrator.enums.CompensationStatus;
import com.orchestrator.enums.SagaStatus;
import com.orchestrator.enums.SagaStep;
import com.orchestrator.enums.StepStatus;
import com.orchestrator.enums.WalletOperationType;
import com.orchestrator.repository.SagaTransactionRepository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * ==========================================================
 * COMPONENT: SagaService (FINAL - PRODUCTION GRADE)
 * ==========================================================
 *
 * Central orchestration service for managing Saga lifecycle
 * across distributed workflow steps (Wallet, Notification,
 * Compensation). Enforces strict transactional boundaries,
 * idempotency, and direction-aware compensation.
 *
 * 🎯 CORE PRINCIPLES
 * - Each public method executes within ONE DB transaction.
 * - Entity is always fetched + updated INSIDE the same method.
 * - No detached entities passed from outside consumers.
 * - All operations identified by {@code sagaId} (UUID string).
 *
 * 🧠 DESIGN MODEL
 * Consumer → Service (pass sagaId only)
 * Service responsibilities:
 * 1. Fetch entity with DB lock
 * 2. Validate state (idempotency)
 * 3. Apply state transition
 * 4. Commit transaction automatically
 *
 * 🔒 RULES
 * - NEVER pass entity from consumer.
 * - ALWAYS fetch inside service with lock.
 * - ALWAYS annotate with {@code @Transactional}.
 * - ALWAYS implement idempotency checks before state change.
 *
 * 🔁 SAGA FLOW
 * INIT → WALLET → NOTIFICATION → COMPLETED
 *                   ↓
 *              COMPENSATION
 *
 * 🔁 COMPENSATION STRATEGY
 * - DEBIT  → compensate with CREDIT
 * - CREDIT → compensate with DEBIT
 *
 * ==========================================================
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class SagaService {

    private final SagaTransactionRepository repository;

    /**
     * CREATE SAGA (Idempotent).
     * Initializes a new SagaTransaction if not already present.
     * Sets initial step based on {@link WalletOperationType}.
     *
     * @param sagaId        unique transaction identifier (UUID string)
     * @param operationType wallet operation type (DEBIT/CREDIT)
     * @param userId        user initiating the transaction
     * @return persisted SagaTransaction (new or existing)
     * @throws IllegalArgumentException if input is invalid
     */
    @Transactional
    public SagaTransaction createSaga(String sagaId,
                                      WalletOperationType operationType,
                                      String userId) {

        if (sagaId == null || operationType == null || userId == null) {
            throw new IllegalArgumentException("Invalid saga creation request");
        }

        UUID transactionId = UUID.fromString(sagaId);

        return repository.findByTransactionId(transactionId)
                .orElseGet(() -> {
                    SagaTransaction saga = new SagaTransaction();

                    // CORE DATA
                    saga.setTransactionId(transactionId);
                    saga.setUserId(userId);
                    saga.setWalletOperationType(operationType);

                    // INITIAL STATE
                    saga.setSagaStatus(SagaStatus.IN_PROGRESS);

                    // INITIAL STEP
                    switch (operationType) {
                        case DEBIT:
                            saga.setCurrentStep(SagaStep.WALLET_DEBIT);
                            break;
                        case CREDIT:
                            saga.setCurrentStep(SagaStep.WALLET_CREDIT);
                            break;
                        default:
                            throw new IllegalArgumentException("Unsupported operation type");
                    }

                    // STEP STATUS
                    saga.setWalletStatus(StepStatus.PENDING);
                    saga.setNotificationStatus(StepStatus.PENDING);

                    // COMPENSATION
                    saga.setCompensationStatus(CompensationStatus.NOT_REQUIRED);
                    saga.setRetryCount(0);

                    log.info("Saga CREATED | txnId={} | operationType={}", sagaId, operationType);

                    return repository.save(saga);
                });
    }

    /**
     * INTERNAL: Fetch saga with DB lock.
     * Ensures safe concurrent updates by locking row.
     *
     * @param sagaId unique transaction identifier
     * @return locked SagaTransaction
     * @throws RuntimeException if saga not found
     */
    private SagaTransaction getSagaOrThrow(String sagaId) {
        return repository.findByTransactionIdForUpdate(UUID.fromString(sagaId))
                .orElseThrow(() -> new RuntimeException("Saga not found: " + sagaId));
    }

    /**
     * WALLET SUCCESS → Transition to Notification step.
     * Idempotent: skips if wallet already marked COMPLETED.
     *
     * @param sagaId unique transaction identifier
     * @return updated SagaTransaction
     */
    @Transactional
    public SagaTransaction handleWalletSuccess(String sagaId) {
        SagaTransaction saga = getSagaOrThrow(sagaId);

        if (saga.getWalletStatus() == StepStatus.COMPLETED) {
            return saga;
        }

        saga.setWalletStatus(StepStatus.COMPLETED);
        saga.setCurrentStep(SagaStep.NOTIFICATION);
        saga.setSagaStatus(SagaStatus.IN_PROGRESS);

        log.info("Wallet SUCCESS | txnId={}", sagaId);

        return saga;
    }

    /**
     * WALLET FAILURE → Terminal failure.
     * Marks saga FAILED and records error message.
     *
     * @param sagaId unique transaction identifier
     * @param error  failure reason
     */
    @Transactional
    public void handleWalletFailure(String sagaId, String error) {
        SagaTransaction saga = getSagaOrThrow(sagaId);

        if (saga.getWalletStatus() == StepStatus.FAILED) return;

        saga.setWalletStatus(StepStatus.FAILED);
        saga.setSagaStatus(SagaStatus.FAILED);
        saga.setErrorMessage(error);

        log.error("Wallet FAILED | txnId={} | error={}", sagaId, error);
    }

    /**
     * NOTIFICATION SUCCESS → Saga completed.
     * Idempotent: skips if notification already marked COMPLETED.
     *
     * @param sagaId unique transaction identifier
     */
    @Transactional
    public void handleNotificationSuccess(String sagaId) {
        SagaTransaction saga = getSagaOrThrow(sagaId);

        if (saga.getNotificationStatus() == StepStatus.COMPLETED) return;

        saga.setNotificationStatus(StepStatus.COMPLETED);
        saga.setSagaStatus(SagaStatus.COMPLETED);
        saga.setCurrentStep(SagaStep.COMPLETED);

        log.info("Saga COMPLETED | txnId={}", sagaId);
    }

    /**
     * NOTIFICATION FAILURE → Start compensation.
     * Direction-aware compensation:
     * - DEBIT → WALLET_CREDIT
     * - CREDIT → WALLET_DEBIT
     *
     * @param sagaId unique transaction identifier
     * @param error  failure reason
     */
    @Transactional
    public void handleNotificationFailure(String sagaId, String error) {
        SagaTransaction saga = getSagaOrThrow(sagaId);

        if (saga.getNotificationStatus() == StepStatus.FAILED) return;

        saga.setNotificationStatus(StepStatus.FAILED);
        saga.setSagaStatus(SagaStatus.COMPENSATING);
        saga.setCompensationStatus(CompensationStatus.IN_PROGRESS);

        switch (saga.getWalletOperationType()) {
            case DEBIT:
                saga.setCurrentStep(SagaStep.WALLET_CREDIT);
                break;
            case CREDIT:
                saga.setCurrentStep(SagaStep.WALLET_DEBIT);
                break;
        }

        saga.setErrorMessage(error);

        log.error("Notification FAILED → Compensation STARTED | txnId={}", sagaId);
    }

    /**
     * COMPENSATION SUCCESS → Final compensated state.
     *
     * @param sagaId unique transaction identifier
     */
    @Transactional
    public void handleCompensationSuccess(String sagaId) {
        SagaTransaction saga = getSagaOrThrow(sagaId);

        if (saga.getCompensationStatus() == CompensationStatus.COMPLETED) return;

        saga.setCompensationStatus(CompensationStatus.COMPLETED);
        saga.setSagaStatus(SagaStatus.COMPENSATED);

        log.info("Compensation SUCCESS | txnId={}", sagaId);
    }

    /**
     * COMPENSATION FAILURE → Critical failure.
     * Marks saga FAILED and records error message.
     *
     * @param sagaId unique transaction identifier
     * @param error  failure reason
     */
    @Transactional
    public void handleCompensationFailure(String sagaId, String error) {
        SagaTransaction saga = getSagaOrThrow(sagaId);

        if (saga.getCompensationStatus() == CompensationStatus.FAILED) return;

        saga.setCompensationStatus(CompensationStatus.FAILED);
        saga.setSagaStatus(SagaStatus.FAILED);
        saga.setErrorMessage(error);

        log.error("Compensation FAILED | txnId={} | error={}", sagaId, error);
    }
}
