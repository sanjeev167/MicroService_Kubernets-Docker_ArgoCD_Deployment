package com.orchestrator.repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.springframework.data.jpa.repository.*;
import org.springframework.data.repository.query.Param;

import com.orchestrator.entity.SagaTransaction;
import com.orchestrator.enums.SagaStatus;
import com.orchestrator.enums.SagaStep;

import jakarta.persistence.LockModeType;

public interface SagaTransactionRepository extends JpaRepository<SagaTransaction, Long> {

    Optional<SagaTransaction> findByTransactionId(UUID transactionId);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT s FROM SagaTransaction s WHERE s.transactionId = :transactionId")
    Optional<SagaTransaction> findByTransactionIdForUpdate(@Param("transactionId") UUID transactionId);

    List<SagaTransaction> findBySagaStatus(SagaStatus sagaStatus);

    List<SagaTransaction> findBySagaStatusAndRetryCountLessThan(
        SagaStatus sagaStatus,
        int retryCount
    );

    List<SagaTransaction> findByCurrentStep(SagaStep step);
}