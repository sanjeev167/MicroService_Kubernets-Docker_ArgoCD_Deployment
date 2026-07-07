package com.wallet.repository;

import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.wallet.entity.ProcessedEvent;



@Repository
public interface ProcessedEventRepository extends JpaRepository<ProcessedEvent, UUID> {

    /**
     * ==========================================================
     * IDEMPOTENCY CHECK
     * ==========================================================
     *
     * Checks if a specific event has already been processed
     */
    boolean existsByTransactionIdAndEventTypeAndServiceName(
            UUID transactionId,
            String eventType,
            String serviceName
    );
}