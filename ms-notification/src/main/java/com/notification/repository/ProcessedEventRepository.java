package com.notification.repository;

import java.util.Optional;
import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.notification.entity.ProcessedEvent;


@Repository
public interface ProcessedEventRepository extends JpaRepository<ProcessedEvent, UUID> {

    /**
     * ==========================================================
     * IDEMPOTENCY CHECK (PRIMARY METHOD)
     * ==========================================================
     */
    boolean existsByTransactionIdAndEventTypeAndServiceName(
            UUID transactionId,
            String eventType,
            String serviceName
    );

    /**
     * ==========================================================
     * OPTIONAL: FETCH FOR DEBUGGING / AUDIT
     * ==========================================================
     */
    Optional<ProcessedEvent> findByTransactionIdAndEventTypeAndServiceName(
            UUID transactionId,
            String eventType,
            String serviceName
    );
}