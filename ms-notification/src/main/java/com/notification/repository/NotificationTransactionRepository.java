package com.notification.repository;

import java.util.List;
import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.notification.entity.NotificationTransaction;
import com.notification.enums.NotificationStatus;

@Repository
public interface NotificationTransactionRepository
        extends JpaRepository<NotificationTransaction, UUID> {

    /**
     * ==========================================================
     * FETCH ALL NOTIFICATIONS FOR A TRANSACTION (SAGA)
     * ==========================================================
     *
     * ✔ Used for:
     *   - debugging
     *   - audit trail
     */
    List<NotificationTransaction> findByTransactionIdOrderByCreatedAtAsc(UUID transactionId);

    /**
     * ==========================================================
     * CHECK IF ANY NOTIFICATION EXISTS FOR TRANSACTION
     * ==========================================================
     */
    boolean existsByTransactionId(UUID transactionId);

    /**
     * ==========================================================
     * FETCH BY TRANSACTION + STATUS
     * ==========================================================
     */
    List<NotificationTransaction> findByTransactionIdAndStatus(
            UUID transactionId,
            NotificationStatus status
    );
}