package com.wallet.repository;

import java.util.List;
import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.wallet.entity.WalletTransaction;
import com.wallet.enums.TransactionStatus;

@Repository
public interface WalletTransactionRepository
        extends JpaRepository<WalletTransaction, UUID> {

    /**
     * ==========================================================
     * FETCH ALL TRANSACTIONS FOR A SAGA (transactionId)
     * ==========================================================
     *
     * ✔ Used for:
     *   - debugging
     *   - reconciliation
     */
    List<WalletTransaction> findByTransactionIdOrderByCreatedAtAsc(UUID transactionId);

    /**
     * ==========================================================
     * CHECK IF ANY TRANSACTION EXISTS FOR SAGA
     * ==========================================================
     */
    boolean existsByTransactionId(UUID transactionId);

    /**
     * ==========================================================
     * FETCH BY TRANSACTION + STATUS
     * ==========================================================
     *
     * ✔ Useful for:
     *   - checking debit success
     *   - verifying compensation
     */
    List<WalletTransaction> findByTransactionIdAndStatus(
            UUID transactionId,
            TransactionStatus status
    );
}