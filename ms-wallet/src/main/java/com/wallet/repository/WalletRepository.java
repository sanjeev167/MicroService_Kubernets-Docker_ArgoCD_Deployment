package com.wallet.repository;

import java.util.Optional;
import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.wallet.entity.Wallet;

import jakarta.persistence.LockModeType;

/**
 * ==========================================================
 * Repository: WalletRepository
 * ==========================================================
 *
 * 🎯 PURPOSE
 * ----------------------------------------------------------
 * Provides database access for Wallet entity.
 *
 * ✔ Standard CRUD operations
 * ✔ Customer-based lookup
 * ✔ Concurrency-safe balance updates
 *
 * ==========================================================
 *
 * 🧠 CONCURRENCY STRATEGY
 * ----------------------------------------------------------
 *
 * 1. DEFAULT → OPTIMISTIC LOCKING (@Version)
 *    ✔ Lightweight
 *    ✔ High performance
 *    ✔ Retry required on conflict
 *
 * 2. CRITICAL SECTION → PESSIMISTIC LOCKING
 *    ✔ Strict consistency
 *    ✔ Prevents concurrent updates
 *    ✔ Used in high-risk balance updates
 *
 * ==========================================================
 *
 * ⚠️ USAGE GUIDELINES
 * ----------------------------------------------------------
 * ✔ Use findById() → for read-only operations
 * ✔ Use findByWalletIdForUpdate() → for debit/credit
 *
 * ❌ NEVER update balance without locking
 *
 * ==========================================================
 */
@Repository
public interface WalletRepository extends JpaRepository<Wallet, UUID> {

    /**
     * ==========================================================
     * FIND BY CUSTOMER ID
     * ==========================================================
     *
     * ✔ Used for customer-level wallet lookup
     */
    Optional<Wallet> findByCustomerId(String customerId);

    /**
     * ==========================================================
     * CHECK WALLET EXISTENCE
     * ==========================================================
     */
    boolean existsByCustomerId(String customerId);

    /**
     * ==========================================================
     * FETCH WALLET WITH PESSIMISTIC LOCK
     * ==========================================================
     *
     * ✔ Locks row for update (SELECT FOR UPDATE)
     * ✔ Prevents concurrent balance modification
     *
     * 🔥 USE CASES:
     * - Debit operation
     * - Credit operation
     * - Compensation (rollback)
     *
     * ⚠️ WARNING:
     * - Can reduce throughput under high contention
     * - Use only when required
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT w FROM Wallet w WHERE w.walletId = :walletId")
    Optional<Wallet> findByWalletIdForUpdate(@Param("walletId") UUID walletId);

    /**
     * ==========================================================
     * EXPLICIT FETCH BY WALLET ID (OPTIONAL)
     * ==========================================================
     *
     * ✔ Same as findById but improves readability in domain logic
     */
    Optional<Wallet> findByWalletId(UUID walletId);
}