package com.wallet.entity;

import java.math.BigDecimal;
import java.util.UUID;

import jakarta.persistence.*;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * ==========================================================
 * Entity: Wallet
 * ==========================================================
 *
 * 🎯 PURPOSE
 * ----------------------------------------------------------
 * Represents a customer wallet storing balance and ownership.
 *
 * ==========================================================
 *
 * 🧱 DB MAPPING (public.wallets)
 * ----------------------------------------------------------
 * wallet_id   → UUID (PK)
 * customer_id → VARCHAR(255)
 * balance     → NUMERIC(19,4) DEFAULT 0
 * version     → BIGINT (Optimistic Locking)
 *
 * ==========================================================
 *
 * ⚠️ DESIGN NOTES
 * ----------------------------------------------------------
 * ✔ walletId is externally generated (UUID)
 * ✔ balance default handled by DB
 * ✔ version managed by Hibernate (@Version)
 * ✔ no business logic inside entity
 *
 * ==========================================================
 */
@Entity
@Table(
    name = "wallets",
    indexes = {
        @Index(name = "idx_wallet_customer_id", columnList = "customer_id")
    }
)
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Wallet {

    /**
     * ==========================================================
     * PRIMARY KEY
     * ==========================================================
     *
     * ✔ UUID-based identifier
     * ✔ Provided externally (no auto-generation)
     */
    @Id
    @Column(name = "wallet_id", nullable = false, updatable = false)
    private UUID walletId;

    /**
     * ==========================================================
     * CUSTOMER IDENTIFIER
     * ==========================================================
     */
    @Column(name = "customer_id", nullable = false, length = 255)
    private String customerId;

    /**
     * ==========================================================
     * WALLET BALANCE
     * ==========================================================
     *
     * ✔ Precision: 19 digits
     * ✔ Scale: 4 decimal places
     * ✔ Default handled at DB level
     */
    @Column(name = "balance", nullable = false, precision = 19, scale = 4)
    private BigDecimal balance;

    /**
     * ==========================================================
     * OPTIMISTIC LOCKING VERSION
     * ==========================================================
     *
     * ✔ Managed by Hibernate
     * ✔ Prevents lost updates
     */
    @Version
    @Column(name = "version", nullable = false)
    private Long version;

    /**
     * ==========================================================
     * LIFECYCLE HOOK (OPTIONAL SAFETY)
     * ==========================================================
     *
     * ✔ Ensures non-null balance at persist time
     * ✔ Works alongside DB default
     */
    @PrePersist
    public void prePersist() {
        if (balance == null) {
            balance = BigDecimal.ZERO;
        }
    }
}