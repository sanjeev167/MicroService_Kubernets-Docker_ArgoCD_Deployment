package com.wallet.entity;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

import com.wallet.enums.TransactionStatus;
import com.wallet.enums.WalletOperationType;

import jakarta.persistence.*;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Table(
    name = "wallet_transaction",
    indexes = {
        @Index(name = "idx_wallet_txn_transaction_id", columnList = "transaction_id")
    }
)
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class WalletTransaction {

    @Id
    @Column(name = "wallet_transaction_id", nullable = false, updatable = false)
    private UUID walletTransactionId;

    // ✅ SAGA ID (global)
    @Column(name = "transaction_id", nullable = false)
    private UUID transactionId;

    @Column(name = "user_id", nullable = false)
    private UUID userId;

    @Column(name = "amount", nullable = false, precision = 12, scale = 2)
    private BigDecimal amount;

    @Enumerated(EnumType.STRING)
    @Column(name = "transaction_type", length = 20)
    private WalletOperationType transactionType;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", length = 30)
    private TransactionStatus status;

    @Column(name = "created_at")
    private LocalDateTime createdAt;

    @Column(name = "updated_at")
    private LocalDateTime updatedAt;
}