package com.wallet.service;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.wallet.entity.ProcessedEvent;
import com.wallet.entity.Wallet;
import com.wallet.entity.WalletTransaction;
import com.wallet.enums.TransactionStatus;
import com.wallet.enums.WalletOperationType;
import com.wallet.event.WalletProcessedEvent;
import com.wallet.event.WalletRequestEvent;
import com.wallet.kafka.producers.WalletResponseProducer;
import com.wallet.repository.ProcessedEventRepository;
import com.wallet.repository.WalletRepository;
import com.wallet.repository.WalletTransactionRepository;

import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;

/**
 * ==========================================================
 * Component: WalletService (Saga Participant)
 * ==========================================================
 *
 * 🎯 PURPOSE
 * ----------------------------------------------------------
 * Handles wallet debit/credit as part of Saga execution.
 *
 * ==========================================================
 *
 * 🧠 CORE RESPONSIBILITIES
 * ----------------------------------------------------------
 * ✔ Perform balance update (DEBIT / CREDIT)
 * ✔ Ensure idempotency (transactionId)
 * ✔ Persist transaction history
 * ✔ Publish response event (ALWAYS)
 *
 * ==========================================================
 *
 * ⚠️ CRITICAL GUARANTEES
 * ----------------------------------------------------------
 * ✔ Exactly-once logical execution (idempotent)
 * ✔ No double debit (DB locking)
 * ✔ Compensation safe
 * ✔ Always responds to orchestrator
 *
 * ==========================================================
 */
@Service
@RequiredArgsConstructor
public class WalletService {

    private static final Logger log = LoggerFactory.getLogger(WalletService.class);

    private final WalletRepository walletRepository;
    private final WalletTransactionRepository txnRepository;
    private final WalletResponseProducer responseProducer;
    private final ProcessedEventRepository processedEventRepository;

    @Transactional
    public void processFromOrchestrator(WalletRequestEvent event) {

        UUID transactionId = UUID.fromString(event.getTransactionId());

        try {
            // --------------------------------------------------
            // STEP 1: Idempotency (NEW CORRECT WAY)
            // --------------------------------------------------
            boolean alreadyProcessed =
                    processedEventRepository.existsByTransactionIdAndEventTypeAndServiceName(
                            transactionId, "WALLET_PROCESS", "WALLET_SERVICE"
                    );

            if (alreadyProcessed) {
                log.warn("Duplicate event | txnId={}", transactionId);
                sendResponse(event, TransactionStatus.COMPLETED, "Duplicate handled");
                return;
            }

            // --------------------------------------------------
            // STEP 2: Fetch Wallet WITH LOCK
            // --------------------------------------------------
            Wallet wallet = walletRepository.findByWalletIdForUpdate(
                   UUID.fromString(event.getWalletId()))
                    .orElseThrow(() -> new RuntimeException("Wallet not found"));

            // --------------------------------------------------
            // STEP 3: Apply Operation
            // --------------------------------------------------
            if (event.getWalletOperationType() == WalletOperationType.DEBIT) {

                if (wallet.getBalance().compareTo(event.getAmount()) < 0) {
                    throw new RuntimeException("Insufficient balance");
                }

                wallet.setBalance(wallet.getBalance().subtract(event.getAmount()));

            } else {
                wallet.setBalance(wallet.getBalance().add(event.getAmount()));
            }

            walletRepository.save(wallet);

            // --------------------------------------------------
            // STEP 4: Save Wallet Transaction (NEW STRUCTURE)
            // --------------------------------------------------
            WalletTransaction txn = WalletTransaction.builder()
                    .walletTransactionId(UUID.randomUUID()) // ✅ PK
                    .transactionId(transactionId)           // ✅ SAGA ID
                    
                    .userId(UUID.fromString(event.getUserId()))
                    .amount(event.getAmount())
                    .transactionType(event.getWalletOperationType())
                    .status(TransactionStatus.COMPLETED)
                    .build();

            txnRepository.save(txn);

            // --------------------------------------------------
            // STEP 5: Mark Idempotency (AFTER SUCCESS)
            // --------------------------------------------------
            processedEventRepository.save(
                    ProcessedEvent.builder()
                            .id(UUID.randomUUID())
                            .transactionId(transactionId)
                            .eventType("WALLET_PROCESS")
                            .serviceName("WALLET_SERVICE")
                            .build()
            );

            // --------------------------------------------------
            // STEP 6: Send SUCCESS
            // --------------------------------------------------
            sendResponse(event, TransactionStatus.COMPLETED, "Wallet processed");

        } catch (Exception ex) {

            log.error("Wallet processing failed | txnId={}", transactionId, ex);

            // Save failure transaction
            txnRepository.save(
                    WalletTransaction.builder()
                            .walletTransactionId(UUID.randomUUID())
                            .transactionId(transactionId)
                            .userId(UUID.fromString(event.getUserId()))
                            .amount(event.getAmount())
                            .transactionType(event.getWalletOperationType())
                            .status(TransactionStatus.FAILED)
                            .build()
            );

            sendResponse(event, TransactionStatus.FAILED, ex.getMessage());
        }
    }

    // ==========================================================
    // COMPENSATION (UPDATED)
    // ==========================================================
    @Transactional
    public void compensateTransaction(WalletRequestEvent event) {

        UUID transactionId = UUID.fromString(event.getTransactionId());

        try {
            // Idempotency for compensation
            boolean alreadyProcessed =
                    processedEventRepository.existsByTransactionIdAndEventTypeAndServiceName(
                            transactionId, "WALLET_COMPENSATE", "WALLET_SERVICE"
                    );

            if (alreadyProcessed) {
                log.warn("Compensation already done | txnId={}", transactionId);
                return;
            }

            Wallet wallet = walletRepository.findByWalletIdForUpdate(
            		UUID.fromString(event.getWalletId()))
                    .orElseThrow(() -> new RuntimeException("Wallet not found"));

            wallet.setBalance(wallet.getBalance().add(event.getAmount()));
            walletRepository.save(wallet);

            txnRepository.save(
                    WalletTransaction.builder()
                            .walletTransactionId(UUID.randomUUID())
                            .transactionId(transactionId)
                            .userId(UUID.fromString(event.getUserId()))
                            .amount(event.getAmount())
                            .transactionType(WalletOperationType.CREDIT)
                            .status(TransactionStatus.COMPENSATED)
                            .build()
            );

            processedEventRepository.save(
                    ProcessedEvent.builder()
                            .id(UUID.randomUUID())
                            .transactionId(transactionId)
                            .eventType("WALLET_COMPENSATE")
                            .serviceName("WALLET_SERVICE")
                            .build()
            );

            sendResponse(event, TransactionStatus.COMPENSATED, "Compensation success");

        } catch (Exception ex) {
            log.error("Compensation failed | txnId={}", transactionId, ex);
        }
    }

    private void sendResponse(WalletRequestEvent event,
                              TransactionStatus status,
                              String message) {

        responseProducer.sendWalletResponse(
                WalletProcessedEvent.builder()
                        .transactionId(event.getTransactionId())
                        .status(status)
                        .message(message)
                        .build()
        );
    }


}