package com.orchestrator.service;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import org.springframework.stereotype.Service;

import com.orchestrator.entity.SagaTransaction;
import com.orchestrator.enums.WalletOperationType;
import com.orchestrator.event.WalletRequestEvent;
import com.orchestrator.kafka.producers.WalletRequestProducer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
@RequiredArgsConstructor
public class TransactionOrchestratorService {

    private final SagaService sagaService;
    private final WalletRequestProducer producer;

    public String startTransaction(String walletId,
                                   BigDecimal amount,
                                   WalletOperationType operationType,
                                   String userId) {

        // --------------------------------------------------
        // STEP 0: VALIDATION
        // --------------------------------------------------
        if (walletId == null || amount == null || operationType == null || userId == null) {
            throw new IllegalArgumentException("Invalid transaction request");
        }

        try {
            UUID.fromString(walletId);
            UUID.fromString(userId);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid UUID format");
        }

        // --------------------------------------------------
        // STEP 1: GENERATE GLOBAL TRANSACTION ID
        // --------------------------------------------------
        
        String transactionId = UUID.randomUUID().toString();

        log.info("Starting Transaction | txnId={} | walletId={} | amount={} | operationType={}",transactionId,walletId,amount,operationType);

        // --------------------------------------------------
        // STEP 2: CREATE SAGA (USING SAME ID)
        // --------------------------------------------------
        SagaTransaction sagaTransaction = sagaService.createSaga(transactionId,operationType,userId);

        // --------------------------------------------------
        // STEP 3: BUILD EVENT (SINGLE ID MODEL)
        // --------------------------------------------------
        WalletRequestEvent event = WalletRequestEvent.builder()
                .transactionId(transactionId)   // ✅ ONLY ID
                .walletId(walletId)
                .amount(amount)
                .walletOperationType(operationType)
                .createdAt(Instant.now())
                .source("ORCHESTRATOR")
                .version("v1")
                .userId(userId)
                .build();

        // --------------------------------------------------
        // STEP 4: PUBLISH EVENT
        // --------------------------------------------------
        producer.sendWalletRequest(event);

        log.info("WalletRequestEvent published | txnId={}", transactionId);

        // --------------------------------------------------
        // STEP 5: RETURN GLOBAL ID
        // --------------------------------------------------
        return transactionId;
    }
}