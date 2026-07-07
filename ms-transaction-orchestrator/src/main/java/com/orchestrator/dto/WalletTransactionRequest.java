package com.orchestrator.dto;

import java.math.BigDecimal;

import com.orchestrator.enums.WalletOperationType;

import lombok.Data;

/**
 * API Request DTO (Client → Orchestrator)
 */
@Data
public class WalletTransactionRequest {

    private String walletId;
    private BigDecimal amount;
    private WalletOperationType walletOperationType;
    private String userId;   
  
}