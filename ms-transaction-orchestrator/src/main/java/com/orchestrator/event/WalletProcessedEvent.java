package com.orchestrator.event;


import com.orchestrator.enums.TransactionStatus;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class WalletProcessedEvent {

    private String transactionId;   // REQUIRED (correlation)
    private TransactionStatus status; // SUCCESS / FAILED
        
    private String message;         // Optional (error/success info)
}