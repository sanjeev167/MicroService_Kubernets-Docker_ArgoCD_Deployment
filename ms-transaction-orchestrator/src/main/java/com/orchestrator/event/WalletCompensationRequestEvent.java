package com.orchestrator.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Event representing a request to reverse a wallet operation.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class WalletCompensationRequestEvent {

    private String transactionId;
}