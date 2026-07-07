package com.orchestrator.enums;

public enum TransactionStatus {

    CREATED,        // initial state
    PROCESSING,     // saga executing

    COMPLETED,      // fully successful

    COMPENSATING,   // rollback in progress
    FAILED          // final failure after compensation
    
    
}