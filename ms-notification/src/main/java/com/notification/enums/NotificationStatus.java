package com.notification.enums;

/**
 * ==========================================================
 * Enum: NotificationStatus
 * Module: MSIMPL Phase 3 - Notification Service
 * ==========================================================
 *
 * DESCRIPTION
 * ----------------------------------------------------------
 * Represents the outcome of notification processing
 * within a Saga workflow.
 *
 * This enum is used in:
 * - NotificationProcessedEvent
 * - NotificationTransaction entity
 *
 * ROLE IN SAGA
 * ----------------------------------------------------------
 * Notification is typically the final step in the Saga.
 * Based on this status:
 *
 * - SUCCESS → Saga is completed successfully
 * - FAILED  → Saga is marked as FAILED (no compensation here,
 *             but orchestrator may trigger compensation for
 *             previous steps like Wallet)
 *
 * DESIGN PRINCIPLES
 * ----------------------------------------------------------
 * - Minimal and explicit states
 * - No compensation state (notification is non-reversible)
 * - Aligned with Saga orchestration logic
 *
 * ==========================================================
 */
public enum NotificationStatus {

    /**
     * Notification successfully processed
     */
	COMPLETED,

    /**
     * Notification processing failed
     */
    FAILED
}