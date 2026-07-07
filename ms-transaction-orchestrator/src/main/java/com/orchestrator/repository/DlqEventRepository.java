package com.orchestrator.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;

import com.orchestrator.entity.DlqEvent;

/**
 * ==========================================================
 * DLQ EVENT REPOSITORY
 * ==========================================================
 *
 * PURPOSE:
 * - CRUD operations on DLQ events
 * - Dynamic filtering via Specifications
 * - Support replay and monitoring use-cases
 */
public interface DlqEventRepository
        extends JpaRepository<DlqEvent, String>,
                JpaSpecificationExecutor<DlqEvent> {

    /**
     * Count unreplayed events (useful for dashboards)
     */
    long countByReplayedFalse();

    /**
     * Fetch unreplayed events (future bulk replay)
     */
    List<DlqEvent> findByReplayedFalse();

    /**
     * Count by event type (monitoring)
     */
    long countByEventType(String eventType);
}