package com.orchestrator.ctrl;

import org.springframework.data.domain.Page;
import org.springframework.web.bind.annotation.*;

import com.orchestrator.entity.DlqEvent;
import com.orchestrator.service.DlqQueryService;
import com.orchestrator.util.TraceUtil;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * ==========================================================
 * DLQ QUERY CONTROLLER (OBSERVABILITY + TRACEABILITY)
 * ==========================================================
 *
 * AUTHOR: Sanjeev Kumar  
 * DATE:   June 16, 2026
 *
 * PURPOSE:
 * - Search and retrieve DLQ events
 * - Provide traceable query operations for debugging and analysis
 *
 * ==========================================================
 * TRACEABILITY DESIGN
 * ==========================================================
 *
 * ✔ traceId generated at entry point (controller)  
 * ✔ Stored in MDC for log correlation  
 * ✔ Enables tracing of query operations across logs  
 *
 * ==========================================================
 * RESPONSIBILITIES
 * ==========================================================
 *
 * ✔ Accept search filters  
 * ✔ Manage trace lifecycle  
 * ✔ Log query requests  
 * ✔ Delegate to query service  
 *
 * ==========================================================
 * NON-RESPONSIBILITIES
 * ==========================================================
 *
 * ❌ No business logic  
 * ❌ No metrics emission  
 * ❌ No persistence logic  
 *
 * ==========================================================
 */
@RestController
@RequestMapping("/api/dlq")
@RequiredArgsConstructor
@Slf4j
public class DlqQueryController {

    private final DlqQueryService queryService;

    /**
     * SEARCH DLQ EVENTS
     * --------------------------------------------------
     * Filters:
     * - transactionId (optional)
     * - eventType (optional)
     * - replayed (optional)
     *
     * Pagination:
     * - page (default 0)
     * - size (default 10)
     *
     * @param transactionId optional transaction identifier
     * @param eventType optional event type filter
     * @param replayed optional replayed flag
     * @param page page number (default 0)
     * @param size page size (default 10)
     * @return paginated list of DLQ events
     */
    @GetMapping("/search")
    public Page<DlqEvent> search(@RequestParam(required = false) String transactionId,
                                 @RequestParam(required = false) String eventType,
                                 @RequestParam(required = false) Boolean replayed,
                                 @RequestParam(defaultValue = "0") int page,
                                 @RequestParam(defaultValue = "10") int size) {

        // --------------------------------------------------
        // STEP 0: TRACE INITIALIZATION
        // --------------------------------------------------
        String traceId = TraceUtil.generateTraceId();
        TraceUtil.setTraceId(traceId);

        try {
            // --------------------------------------------------
            // STEP 1: LOG REQUEST
            // --------------------------------------------------
            log.info("DLQ search request | transactionId={} | eventType={} | replayed={} | page={} | size={}",
                    transactionId, eventType, replayed, page, size);

            // --------------------------------------------------
            // STEP 2: DELEGATE TO SERVICE
            // --------------------------------------------------
            Page<DlqEvent> result = queryService.search(transactionId, eventType, replayed, page, size);

            log.info("DLQ search SUCCESS | totalElements={} | page={} | size={}",
                    result.getTotalElements(), page, size);

            return result;

        } finally {
            // --------------------------------------------------
            // STEP 3: CLEAR TRACE CONTEXT
            // --------------------------------------------------
            TraceUtil.clear();
        }
    }
}
