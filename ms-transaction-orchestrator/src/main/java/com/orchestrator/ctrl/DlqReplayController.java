package com.orchestrator.ctrl;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import com.orchestrator.service.DlqReplayService;
import com.orchestrator.util.TraceUtil;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * ==========================================================
 * DLQ REPLAY CONTROLLER (OBSERVABILITY + TRACEABILITY)
 * ==========================================================
 *
 * AUTHOR: Sanjeev Kumar  
 * DATE:   June 16, 2026
 *
 * PURPOSE:
 * - Trigger replay of failed Kafka events (DLQ messages)
 * - Provide traceable and debuggable replay operations
 *
 * ==========================================================
 * TRACEABILITY DESIGN
 * ==========================================================
 *
 * ✔ traceId generated at entry point (controller)  
 * ✔ Stored in MDC for log correlation  
 * ✔ Ensures replay operations are traceable across services  
 *
 * ==========================================================
 * RESPONSIBILITIES
 * ==========================================================
 *
 * ✔ Accept replay request  
 * ✔ Manage trace lifecycle  
 * ✔ Log replay actions  
 * ✔ Delegate to replay service  
 *
 * ==========================================================
 * NON-RESPONSIBILITIES
 * ==========================================================
 *
 * ❌ No business logic  
 * ❌ No Kafka logic  
 * ❌ No metrics emission  
 *
 * ==========================================================
 */
@RestController
@RequestMapping("/api/dlq")
@RequiredArgsConstructor
@Slf4j
public class DlqReplayController {

    private final DlqReplayService replayService;

    /**
     * REPLAY SINGLE DLQ MESSAGE
     * --------------------------------------------------
     * Accepts a DLQ message ID and triggers replay.
     *
     * @param id DLQ message identifier
     * @return ResponseEntity with replay status
     */
    @PostMapping("/replay/{id}")
    public ResponseEntity<String> replay(@PathVariable String id) {

        // --------------------------------------------------
        // STEP 0: TRACE INITIALIZATION
        // --------------------------------------------------
        String traceId = TraceUtil.generateTraceId();
        TraceUtil.setTraceId(traceId);

        try {
            // --------------------------------------------------
            // STEP 1: LOG REQUEST
            // --------------------------------------------------
            log.info("DLQ replay requested | id={}", id);

            // --------------------------------------------------
            // STEP 2: DELEGATE TO SERVICE
            // --------------------------------------------------
            replayService.replay(id);

            log.info("DLQ replay SUCCESS | id={}", id);

            return ResponseEntity.ok("Replay triggered for id: " + id);

        } catch (Exception ex) {
            log.error("DLQ replay FAILED | id={}", id, ex);
            return ResponseEntity.badRequest()
                    .body("Replay failed: " + ex.getMessage());

        } finally {
            // --------------------------------------------------
            // STEP 3: CLEAR TRACE CONTEXT
            // --------------------------------------------------
            TraceUtil.clear();
        }
    }
}
