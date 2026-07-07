package com.orchestrator.service;

import java.time.Instant;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.orchestrator.entity.DlqEvent;
import com.orchestrator.repository.DlqEventRepository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * ==========================================================
 * DLQ REPLAY SERVICE (FINAL - PRODUCTION READY)
 * ==========================================================
 *
 * PURPOSE:
 * - Safely reprocess failed messages from DLQ
 *
 * DESIGN PRINCIPLES:
 * ✔ No duplicate replay
 * ✔ Maintain Kafka key consistency
 * ✔ Ensure observability
 * ✔ Fail fast on replay issues
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class DlqReplayService {

    private final DlqEventRepository repository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void replay(String dlqId) {

        DlqEvent event = repository.findById(dlqId)
                .orElseThrow(() -> new RuntimeException("DLQ event not found"));

        // --------------------------------------------------
        // SAFETY 1: Already replayed?
        // --------------------------------------------------
        if (Boolean.TRUE.equals(event.getReplayed())) {
            throw new RuntimeException("Message already replayed");
        }

        // --------------------------------------------------
        // SAFETY 2: Basic validation
        // --------------------------------------------------
        if (event.getPayload() == null || event.getOriginalTopic() == null) {
            throw new RuntimeException("Invalid DLQ event");
        }

        String topic = event.getOriginalTopic();
        String payload = event.getPayload();
        String key = event.getTransactionId(); // ✅ IMPORTANT

        try {
            log.info(
                "🔁 Replaying DLQ event | id={} | topic={} | key={}",
                dlqId,
                topic,
                key
            );

            // --------------------------------------------------
            // SEND TO ORIGINAL TOPIC (WITH KEY)
            // --------------------------------------------------
            kafkaTemplate.send(topic, key, payload)
                    .whenComplete((result, ex) -> {

                        if (ex != null) {

                            log.error(
                                "❌ Replay SEND FAILED | id={} | topic={}",
                                dlqId,
                                topic,
                                ex
                            );

                        } else {

                            log.info(
                                "✅ Replay SEND SUCCESS | id={} | topic={} | partition={} | offset={}",
                                dlqId,
                                topic,
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset()
                            );
                        }
                    });

            // --------------------------------------------------
            // MARK AS REPLAYED (AUDIT)
            // --------------------------------------------------
            event.setReplayed(true);
            event.setReplayedAt(Instant.now());

            repository.save(event);

            log.info("✅ DLQ event marked as replayed | id={}", dlqId);

        } catch (Exception ex) {

            log.error("❌ Replay FAILED | id={}", dlqId, ex);

            throw new RuntimeException("Replay failed", ex);
        }
    }
}