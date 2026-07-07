package com.notification.service;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.notification.entity.NotificationTransaction;
import com.notification.entity.ProcessedEvent;
import com.notification.enums.NotificationStatus;
import com.notification.event.NotificationRequestEvent;
import com.notification.kafka.producer.NotificationResponseProducer;
import com.notification.repository.NotificationTransactionRepository;
import com.notification.repository.ProcessedEventRepository;

/**
 * ==========================================================
 * NotificationService (Idempotent & Production-Ready)
 * ==========================================================
 */
@Service
public class NotificationService {

    private static final Logger log = LoggerFactory.getLogger(NotificationService.class);

    private final NotificationTransactionRepository transactionRepository;
    private final NotificationResponseProducer responseProducer;
    private final ProcessedEventRepository processedEventRepository;

    public NotificationService(NotificationTransactionRepository transactionRepository,
                               NotificationResponseProducer responseProducer,
                               ProcessedEventRepository processedEventRepository) {
        this.transactionRepository = transactionRepository;
        this.responseProducer = responseProducer;
        this.processedEventRepository = processedEventRepository;
    }

    @Transactional
    public void processNotification(NotificationRequestEvent event) {

        // --------------------------------------------------
        // STEP 0: Validation
        // --------------------------------------------------
        if (event == null || event.getTransactionId() == null) {
            log.error("Invalid notification event");
            return;
        }

        UUID transactionId = UUID.fromString(event.getTransactionId());

        try {
            // --------------------------------------------------
            // STEP 1: Idempotency (CORRECT WAY)
            // --------------------------------------------------
            boolean alreadyProcessed =
                    processedEventRepository.existsByTransactionIdAndEventTypeAndServiceName(
                            transactionId,
                            "NOTIFICATION_PROCESS",
                            "NOTIFICATION_SERVICE"
                    );

            if (alreadyProcessed) {
                log.warn("Duplicate notification ignored | txnId={}", transactionId);

                sendResponse(transactionId, NotificationStatus.COMPLETED,
                        "Duplicate handled");

                return;
            }

            // --------------------------------------------------
            // STEP 2: Process Notification
            // --------------------------------------------------
            log.info("Processing notification | txnId={} | message={}",
                    transactionId, event.getMessage());

            // TODO: Email/SMS logic

            // --------------------------------------------------
            // STEP 3: Save Notification Record
            // --------------------------------------------------
            NotificationTransaction txn = NotificationTransaction.builder()
                    .notificationTransactionId(UUID.randomUUID()) // PK
                    .transactionId(transactionId)                 // Saga ID
                    .userId(UUID.fromString(event.getUserId()))
                    .message(event.getMessage())
                    .status(NotificationStatus.COMPLETED)
                    .build();

            transactionRepository.save(txn);

            // --------------------------------------------------
            // STEP 4: Mark Idempotent AFTER SUCCESS
            // --------------------------------------------------
            processedEventRepository.save(
                    ProcessedEvent.builder()
                            .id(UUID.randomUUID())
                            .transactionId(transactionId)
                            .eventType("NOTIFICATION_PROCESS")
                            .serviceName("NOTIFICATION_SERVICE")
                            .build()
            );

            // --------------------------------------------------
            // STEP 5: Send SUCCESS
            // --------------------------------------------------
            sendResponse(transactionId,
                    NotificationStatus.COMPLETED,
                    "Notification processed");

        } catch (Exception ex) {

            log.error("Notification failed | txnId={}", transactionId, ex);

            try {
                transactionRepository.save(
                        NotificationTransaction.builder()
                                .notificationTransactionId(UUID.randomUUID())
                                .transactionId(transactionId)
                                .userId(UUID.fromString(event.getUserId()))
                                .message(event.getMessage())
                                .status(NotificationStatus.FAILED)
                                .build()
                );
            } catch (Exception ignored) {
                log.warn("Failed to persist failure | txnId={}", transactionId);
            }

            sendResponse(transactionId,
                    NotificationStatus.FAILED,
                    "Notification failed");
        }
    }

    private void sendResponse(UUID transactionId,
                              NotificationStatus status,
                              String message) {

        responseProducer.sendNotificationResponse(
                transactionId,
                status,
                message
        );
    }
}