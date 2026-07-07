package com.orchestrator.specification;

import org.springframework.data.jpa.domain.Specification;

import com.orchestrator.entity.DlqEvent;

public class DlqEventSpecification {

    public static Specification<DlqEvent> hasTransactionId(String transactionId) {
        return (root, query, cb) ->
                transactionId == null ? null :
                        cb.equal(root.get("transactionId"), transactionId);
    }

    public static Specification<DlqEvent> hasEventType(String eventType) {
        return (root, query, cb) ->
                eventType == null ? null :
                        cb.equal(root.get("eventType"), eventType);
    }

    public static Specification<DlqEvent> isReplayed(Boolean replayed) {
        return (root, query, cb) ->
                replayed == null ? null :
                        cb.equal(root.get("replayed"), replayed);
    }
}