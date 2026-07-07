-- ==========================================================
-- Table: notification_transactions
-- Module: MSIMPL Phase 3 - Notification Service
-- ==========================================================

CREATE TABLE notification (
    notification_transaction_id UUID PRIMARY KEY,
    transaction_id UUID NOT NULL, -- SAME SAGA ID

    user_id UUID NOT NULL,
    message TEXT,

    status VARCHAR(30),

    created_at TIMESTAMP
);

-- ==========================================================
-- Indexes (Performance + Debugging)
-- ==========================================================

-- Fast lookup by saga (tracing/debugging)
CREATE INDEX idx_notification_saga_id
ON notification_transactions (saga_id);

-- Optional composite index (useful in audits)
CREATE INDEX idx_notification_saga_status
ON notification_transactions (saga_id, status);