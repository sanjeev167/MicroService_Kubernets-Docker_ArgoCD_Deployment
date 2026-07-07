-- ==========================================================
-- Table: wallets
-- Module: MSIMPL Phase 3 - Wallet Service
-- ==========================================================

CREATE TABLE wallets (

    wallet_id UUID PRIMARY KEY,

    customer_id VARCHAR(255) NOT NULL,

    balance NUMERIC(19, 4) NOT NULL DEFAULT 0,

    version BIGINT NOT NULL DEFAULT 0
);

-- ==========================================================
-- Indexes
-- ==========================================================

-- Fast lookup by customer
CREATE INDEX idx_wallet_customer_id
ON wallets (customer_id);

-- Optional: ensure one wallet per customer
-- Uncomment if business rule requires uniqueness
-- CREATE UNIQUE INDEX uq_wallet_customer
-- ON wallets (customer_id);



INSERT INTO public.wallets (
    wallet_id,
    customer_id,
    balance,
    version
) VALUES (
    '123e4567-e89b-12d3-a456-426614174000',  -- UUID
    'U1',                                    -- customer_id
    10000.0000,                              -- balance
    0                                        -- version
);

---Second table--

-- ==========================================================
-- Table: wallet_transactions
-- Module: MSIMPL Phase 3 - Wallet Service
-- ==========================================================

CREATE TABLE wallet_transaction (
    wallet_transaction_id UUID PRIMARY KEY,
    transaction_id UUID NOT NULL, -- SAGA ID (common across services)

    user_id UUID NOT NULL,
    amount DECIMAL(12,2) NOT NULL,
    transaction_type VARCHAR(20), -- DEBIT / CREDIT

    status VARCHAR(30), -- PENDING / SUCCESS / FAILED

    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

-- ==========================================================
-- Indexes (CRITICAL for performance)
-- ==========================================================

-- Idempotency + Saga tracking
CREATE INDEX idx_wallet_txn_saga_id
ON wallet_transactions (saga_id);

-- Wallet history lookup
CREATE INDEX idx_wallet_txn_wallet_id
ON wallet_transactions (wallet_id);

-- Combined index (very useful for compensation + queries)
CREATE INDEX idx_wallet_txn_saga_status
ON wallet_transactions (saga_id, status);

-- Optional: faster transaction lookup
CREATE INDEX idx_wallet_txn_transaction_id
ON wallet_transactions (transaction_id);