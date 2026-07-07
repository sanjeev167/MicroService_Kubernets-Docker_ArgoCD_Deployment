-- ==========================================================
-- DROP EXISTING TABLE (SAFE RESET)
-- ==========================================================
DROP TABLE IF EXISTS saga_transaction;

-- ==========================================================
-- CREATE TABLE: saga_transaction
-- ==========================================================
CREATE TABLE saga_transaction (


-- ------------------------------------------------------
-- PRIMARY KEY
-- ------------------------------------------------------
id BIGSERIAL PRIMARY KEY,

-- ------------------------------------------------------
-- CORE IDENTIFIER
-- ------------------------------------------------------
transaction_id UUID NOT NULL UNIQUE,

-- ------------------------------------------------------
-- OPERATION TYPE
-- ------------------------------------------------------
wallet_operation_type VARCHAR(20) NOT NULL,

-- ------------------------------------------------------
-- SAGA STATE
-- ------------------------------------------------------
saga_status VARCHAR(30) NOT NULL,
current_step VARCHAR(30) NOT NULL,

-- ------------------------------------------------------
-- STEP STATUS
-- ------------------------------------------------------
wallet_status VARCHAR(30) NOT NULL,
notification_status VARCHAR(30) NOT NULL,

-- ------------------------------------------------------
-- COMPENSATION
-- ------------------------------------------------------
compensation_status VARCHAR(30) NOT NULL,

-- ------------------------------------------------------
-- RETRY + ERROR
-- ------------------------------------------------------
retry_count INT NOT NULL DEFAULT 0,
error_message VARCHAR(500),

-- ------------------------------------------------------
-- AUDIT
-- ------------------------------------------------------
user_id VARCHAR(100) NOT NULL,

created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP


);

-- ==========================================================
-- INDEXES
-- ==========================================================

-- Fast lookup by transactionId (most critical)
CREATE INDEX idx_saga_txn_id
ON saga_transaction(transaction_id);

-- Optional but highly useful for reporting/debugging
CREATE INDEX idx_saga_operation_type
ON saga_transaction(wallet_operation_type);

CREATE INDEX idx_saga_status
ON saga_transaction(saga_status);


-- Table: public.dlq_event

-- DROP TABLE IF EXISTS public.dlq_event;

CREATE TABLE IF NOT EXISTS public.dlq_event
(
    id character varying(255) COLLATE pg_catalog."default" NOT NULL,
    original_topic character varying(255) COLLATE pg_catalog."default",
    partition_id integer,
    offset_value bigint,
    payload text COLLATE pg_catalog."default",
    error_message text COLLATE pg_catalog."default",
    created_at timestamp without time zone,
    replayed boolean,
    createdat timestamp(6) with time zone,
    errormessage character varying(255) COLLATE pg_catalog."default",
    offsetvalue bigint,
    originaltopic character varying(255) COLLATE pg_catalog."default",
    partitionid integer,
    event_type character varying(255) COLLATE pg_catalog."default",
    transaction_id character varying(255) COLLATE pg_catalog."default",
    replayed_at timestamp(6) with time zone,
    CONSTRAINT dlq_event_pkey PRIMARY KEY (id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.dlq_event
    OWNER to postgres;
-- Index: idx_dlq_event_type

-- DROP INDEX IF EXISTS public.idx_dlq_event_type;

CREATE INDEX IF NOT EXISTS idx_dlq_event_type
    ON public.dlq_event USING btree
    (event_type COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_dlq_replayed

-- DROP INDEX IF EXISTS public.idx_dlq_replayed;

CREATE INDEX IF NOT EXISTS idx_dlq_replayed
    ON public.dlq_event USING btree
    (replayed ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_dlq_txn_id

-- DROP INDEX IF EXISTS public.idx_dlq_txn_id;

CREATE INDEX IF NOT EXISTS idx_dlq_txn_id
    ON public.dlq_event USING btree
    (transaction_id COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;