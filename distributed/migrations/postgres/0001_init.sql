-- Distributed ledger V1 schema for PostgreSQL.
--
-- All monetary values use the in-memory ledger's fixed-point representation:
-- an i64 scaled by 10,000. The global transaction index is the primary key on
-- `transactions.tx_id`; chargebacks are terminal rows rather than deletes.

CREATE TABLE IF NOT EXISTS clients (
    client_id BIGINT PRIMARY KEY,
    source_id VARCHAR(128) NOT NULL,
    external_client INTEGER NOT NULL
        CHECK (external_client >= 0 AND external_client <= 65535),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT clients_source_external_key UNIQUE (source_id, external_client)
);

CREATE TABLE IF NOT EXISTS accounts (
    client_id BIGINT PRIMARY KEY REFERENCES clients (client_id),
    available BIGINT NOT NULL DEFAULT 0 CHECK (available >= 0),
    held BIGINT NOT NULL DEFAULT 0 CHECK (held >= 0),
    locked BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT accounts_total_no_overflow
        CHECK (available <= 9223372036854775807 - held)
);

CREATE TABLE IF NOT EXISTS transactions (
    tx_id BIGINT PRIMARY KEY,
    client_id BIGINT NOT NULL REFERENCES clients (client_id),
    amount BIGINT NOT NULL CHECK (amount >= 0),
    state VARCHAR(16) NOT NULL
        CHECK (state IN ('settled', 'disputed', 'charged_back')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS transactions_client_id_idx
    ON transactions (client_id);

CREATE TABLE IF NOT EXISTS jobs (
    job_id VARCHAR(64) PRIMARY KEY,
    manifest_hash VARCHAR(64) NOT NULL,
    state VARCHAR(16) NOT NULL
        CHECK (state IN ('running', 'completed', 'failed', 'rejected')),
    failure_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS job_files (
    job_id VARCHAR(64) NOT NULL REFERENCES jobs (job_id),
    source_id VARCHAR(128) NOT NULL,
    file_ord INTEGER NOT NULL CHECK (file_ord >= 0),
    file_hash VARCHAR(64) NOT NULL,
    location TEXT NOT NULL,
    state VARCHAR(16) NOT NULL
        CHECK (state IN ('pending', 'staged', 'complete', 'failed')),
    row_count BIGINT NOT NULL DEFAULT 0 CHECK (row_count >= 0),
    completed_at TIMESTAMPTZ,
    PRIMARY KEY (job_id, file_ord),
    CONSTRAINT job_files_source_ord_key
        UNIQUE (job_id, source_id, file_ord)
);

CREATE TABLE IF NOT EXISTS ledger_events (
    event_id VARCHAR(64) PRIMARY KEY,
    source_id VARCHAR(128) NOT NULL,
    file_hash VARCHAR(64) NOT NULL,
    row_ord INTEGER NOT NULL CHECK (row_ord >= 0),
    first_job_id VARCHAR(64) NOT NULL REFERENCES jobs (job_id),
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ledger_events_content_key
        UNIQUE (source_id, file_hash, row_ord)
);

CREATE TABLE IF NOT EXISTS job_events (
    job_id VARCHAR(64) NOT NULL REFERENCES jobs (job_id),
    event_id VARCHAR(64) NOT NULL REFERENCES ledger_events (event_id),
    tx_id BIGINT,
    file_ord INTEGER NOT NULL,
    row_ord INTEGER NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (job_id, event_id)
);

CREATE TABLE IF NOT EXISTS job_progress (
    job_id VARCHAR(64) NOT NULL REFERENCES jobs (job_id),
    partition_id INTEGER NOT NULL CHECK (partition_id >= 0),
    emitted BIGINT NOT NULL DEFAULT 0 CHECK (emitted >= 0),
    processed BIGINT NOT NULL DEFAULT 0 CHECK (processed >= 0),
    PRIMARY KEY (job_id, partition_id),
    CONSTRAINT job_progress_processed_le_emitted CHECK (processed <= emitted)
);

CREATE TABLE IF NOT EXISTS job_clients (
    job_id VARCHAR(64) NOT NULL REFERENCES jobs (job_id),
    client_id BIGINT NOT NULL REFERENCES clients (client_id),
    PRIMARY KEY (job_id, client_id)
);

CREATE TABLE IF NOT EXISTS id_allocator (
    name VARCHAR(32) PRIMARY KEY,
    next_id BIGINT NOT NULL CHECK (next_id > 0),
    block_size BIGINT NOT NULL CHECK (block_size > 0)
);

INSERT INTO id_allocator (name, next_id, block_size)
VALUES ('client', 1, 1000)
ON CONFLICT (name) DO NOTHING;

CREATE TABLE IF NOT EXISTS job_lock (
    lock_id SMALLINT PRIMARY KEY CHECK (lock_id = 1),
    active_job_id VARCHAR(64) REFERENCES jobs (job_id),
    acquired_at TIMESTAMPTZ
);

INSERT INTO job_lock (lock_id) VALUES (1)
ON CONFLICT (lock_id) DO NOTHING;
