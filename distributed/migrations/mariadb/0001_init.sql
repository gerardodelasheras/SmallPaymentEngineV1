-- Distributed ledger V1 schema for MariaDB 11.4 / Galera.
--
-- All monetary values use the in-memory ledger's fixed-point representation:
-- an i64 scaled by 10,000. The global transaction index is the primary key on
-- `transactions.tx_id`; chargebacks are terminal rows rather than deletes.
--
-- Galera replicates this full schema to every node. It is high availability,
-- not sharding.

CREATE TABLE IF NOT EXISTS clients (
    client_id BIGINT NOT NULL,
    source_id VARCHAR(128) NOT NULL,
    external_client INT NOT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (client_id),
    UNIQUE KEY clients_source_external_key (source_id, external_client),
    CONSTRAINT clients_external_client_range
        CHECK (external_client BETWEEN 0 AND 65535)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS accounts (
    client_id BIGINT NOT NULL,
    available BIGINT NOT NULL DEFAULT 0,
    held BIGINT NOT NULL DEFAULT 0,
    locked TINYINT(1) NOT NULL DEFAULT 0,
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (client_id),
    CONSTRAINT accounts_client_fk FOREIGN KEY (client_id) REFERENCES clients (client_id),
    CONSTRAINT accounts_available_non_negative CHECK (available >= 0),
    CONSTRAINT accounts_held_non_negative CHECK (held >= 0),
    CONSTRAINT accounts_locked_boolean CHECK (locked IN (0, 1)),
    CONSTRAINT accounts_total_no_overflow
        CHECK (available <= 9223372036854775807 - held)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS transactions (
    tx_id BIGINT NOT NULL,
    client_id BIGINT NOT NULL,
    amount BIGINT NOT NULL,
    state VARCHAR(16) NOT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (tx_id),
    KEY transactions_client_id_idx (client_id),
    CONSTRAINT transactions_client_fk FOREIGN KEY (client_id) REFERENCES clients (client_id),
    CONSTRAINT transactions_amount_non_negative CHECK (amount >= 0),
    CONSTRAINT transactions_state_valid
        CHECK (state IN ('settled', 'disputed', 'charged_back'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS jobs (
    job_id VARCHAR(64) NOT NULL,
    manifest_hash VARCHAR(64) NOT NULL,
    state VARCHAR(16) NOT NULL,
    failure_reason TEXT,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    completed_at DATETIME(6),
    PRIMARY KEY (job_id),
    CONSTRAINT jobs_state_valid
        CHECK (state IN ('running', 'completed', 'failed', 'rejected'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS job_files (
    job_id VARCHAR(64) NOT NULL,
    source_id VARCHAR(128) NOT NULL,
    file_ord INT NOT NULL,
    file_hash VARCHAR(64) NOT NULL,
    location TEXT NOT NULL,
    state VARCHAR(16) NOT NULL,
    row_count BIGINT NOT NULL DEFAULT 0,
    completed_at DATETIME(6),
    PRIMARY KEY (job_id, file_ord),
    UNIQUE KEY job_files_source_ord_key (job_id, source_id, file_ord),
    CONSTRAINT job_files_job_fk FOREIGN KEY (job_id) REFERENCES jobs (job_id),
    CONSTRAINT job_files_file_ord_non_negative CHECK (file_ord >= 0),
    CONSTRAINT job_files_row_count_non_negative CHECK (row_count >= 0),
    CONSTRAINT job_files_state_valid
        CHECK (state IN ('pending', 'staged', 'complete', 'failed'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS ledger_events (
    event_id VARCHAR(64) NOT NULL,
    source_id VARCHAR(128) NOT NULL,
    file_hash VARCHAR(64) NOT NULL,
    row_ord INT NOT NULL,
    first_job_id VARCHAR(64) NOT NULL,
    applied_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (event_id),
    UNIQUE KEY ledger_events_content_key (source_id, file_hash, row_ord),
    CONSTRAINT ledger_events_job_fk
        FOREIGN KEY (first_job_id) REFERENCES jobs (job_id),
    CONSTRAINT ledger_events_row_ord_non_negative CHECK (row_ord >= 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS job_events (
    job_id VARCHAR(64) NOT NULL,
    event_id VARCHAR(64) NOT NULL,
    tx_id BIGINT,
    file_ord INT NOT NULL,
    row_ord INT NOT NULL,
    applied_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (job_id, event_id),
    CONSTRAINT job_events_job_fk FOREIGN KEY (job_id) REFERENCES jobs (job_id),
    CONSTRAINT job_events_event_fk FOREIGN KEY (event_id) REFERENCES ledger_events (event_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS job_progress (
    job_id VARCHAR(64) NOT NULL,
    partition_id INT NOT NULL,
    emitted BIGINT NOT NULL DEFAULT 0,
    processed BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (job_id, partition_id),
    CONSTRAINT job_progress_job_fk FOREIGN KEY (job_id) REFERENCES jobs (job_id),
    CONSTRAINT job_progress_partition_non_negative CHECK (partition_id >= 0),
    CONSTRAINT job_progress_emitted_non_negative CHECK (emitted >= 0),
    CONSTRAINT job_progress_processed_non_negative CHECK (processed >= 0),
    CONSTRAINT job_progress_processed_le_emitted CHECK (processed <= emitted)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS job_clients (
    job_id VARCHAR(64) NOT NULL,
    client_id BIGINT NOT NULL,
    PRIMARY KEY (job_id, client_id),
    CONSTRAINT job_clients_job_fk FOREIGN KEY (job_id) REFERENCES jobs (job_id),
    CONSTRAINT job_clients_client_fk FOREIGN KEY (client_id) REFERENCES clients (client_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS id_allocator (
    name VARCHAR(32) NOT NULL,
    next_id BIGINT NOT NULL,
    block_size BIGINT NOT NULL,
    PRIMARY KEY (name),
    CONSTRAINT id_allocator_next_id_positive CHECK (next_id > 0),
    CONSTRAINT id_allocator_block_size_positive CHECK (block_size > 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

INSERT IGNORE INTO id_allocator (name, next_id, block_size)
VALUES ('client', 1, 1000);

CREATE TABLE IF NOT EXISTS job_lock (
    lock_id TINYINT NOT NULL,
    active_job_id VARCHAR(64) NULL,
    acquired_at DATETIME(6),
    PRIMARY KEY (lock_id),
    CONSTRAINT job_lock_singleton CHECK (lock_id = 1),
    CONSTRAINT job_lock_job_fk FOREIGN KEY (active_job_id) REFERENCES jobs (job_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

INSERT IGNORE INTO job_lock (lock_id) VALUES (1);
