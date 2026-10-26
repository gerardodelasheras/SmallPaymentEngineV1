//! MariaDB / Galera adapter for the distributed ledger repository.
//!
//! Galera is virtually synchronous, full-copy, multi-primary replication used
//! for availability. It is not sharding; V1 uses one logical database.

use async_trait::async_trait;
use sqlx::mysql::{MySqlPool, MySqlPoolOptions};
use sqlx::QueryBuilder;
use sqlx::Row;
use tokio::sync::Mutex;
use transaction_processor::{
    apply, Account, Amount, CanonicalClientId, EntryState, JournalEffect, JournalPolicy,
    LedgerEntry, LedgerTransaction, ProcessingError, TransactionId, TransactionType,
};

use super::{
    entry_state_from_str, entry_state_str, retry_transient, AccountRow, ApplyOutcome,
    EmittedRow, FileCompletionRow, IdBlock, JobFileInput, JobFileRow, JobProgress, JobState,
    JobMetrics, JobStatus, LedgerEvent, LedgerRepository, RepoError, RowJournal, TransactionRow,
};

pub struct MariaDbRepository {
    pool: MySqlPool,
    ids: Mutex<IdBlock>,
}

impl MariaDbRepository {
    pub async fn connect(database_url: &str) -> Result<Self, RepoError> {
        let pool = MySqlPoolOptions::new()
            .max_connections(10)
            .connect(database_url)
            .await?;
        Ok(Self {
            pool,
            ids: Mutex::new(IdBlock::default()),
        })
    }

    pub fn from_pool(pool: MySqlPool) -> Self {
        Self {
            pool,
            ids: Mutex::new(IdBlock::default()),
        }
    }

    pub fn pool(&self) -> &MySqlPool {
        &self.pool
    }
}

fn locked_value(locked: bool) -> i8 {
    if locked {
        1
    } else {
        0
    }
}

fn state_from_row(row: &sqlx::mysql::MySqlRow) -> Result<EntryState, RepoError> {
    let raw: Vec<u8> = row.try_get("state")?;
    let text = String::from_utf8(raw)
        .map_err(|error| RepoError::InvalidState(error.to_string()))?;
    entry_state_from_str(&text)
}

#[async_trait]
impl LedgerRepository for MariaDbRepository {
    async fn create_job(&self, job_id: &str, manifest_hash: &str) -> Result<(), RepoError> {
        sqlx::query(
            "INSERT IGNORE INTO jobs (job_id, manifest_hash, state) VALUES (?, ?, 'running')",
        )
        .bind(job_id)
        .bind(manifest_hash)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn submit_job(&self, job_id: &str, manifest_hash: &str) -> Result<(), RepoError> {
        let mut db = self.pool.begin().await?;
        let active: Option<Vec<u8>> = sqlx::query_scalar(
            "SELECT active_job_id FROM job_lock WHERE lock_id = 1 FOR UPDATE",
        )
        .fetch_one(&mut *db)
        .await?;
        if let Some(active) = active {
            let active = String::from_utf8(active)
                .map_err(|error| RepoError::InvalidState(error.to_string()))?;
            if active != job_id {
                return Err(RepoError::JobAlreadyActive { active });
            }
        }
        sqlx::query(
            "INSERT IGNORE INTO jobs (job_id, manifest_hash, state) VALUES (?, ?, 'running')",
        )
        .bind(job_id)
        .bind(manifest_hash)
        .execute(&mut *db)
        .await?;
        sqlx::query("UPDATE job_lock SET active_job_id = ?, acquired_at = CURRENT_TIMESTAMP(6) WHERE lock_id = 1")
            .bind(job_id)
            .execute(&mut *db)
            .await?;
        db.commit().await?;
        Ok(())
    }

    async fn job_status(&self, job_id: &str) -> Result<JobStatus, RepoError> {
        let row = sqlx::query("SELECT state, failure_reason FROM jobs WHERE job_id = ?")
            .bind(job_id)
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| RepoError::JobNotFound(job_id.to_string()))?;
        let state: Vec<u8> = row.try_get("state")?;
        let state = String::from_utf8(state)
            .map_err(|error| RepoError::InvalidState(error.to_string()))?;
        let failure_reason: Option<Vec<u8>> = row.try_get("failure_reason")?;
        let failure_reason = failure_reason
            .map(|value| {
                String::from_utf8(value)
                    .map_err(|error| RepoError::InvalidState(error.to_string()))
            })
            .transpose()?;
        let pending: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM job_files jf \
             WHERE jf.job_id = ? AND (jf.state <> 'complete' OR jf.row_count <> (\
                 SELECT COUNT(*) FROM job_emitted je \
                 WHERE je.job_id = jf.job_id AND je.file_ord = jf.file_ord))",
        )
        .bind(job_id)
        .fetch_one(&self.pool)
        .await?;

        Ok(JobStatus {
            job_id: job_id.to_string(),
            state: JobState::parse(&state)?,
            failure_reason,
            progress: self.job_progress(job_id).await?,
            partitioner_complete: pending == 0,
        })
    }

    async fn job_metrics(&self) -> Result<Vec<JobMetrics>, RepoError> {
        let rows = sqlx::query(
            "SELECT j.job_id, j.state, \
             (SELECT COUNT(*) FROM job_files jf WHERE jf.job_id = j.job_id AND (\
                 jf.state <> 'complete' OR jf.row_count <> (\
                     SELECT COUNT(*) FROM job_emitted je \
                     WHERE je.job_id = jf.job_id AND je.file_ord = jf.file_ord))) AS pending_files, \
             CAST(COALESCE((SELECT SUM(jp.emitted - jp.processed) FROM job_progress jp \
                 WHERE jp.job_id = j.job_id), 0) AS SIGNED) AS pending_events \
             FROM jobs j ORDER BY j.job_id",
        )
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                let job_id: Vec<u8> = row.try_get("job_id")?;
                let job_id = String::from_utf8(job_id)
                    .map_err(|error| RepoError::InvalidState(error.to_string()))?;
                let pending_files: i64 = row.try_get("pending_files")?;
                let pending_events: i64 = row.try_get("pending_events")?;
                let state: Vec<u8> = row.try_get("state")?;
                let state = JobState::parse(&String::from_utf8(state).map_err(|error| RepoError::InvalidState(error.to_string()))?)?;
                Ok(JobMetrics {
                    job_id,
                    state,
                    partitioner_complete: pending_files == 0,
                    pending_events,
                })
            })
            .collect()
    }

    async fn finish_job(
        &self,
        job_id: &str,
        state: JobState,
        failure_reason: Option<&str>,
    ) -> Result<(), RepoError> {
        let mut db = self.pool.begin().await?;
        sqlx::query(
            "UPDATE jobs SET state = ?, failure_reason = ?, \
             updated_at = CURRENT_TIMESTAMP(6), completed_at = CURRENT_TIMESTAMP(6) \
             WHERE job_id = ?",
        )
        .bind(state.as_str())
        .bind(failure_reason)
        .bind(job_id)
        .execute(&mut *db)
        .await?;
        sqlx::query(
            "UPDATE job_lock SET active_job_id = NULL, acquired_at = NULL \
             WHERE lock_id = 1 AND active_job_id = ?",
        )
        .bind(job_id)
        .execute(&mut *db)
        .await?;
        db.commit().await?;
        Ok(())
    }

    async fn mint_or_get_client(
        &self,
        source_id: &str,
        external_client: u16,
    ) -> Result<CanonicalClientId, RepoError> {
        retry_transient(|| self.mint_or_get_client_once(source_id, external_client)).await
    }

    async fn apply_event(&self, event: &LedgerEvent) -> Result<ApplyOutcome, RepoError> {
        retry_transient(|| async {
            super::maybe_inject_fault()?;
            self.apply_event_once(event).await
        })
        .await
    }

    async fn create_job_files(
        &self,
        job_id: &str,
        files: &[JobFileInput],
    ) -> Result<(), RepoError> {
        if files.is_empty() {
            return Ok(());
        }

        let rows: Vec<(i32, &str, &str, &str)> = files
            .iter()
            .map(|file| {
                let file_ord = i32::try_from(file.file_ord).map_err(|_| {
                    RepoError::InvalidState("file ordinal exceeds i32".to_string())
                })?;
                Ok((
                    file_ord,
                    file.source_id.as_str(),
                    file.file_hash.as_str(),
                    file.location.as_str(),
                ))
            })
            .collect::<Result<_, RepoError>>()?;

        for chunk in rows.chunks(1_000) {
            let mut builder = QueryBuilder::new(
                "INSERT IGNORE INTO job_files \
                 (job_id, source_id, file_ord, file_hash, location, state) ",
            );
            builder.push_values(chunk, |mut row, (file_ord, source_id, file_hash, location)| {
                row.push_bind(job_id)
                    .push_bind(*source_id)
                    .push_bind(*file_ord)
                    .push_bind(*file_hash)
                    .push_bind(*location)
                    .push_bind("pending");
            });
            builder.build().execute(&self.pool).await?;
        }
        Ok(())
    }

    async fn mark_files_complete(
        &self,
        job_id: &str,
        files: &[FileCompletionRow],
    ) -> Result<u64, RepoError> {
        if files.is_empty() {
            return Ok(0);
        }

        let mut db = self.pool.begin().await?;
        let mut updated = 0_u64;
        for file in files {
            let file_ord = i32::try_from(file.file_ord).map_err(|_| {
                RepoError::InvalidState("file ordinal exceeds i32".to_string())
            })?;
            let row_count = i64::try_from(file.row_count).map_err(|_| {
                RepoError::InvalidState("row count exceeds i64".to_string())
            })?;
            let result = sqlx::query(
                "UPDATE job_files SET state = 'complete', row_count = ?, \
                 completed_at = CURRENT_TIMESTAMP(6) \
                 WHERE job_id = ? AND source_id = ? AND file_ord = ?",
            )
            .bind(row_count)
            .bind(job_id)
            .bind(&file.source_id)
            .bind(file_ord)
            .execute(&mut *db)
            .await?;
            updated += result.rows_affected();
        }
        db.commit().await?;
        Ok(updated)
    }

    async fn job_file_complete(&self, job_id: &str, file_ord: u32) -> Result<bool, RepoError> {
        let file_ord = i32::try_from(file_ord).map_err(|_| {
            RepoError::InvalidState("file ordinal exceeds i32".to_string())
        })?;
        let row = sqlx::query(
            "SELECT state FROM job_files WHERE job_id = ? AND file_ord = ?",
        )
        .bind(job_id)
        .bind(file_ord)
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some(row) => {
                let state: Vec<u8> = row.try_get("state")?;
                Ok(state == b"complete")
            }
            None => Ok(false),
        }
    }

    async fn record_emitted(
        &self,
        job_id: &str,
        partition: i32,
        rows: &[EmittedRow],
    ) -> Result<u64, RepoError> {
        if rows.is_empty() {
            return Ok(0);
        }

        let mut db = self.pool.begin().await?;
        let mut inserted = 0_u64;
        for row in rows {
            let file_ord = i32::try_from(row.file_ord).map_err(|_| {
                RepoError::InvalidState("file ordinal exceeds i32".to_string())
            })?;
            let result = sqlx::query(
                "INSERT IGNORE INTO job_emitted \
                 (job_id, event_id, partition_id, file_ord) \
                 VALUES (?, ?, ?, ?)",
            )
            .bind(job_id)
            .bind(&row.event_id)
            .bind(partition)
            .bind(file_ord)
            .execute(&mut *db)
            .await?;
            inserted += result.rows_affected();
        }

        if inserted > 0 {
            sqlx::query(
                "INSERT INTO job_progress (job_id, partition_id, emitted, processed) \
                 VALUES (?, ?, ?, 0) \
                 ON DUPLICATE KEY UPDATE emitted = emitted + VALUES(emitted)",
            )
            .bind(job_id)
            .bind(partition)
            .bind(inserted as i64)
            .execute(&mut *db)
            .await?;
        }

        db.commit().await?;
        Ok(inserted)
    }

    async fn job_files(&self, job_id: &str) -> Result<Vec<JobFileRow>, RepoError> {
        let rows = sqlx::query(
            "SELECT jf.file_ord, jf.source_id, jf.state, jf.row_count, \
             (SELECT COUNT(*) FROM job_emitted je \
              WHERE je.job_id = jf.job_id AND je.file_ord = jf.file_ord) \
              AS emitted_rows \
             FROM job_files jf WHERE jf.job_id = ? ORDER BY jf.file_ord",
        )
        .bind(job_id)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                let file_ord: i32 = row.try_get("file_ord")?;
                let file_ord = u32::try_from(file_ord).map_err(|_| {
                    RepoError::InvalidState("file ordinal is negative".to_string())
                })?;
                let source_id: Vec<u8> = row.try_get("source_id")?;
                let state: Vec<u8> = row.try_get("state")?;
                let row_count: i64 = row.try_get("row_count")?;
                let emitted_rows: i64 = row.try_get("emitted_rows")?;
                Ok(JobFileRow {
                    file_ord,
                    source_id: String::from_utf8(source_id)
                        .map_err(|error| RepoError::InvalidState(error.to_string()))?,
                    state: String::from_utf8(state)
                        .map_err(|error| RepoError::InvalidState(error.to_string()))?,
                    row_count: u64::try_from(row_count).map_err(|_| {
                        RepoError::InvalidState("row count is negative".to_string())
                    })?,
                    emitted_rows: u64::try_from(emitted_rows).unwrap_or(0),
                })
            })
            .collect()
    }

    async fn job_progress(&self, job_id: &str) -> Result<Vec<JobProgress>, RepoError> {
        let rows = sqlx::query(
            "SELECT partition_id, emitted, processed FROM job_progress \
             WHERE job_id = ? ORDER BY partition_id",
        )
        .bind(job_id)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(JobProgress {
                    partition: row.try_get("partition_id")?,
                    emitted: row.try_get("emitted")?,
                    processed: row.try_get("processed")?,
                })
            })
            .collect()
    }

    async fn job_clients(&self, job_id: &str) -> Result<Vec<CanonicalClientId>, RepoError> {
        let rows =
            sqlx::query("SELECT client_id FROM job_clients WHERE job_id = ? ORDER BY client_id")
                .bind(job_id)
                .fetch_all(&self.pool)
                .await?;
        rows.into_iter()
            .map(|row| Ok(row.try_get("client_id")?))
            .collect()
    }

    async fn accounts_for_job(&self, job_id: &str) -> Result<Vec<AccountRow>, RepoError> {
        let rows = sqlx::query(
            "SELECT a.client_id, a.available, a.held, a.locked \
             FROM job_clients jc JOIN accounts a ON a.client_id = jc.client_id \
             WHERE jc.job_id = ? ORDER BY a.client_id",
        )
        .bind(job_id)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(AccountRow {
                    client_id: row.try_get("client_id")?,
                    available: Amount::from_scaled(row.try_get("available")?),
                    held: Amount::from_scaled(row.try_get("held")?),
                    locked: row.try_get::<i8, _>("locked")? != 0,
                })
            })
            .collect()
    }

    async fn transaction(
        &self,
        tx_id: TransactionId,
    ) -> Result<Option<TransactionRow>, RepoError> {
        let row = sqlx::query(
            "SELECT tx_id, client_id, amount, state FROM transactions WHERE tx_id = ?",
        )
        .bind(i64::from(tx_id))
        .fetch_optional(&self.pool)
        .await?;

        row.map(|row| {
            let stored: i64 = row.try_get("tx_id")?;
            let tx_id = TransactionId::try_from(stored)
                .map_err(|_| RepoError::InvalidState(format!("tx_id {stored} exceeds u32")))?;
            Ok(TransactionRow {
                tx_id,
                client_id: row.try_get("client_id")?,
                amount: Amount::from_scaled(row.try_get("amount")?),
                state: state_from_row(&row)?,
            })
        })
        .transpose()
    }
}

impl MariaDbRepository {
    async fn mint_or_get_client_once(
        &self,
        source_id: &str,
        external_client: u16,
    ) -> Result<CanonicalClientId, RepoError> {
        let external = i32::from(external_client);
        if let Some(client_id) = self.lookup_client(source_id, external).await? {
            return Ok(client_id);
        }

        let client_id = self.next_client_id().await?;
        let mut db = self.pool.begin().await?;

        if let Some(row) = sqlx::query(
            "SELECT client_id FROM clients WHERE source_id = ? AND external_client = ?",
        )
        .bind(source_id)
        .bind(external)
        .fetch_optional(&mut *db)
        .await?
        {
            let client_id = row.try_get("client_id")?;
            db.commit().await?;
            return Ok(client_id);
        }

        let inserted = sqlx::query(
            "INSERT IGNORE INTO clients (client_id, source_id, external_client) \
             VALUES (?, ?, ?)",
        )
        .bind(client_id)
        .bind(source_id)
        .bind(external)
        .execute(&mut *db)
        .await?;

        let client_id = if inserted.rows_affected() == 0 {
            // A locking read sees the row committed by the winning transaction,
            // even under MariaDB's default REPEATABLE READ snapshot.
            let row = sqlx::query(
                "SELECT client_id FROM clients \
                 WHERE source_id = ? AND external_client = ? FOR UPDATE",
            )
            .bind(source_id)
            .bind(external)
            .fetch_one(&mut *db)
            .await?;
            row.try_get("client_id")?
        } else {
            client_id
        };

        db.commit().await?;
        Ok(client_id)
    }

    async fn lookup_client(
        &self,
        source_id: &str,
        external: i32,
    ) -> Result<Option<CanonicalClientId>, RepoError> {
        let row = sqlx::query(
            "SELECT client_id FROM clients WHERE source_id = ? AND external_client = ?",
        )
        .bind(source_id)
        .bind(external)
        .fetch_optional(&self.pool)
        .await?;
        row.map(|row| Ok(row.try_get("client_id")?)).transpose()
    }

    async fn next_client_id(&self) -> Result<CanonicalClientId, RepoError> {
        let mut block = self.ids.lock().await;
        if let Some(id) = block.take() {
            return Ok(id);
        }

        let mut db = self.pool.begin().await?;
        let row = sqlx::query(
            "SELECT next_id, block_size FROM id_allocator WHERE name = 'client' FOR UPDATE",
        )
        .fetch_one(&mut *db)
        .await?;
        let start: i64 = row.try_get("next_id")?;
        let size: i64 = row.try_get("block_size")?;

        sqlx::query("UPDATE id_allocator SET next_id = ? WHERE name = 'client'")
            .bind(start + size)
            .execute(&mut *db)
            .await?;
        db.commit().await?;

        *block = IdBlock {
            next: start + 1,
            end: start + size,
        };
        Ok(start)
    }

    async fn apply_event_once(&self, event: &LedgerEvent) -> Result<ApplyOutcome, RepoError> {
        let client = event.transaction.client;
        let tx_id = event.transaction.transaction_id;
        let mut db = self.pool.begin().await?;

        sqlx::query("INSERT IGNORE INTO accounts (client_id) VALUES (?)")
            .bind(client)
            .execute(&mut *db)
            .await?;

        let row = sqlx::query(
            "SELECT available, held, locked FROM accounts WHERE client_id = ? FOR UPDATE",
        )
        .bind(client)
        .fetch_one(&mut *db)
        .await?;

        let mut account = Account {
            available: Amount::from_scaled(row.try_get("available")?),
            held: Amount::from_scaled(row.try_get("held")?),
            locked: row.try_get::<i8, _>("locked")? != 0,
        };
        let was_locked = account.locked;

        let event_inserted = sqlx::query(
            "INSERT IGNORE INTO ledger_events \
             (event_id, source_id, file_hash, row_ord, first_job_id) \
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(&event.event_id)
        .bind(&event.source_id)
        .bind(&event.file_hash)
        .bind(event.row_ord)
        .bind(&event.job_id)
        .execute(&mut *db)
        .await?;
        let duplicate = event_inserted.rows_affected() == 0;

        let job_event_inserted = sqlx::query(
            "INSERT IGNORE INTO job_events \
             (job_id, event_id, tx_id, file_ord, row_ord) \
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(&event.job_id)
        .bind(&event.event_id)
        .bind(i64::from(tx_id))
        .bind(event.file_ord)
        .bind(event.row_ord)
        .execute(&mut *db)
        .await?
        .rows_affected()
            > 0;

        sqlx::query("INSERT IGNORE INTO job_clients (job_id, client_id) VALUES (?, ?)")
            .bind(&event.job_id)
            .bind(client)
            .execute(&mut *db)
            .await?;

        if job_event_inserted {
            let updated = sqlx::query(
                "UPDATE job_progress SET processed = processed + 1 \
                 WHERE job_id = ? AND partition_id = ?",
            )
            .bind(&event.job_id)
            .bind(event.partition)
            .execute(&mut *db)
            .await?;
            if updated.rows_affected() == 0 {
                return Err(RepoError::MissingProgress {
                    job_id: event.job_id.clone(),
                    partition: event.partition,
                });
            }
        }

        let mut row_error: Option<ProcessingError> = None;
        if !duplicate {
            let tx_row = sqlx::query(
                "SELECT client_id, amount, state FROM transactions WHERE tx_id = ?",
            )
            .bind(i64::from(tx_id))
            .fetch_optional(&mut *db)
            .await?;

            let journal = RowJournal {
                tx_id,
                entry: match tx_row {
                    Some(row) => Some(LedgerEntry {
                        client: row.try_get("client_id")?,
                        amount: Amount::from_scaled(row.try_get("amount")?),
                        state: state_from_row(&row)?,
                    }),
                    None => None,
                },
            };

            let ledger_transaction = LedgerTransaction {
                transaction_type: event.transaction.transaction_type,
                client,
                transaction_id: tx_id,
                amount: event.transaction.amount,
            };

            // A monetary row that reuses an existing identifier is a duplicate
            // even when the account is already locked. The shared core checks
            // the lock first; the distributed terminal journal must classify
            // reuse distinctly (charged-back IDs are never reusable).
            let domain_result = if journal.entry.is_some()
                && matches!(
                    ledger_transaction.transaction_type,
                    TransactionType::Deposit | TransactionType::Withdrawal
                ) {
                Err(ProcessingError::DuplicateTransaction(tx_id))
            } else {
                apply(
                    &mut account,
                    &ledger_transaction,
                    &journal,
                    JournalPolicy::RetainChargedBack,
                )
            };

            match domain_result {
                Ok(effect) => {
                    self.update_account(&mut db, client, &account).await?;
                    match effect {
                        JournalEffect::Insert(entry) => {
                            sqlx::query(
                                "INSERT INTO transactions (tx_id, client_id, amount, state) \
                                 VALUES (?, ?, ?, ?) \
                                 ON DUPLICATE KEY UPDATE \
                                 client_id = VALUES(client_id), \
                                 amount = VALUES(amount), \
                                 state = VALUES(state), \
                                 updated_at = CURRENT_TIMESTAMP(6)",
                            )
                            .bind(i64::from(tx_id))
                            .bind(entry.client)
                            .bind(entry.amount.scaled())
                            .bind(entry_state_str(entry.state))
                            .execute(&mut *db)
                            .await?;
                        }
                        JournalEffect::Remove => {
                            sqlx::query("DELETE FROM transactions WHERE tx_id = ?")
                                .bind(i64::from(tx_id))
                                .execute(&mut *db)
                                .await?;
                        }
                    }
                }
                Err(error) => {
                    row_error = Some(error);
                    if account.locked != was_locked {
                        self.update_account(&mut db, client, &account).await?;
                    }
                }
            }
        }

        db.commit().await?;
        Ok(ApplyOutcome {
            duplicate,
            locked: account.locked,
            client_id: client,
            row_error,
        })
    }

    async fn update_account(
        &self,
        db: &mut sqlx::Transaction<'_, sqlx::MySql>,
        client: CanonicalClientId,
        account: &Account,
    ) -> Result<(), RepoError> {
        sqlx::query(
            "UPDATE accounts SET available = ?, held = ?, locked = ?, \
             updated_at = CURRENT_TIMESTAMP(6) WHERE client_id = ?",
        )
        .bind(account.available.scaled())
        .bind(account.held.scaled())
        .bind(locked_value(account.locked))
        .bind(client)
        .execute(&mut **db)
        .await?;
        Ok(())
    }
}
