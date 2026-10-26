//! MariaDB / Galera adapter for the distributed ledger repository.
//!
//! Galera is virtually synchronous, full-copy, multi-primary replication used
//! for availability. It is not sharding; V1 uses one logical database.

use async_trait::async_trait;
use sqlx::mysql::{MySqlPool, MySqlPoolOptions};
use sqlx::Row;
use tokio::sync::Mutex;
use transaction_processor::{
    apply, Account, Amount, CanonicalClientId, EntryState, JournalEffect, JournalPolicy,
    LedgerEntry, LedgerTransaction, ProcessingError, TransactionId,
};

use super::{
    entry_state_from_str, entry_state_str, retry_transient, AccountRow, ApplyOutcome, IdBlock,
    JobProgress, JobState, LedgerEvent, LedgerRepository, RepoError, RowJournal, TransactionRow,
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

    async fn finish_job(
        &self,
        job_id: &str,
        state: JobState,
        failure_reason: Option<&str>,
    ) -> Result<(), RepoError> {
        sqlx::query(
            "UPDATE jobs SET state = ?, failure_reason = ?, \
             updated_at = CURRENT_TIMESTAMP(6), completed_at = CURRENT_TIMESTAMP(6) \
             WHERE job_id = ?",
        )
        .bind(state.as_str())
        .bind(failure_reason)
        .bind(job_id)
        .execute(&self.pool)
        .await?;
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
        retry_transient(|| self.apply_event_once(event)).await
    }

    async fn create_job_file(
        &self,
        job_id: &str,
        source_id: &str,
        file_ord: u32,
        file_hash: &str,
        location: &str,
    ) -> Result<(), RepoError> {
        let file_ord = i32::try_from(file_ord).map_err(|_| {
            RepoError::InvalidState("file ordinal exceeds i32".to_string())
        })?;
        sqlx::query(
            "INSERT IGNORE INTO job_files \
             (job_id, source_id, file_ord, file_hash, location, state) \
             VALUES (?, ?, ?, ?, ?, 'pending')",
        )
        .bind(job_id)
        .bind(source_id)
        .bind(file_ord)
        .bind(file_hash)
        .bind(location)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn mark_file_complete(
        &self,
        job_id: &str,
        source_id: &str,
        file_ord: u32,
        row_count: u64,
    ) -> Result<(), RepoError> {
        let file_ord = i32::try_from(file_ord).map_err(|_| {
            RepoError::InvalidState("file ordinal exceeds i32".to_string())
        })?;
        let row_count = i64::try_from(row_count).map_err(|_| {
            RepoError::InvalidState("row count exceeds i64".to_string())
        })?;
        let updated = sqlx::query(
            "UPDATE job_files SET state = 'complete', row_count = ?, \
             completed_at = CURRENT_TIMESTAMP(6) \
             WHERE job_id = ? AND source_id = ? AND file_ord = ?",
        )
        .bind(row_count)
        .bind(job_id)
        .bind(source_id)
        .bind(file_ord)
        .execute(&self.pool)
        .await?;
        if updated.rows_affected() == 0 {
            return Err(RepoError::Inconsistent(format!(
                "job file {job_id}/{source_id}/{file_ord} is not registered"
            )));
        }
        Ok(())
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
        count: i64,
    ) -> Result<(), RepoError> {
        sqlx::query(
            "INSERT INTO job_progress (job_id, partition_id, emitted, processed) \
             VALUES (?, ?, ?, 0) \
             ON DUPLICATE KEY UPDATE emitted = emitted + VALUES(emitted)",
        )
        .bind(job_id)
        .bind(partition)
        .bind(count)
        .execute(&self.pool)
        .await?;
        Ok(())
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

            match apply(
                &mut account,
                &ledger_transaction,
                &journal,
                JournalPolicy::RetainChargedBack,
            ) {
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
