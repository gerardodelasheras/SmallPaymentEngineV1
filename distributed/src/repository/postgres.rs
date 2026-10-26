//! PostgreSQL adapter for the distributed ledger repository.

use async_trait::async_trait;
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::QueryBuilder;
use sqlx::Row;
use tokio::sync::Mutex;
use transaction_processor::{
    apply, Account, Amount, CanonicalClientId, JournalEffect, JournalPolicy,
    LedgerEntry, LedgerTransaction, ProcessingError, TransactionId, TransactionType,
};

use super::{
    entry_state_from_str, entry_state_str, retry_transient, AccountRow, ApplyOutcome,
    EmittedRow, FileCompletionRow, IdBlock, JobFileInput, JobFileRow, JobProgress, JobState,
    LedgerEvent, LedgerRepository, RepoError, RowJournal, TransactionRow,
};

pub struct PostgresRepository {
    pool: PgPool,
    ids: Mutex<IdBlock>,
}

impl PostgresRepository {
    pub async fn connect(database_url: &str) -> Result<Self, RepoError> {
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(database_url)
            .await?;
        Ok(Self {
            pool,
            ids: Mutex::new(IdBlock::default()),
        })
    }

    pub fn from_pool(pool: PgPool) -> Self {
        Self {
            pool,
            ids: Mutex::new(IdBlock::default()),
        }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

#[async_trait]
impl LedgerRepository for PostgresRepository {
    async fn create_job(&self, job_id: &str, manifest_hash: &str) -> Result<(), RepoError> {
        sqlx::query(
            "INSERT INTO jobs (job_id, manifest_hash, state) \
             VALUES ($1, $2, 'running') ON CONFLICT (job_id) DO NOTHING",
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
            "UPDATE jobs SET state = $2, failure_reason = $3, \
             updated_at = now(), completed_at = now() WHERE job_id = $1",
        )
        .bind(job_id)
        .bind(state.as_str())
        .bind(failure_reason)
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
                "INSERT INTO job_files \
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
            builder.push(" ON CONFLICT (job_id, file_ord) DO NOTHING");
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
                "UPDATE job_files SET state = 'complete', row_count = $4, \
                 completed_at = now() \
                 WHERE job_id = $1 AND source_id = $2 AND file_ord = $3",
            )
            .bind(job_id)
            .bind(&file.source_id)
            .bind(file_ord)
            .bind(row_count)
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
            "SELECT state FROM job_files WHERE job_id = $1 AND file_ord = $2",
        )
        .bind(job_id)
        .bind(file_ord)
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some(row) => {
                let state: String = row.try_get("state")?;
                Ok(state == "complete")
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
                "INSERT INTO job_emitted \
                 (job_id, event_id, partition_id, file_ord) \
                 VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING",
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
                 VALUES ($1, $2, $3, 0) \
                 ON CONFLICT (job_id, partition_id) DO UPDATE SET \
                 emitted = job_progress.emitted + EXCLUDED.emitted",
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
             FROM job_files jf WHERE jf.job_id = $1 ORDER BY jf.file_ord",
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
                let row_count: i64 = row.try_get("row_count")?;
                let emitted_rows: i64 = row.try_get("emitted_rows")?;
                Ok(JobFileRow {
                    file_ord,
                    source_id: row.try_get("source_id")?,
                    state: row.try_get("state")?,
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
             WHERE job_id = $1 ORDER BY partition_id",
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
        let rows = sqlx::query(
            "SELECT client_id FROM job_clients WHERE job_id = $1 ORDER BY client_id",
        )
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
             WHERE jc.job_id = $1 ORDER BY a.client_id",
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
                    locked: row.try_get("locked")?,
                })
            })
            .collect()
    }

    async fn transaction(
        &self,
        tx_id: TransactionId,
    ) -> Result<Option<TransactionRow>, RepoError> {
        let row = sqlx::query(
            "SELECT tx_id, client_id, amount, state FROM transactions WHERE tx_id = $1",
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
                state: entry_state_from_str(row.try_get("state")?)?,
            })
        })
        .transpose()
    }
}

impl PostgresRepository {
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

        if let Some((client_id,)) = sqlx::query_as::<_, (i64,)>(
            "SELECT client_id FROM clients \
             WHERE source_id = $1 AND external_client = $2",
        )
        .bind(source_id)
        .bind(external)
        .fetch_optional(&mut *db)
        .await?
        {
            db.commit().await?;
            return Ok(client_id);
        }

        let inserted = sqlx::query_as::<_, (i64,)>(
            "INSERT INTO clients (client_id, source_id, external_client) \
             VALUES ($1, $2, $3) ON CONFLICT DO NOTHING RETURNING client_id",
        )
        .bind(client_id)
        .bind(source_id)
        .bind(external)
        .fetch_optional(&mut *db)
        .await?;

        let client_id = match inserted {
            Some((client_id,)) => client_id,
            None => {
                let (client_id,) = sqlx::query_as::<_, (i64,)>(
                    "SELECT client_id FROM clients \
                     WHERE source_id = $1 AND external_client = $2",
                )
                .bind(source_id)
                .bind(external)
                .fetch_one(&mut *db)
                .await?;
                client_id
            }
        };

        db.commit().await?;
        Ok(client_id)
    }

    async fn lookup_client(
        &self,
        source_id: &str,
        external: i32,
    ) -> Result<Option<CanonicalClientId>, RepoError> {
        sqlx::query_scalar(
            "SELECT client_id FROM clients \
             WHERE source_id = $1 AND external_client = $2",
        )
        .bind(source_id)
        .bind(external)
        .fetch_optional(&self.pool)
        .await
        .map_err(RepoError::from)
    }

    async fn next_client_id(&self) -> Result<CanonicalClientId, RepoError> {
        let mut block = self.ids.lock().await;
        if let Some(id) = block.take() {
            return Ok(id);
        }

        let (start, size) = sqlx::query_as::<_, (i64, i64)>(
            "UPDATE id_allocator SET next_id = next_id + block_size \
             WHERE name = 'client' RETURNING next_id - block_size, block_size",
        )
        .fetch_one(&self.pool)
        .await?;

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

        // A parsed row initializes its account before validation.
        sqlx::query("INSERT INTO accounts (client_id) VALUES ($1) ON CONFLICT DO NOTHING")
            .bind(client)
            .execute(&mut *db)
            .await?;

        let row = sqlx::query(
            "SELECT available, held, locked FROM accounts \
             WHERE client_id = $1 FOR UPDATE",
        )
        .bind(client)
        .fetch_one(&mut *db)
        .await?;

        let mut account = Account {
            available: Amount::from_scaled(row.try_get("available")?),
            held: Amount::from_scaled(row.try_get("held")?),
            locked: row.try_get("locked")?,
        };
        let was_locked = account.locked;

        let event_inserted = sqlx::query_as::<_, (String,)>(
            "INSERT INTO ledger_events (event_id, source_id, file_hash, row_ord, first_job_id) \
             VALUES ($1, $2, $3, $4, $5) \
             ON CONFLICT DO NOTHING RETURNING event_id",
        )
        .bind(&event.event_id)
        .bind(&event.source_id)
        .bind(&event.file_hash)
        .bind(event.row_ord)
        .bind(&event.job_id)
        .fetch_optional(&mut *db)
        .await?;
        let duplicate = event_inserted.is_none();

        let job_event_inserted = sqlx::query_as::<_, (String,)>(
            "INSERT INTO job_events (job_id, event_id, tx_id, file_ord, row_ord) \
             VALUES ($1, $2, $3, $4, $5) \
             ON CONFLICT DO NOTHING RETURNING event_id",
        )
        .bind(&event.job_id)
        .bind(&event.event_id)
        .bind(i64::from(tx_id))
        .bind(event.file_ord)
        .bind(event.row_ord)
        .fetch_optional(&mut *db)
        .await?;

        sqlx::query(
            "INSERT INTO job_clients (job_id, client_id) VALUES ($1, $2) \
             ON CONFLICT DO NOTHING",
        )
        .bind(&event.job_id)
        .bind(client)
        .execute(&mut *db)
        .await?;

        if job_event_inserted.is_some() {
            let updated = sqlx::query(
                "UPDATE job_progress SET processed = processed + 1 \
                 WHERE job_id = $1 AND partition_id = $2",
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
                "SELECT client_id, amount, state FROM transactions WHERE tx_id = $1",
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
                        state: entry_state_from_str(row.try_get("state")?)?,
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
                                 VALUES ($1, $2, $3, $4) \
                                 ON CONFLICT (tx_id) DO UPDATE SET \
                                 client_id = EXCLUDED.client_id, \
                                 amount = EXCLUDED.amount, \
                                 state = EXCLUDED.state, \
                                 updated_at = now()",
                            )
                            .bind(i64::from(tx_id))
                            .bind(entry.client)
                            .bind(entry.amount.scaled())
                            .bind(entry_state_str(entry.state))
                            .execute(&mut *db)
                            .await?;
                        }
                        JournalEffect::Remove => {
                            sqlx::query("DELETE FROM transactions WHERE tx_id = $1")
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
        db: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        client: CanonicalClientId,
        account: &Account,
    ) -> Result<(), RepoError> {
        sqlx::query(
            "UPDATE accounts SET available = $2, held = $3, locked = $4, \
             updated_at = now() WHERE client_id = $1",
        )
        .bind(client)
        .bind(account.available.scaled())
        .bind(account.held.scaled())
        .bind(account.locked)
        .execute(&mut **db)
        .await?;
        Ok(())
    }
}
