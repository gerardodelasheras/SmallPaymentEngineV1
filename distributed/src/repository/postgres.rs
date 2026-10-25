//! PostgreSQL adapter for the distributed ledger repository.

use async_trait::async_trait;
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::Row;
use transaction_processor::{
    apply, Account, Amount, CanonicalClientId, EntryState, Journal, JournalEffect,
    JournalPolicy, LedgerEntry, LedgerTransaction, ProcessingError, TransactionId,
};

use super::{
    AccountRow, ApplyOutcome, JobProgress, JobState, LedgerEvent, LedgerRepository, RepoError,
    TransactionRow,
};

pub struct PostgresRepository {
    pool: PgPool,
}

impl PostgresRepository {
    pub async fn connect(database_url: &str) -> Result<Self, RepoError> {
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(database_url)
            .await?;
        Ok(Self { pool })
    }

    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

struct RowJournal {
    tx_id: TransactionId,
    entry: Option<LedgerEntry<CanonicalClientId>>,
}

impl Journal<CanonicalClientId> for RowJournal {
    fn get(&self, id: TransactionId) -> Option<LedgerEntry<CanonicalClientId>> {
        if id == self.tx_id {
            self.entry
        } else {
            None
        }
    }
}

fn state_from_str(state: &str) -> Result<EntryState, RepoError> {
    match state {
        "settled" => Ok(EntryState::Settled),
        "disputed" => Ok(EntryState::Disputed),
        "charged_back" => Ok(EntryState::ChargedBack),
        other => Err(RepoError::InvalidState(other.to_string())),
    }
}

fn state_str(state: EntryState) -> &'static str {
    match state {
        EntryState::Settled => "settled",
        EntryState::Disputed => "disputed",
        EntryState::ChargedBack => "charged_back",
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
        let external = i32::from(external_client);
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

        let (client_id,) = sqlx::query_as::<_, (i64,)>(
            "UPDATE id_allocator SET next_id = next_id + 1 \
             WHERE name = 'client' RETURNING next_id - 1",
        )
        .fetch_one(&mut *db)
        .await?;

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

    async fn apply_event(&self, event: &LedgerEvent) -> Result<ApplyOutcome, RepoError> {
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
                        state: state_from_str(row.try_get("state")?)?,
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
                            .bind(state_str(entry.state))
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

    async fn record_emitted(
        &self,
        job_id: &str,
        partition: i32,
        count: i64,
    ) -> Result<(), RepoError> {
        sqlx::query(
            "INSERT INTO job_progress (job_id, partition_id, emitted, processed) \
             VALUES ($1, $2, $3, 0) \
             ON CONFLICT (job_id, partition_id) DO UPDATE SET \
             emitted = job_progress.emitted + EXCLUDED.emitted",
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
                state: state_from_str(row.try_get("state")?)?,
            })
        })
        .transpose()
    }
}

impl PostgresRepository {
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
