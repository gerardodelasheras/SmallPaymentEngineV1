//! Partitioner: consumes staging records, maps external keys to canonical IDs,
//! orders rows per client, and emits to the transactions topic.
//!
//! Ordering rule: a row from file `F` is emitted only when:
//! - `F` is complete (all of its rows are staged), and
//! - every earlier file for the same source has been fully emitted
//!   (`emitted_rows == row_count` in the durable `job_emitted` ledger).
//!
//! Because rows for one client share a staging partition and are produced in
//! row order within a file, the per-client buffer only needs to sort by
//! `(file_ord, row_ord)`.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::ingest::{StagingKind, StagingRecord};
use crate::repository::mariadb::MariaDbRepository;
use crate::repository::postgres::PostgresRepository;
use crate::repository::{EmittedRow, JobFileRow, LedgerRepository, RepoError};
use transaction_processor::CanonicalClientId;

const DEFAULT_REFRESH_INTERVAL: Duration = Duration::from_millis(250);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransactionRecord {
    pub event_id: String,
    pub job_id: String,
    pub source_id: String,
    pub file_hash: String,
    pub file_ord: u32,
    pub row_ord: u32,
    pub client_id: CanonicalClientId,
    pub kind: StagingKind,
    pub transaction_id: u32,
    pub amount_scaled: Option<i64>,
}

impl TransactionRecord {
    pub fn from_staging(record: &StagingRecord, client_id: CanonicalClientId) -> Self {
        Self {
            event_id: record.event_id.clone(),
            job_id: record.job_id.clone(),
            source_id: record.source_id.clone(),
            file_hash: record.file_hash.clone(),
            file_ord: record.file_ord,
            row_ord: record.row_ord,
            client_id,
            kind: record.kind,
            transaction_id: record.transaction_id,
            amount_scaled: record.amount_scaled,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StagingPosition {
    pub partition: i32,
    pub offset: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmittedEvent {
    pub job_id: String,
    pub partition: i32,
    pub event_id: String,
    pub file_ord: u32,
    pub row_ord: u32,
    pub client_id: CanonicalClientId,
    pub staging: StagingPosition,
}

#[async_trait]
pub trait TransactionSink: Send + Sync {
    async fn emit(&self, record: TransactionRecord) -> Result<i32, PartitionError>;
}

#[async_trait]
pub trait PartitionRepository: Send + Sync {
    async fn mint_or_get_client(
        &self,
        source_id: &str,
        external_client: u16,
    ) -> Result<CanonicalClientId, RepoError>;

    async fn job_files(&self, job_id: &str) -> Result<Vec<JobFileRow>, RepoError>;

    async fn record_emitted(
        &self,
        job_id: &str,
        partition: i32,
        rows: &[EmittedRow],
    ) -> Result<u64, RepoError>;
}

#[async_trait]
impl PartitionRepository for PostgresRepository {
    async fn mint_or_get_client(
        &self,
        source_id: &str,
        external_client: u16,
    ) -> Result<CanonicalClientId, RepoError> {
        LedgerRepository::mint_or_get_client(self, source_id, external_client).await
    }

    async fn job_files(&self, job_id: &str) -> Result<Vec<JobFileRow>, RepoError> {
        LedgerRepository::job_files(self, job_id).await
    }

    async fn record_emitted(
        &self,
        job_id: &str,
        partition: i32,
        rows: &[EmittedRow],
    ) -> Result<u64, RepoError> {
        LedgerRepository::record_emitted(self, job_id, partition, rows).await
    }
}

#[async_trait]
impl PartitionRepository for MariaDbRepository {
    async fn mint_or_get_client(
        &self,
        source_id: &str,
        external_client: u16,
    ) -> Result<CanonicalClientId, RepoError> {
        LedgerRepository::mint_or_get_client(self, source_id, external_client).await
    }

    async fn job_files(&self, job_id: &str) -> Result<Vec<JobFileRow>, RepoError> {
        LedgerRepository::job_files(self, job_id).await
    }

    async fn record_emitted(
        &self,
        job_id: &str,
        partition: i32,
        rows: &[EmittedRow],
    ) -> Result<u64, RepoError> {
        LedgerRepository::record_emitted(self, job_id, partition, rows).await
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PartitionError {
    #[error("repository error: {0}")]
    Repo(#[from] RepoError),

    #[error("transaction sink error: {0}")]
    Sink(String),

    #[error("job file {file_ord} for job {job_id} failed")]
    JobFileFailed { job_id: String, file_ord: u32 },

    #[error("partition buffer limit exceeded: {0}")]
    BufferFull(usize),

    #[error("job {0} has no registered files")]
    NoFiles(String),
}

#[derive(Debug, Clone)]
struct BufferedRow {
    record: TransactionRecord,
    staging: StagingPosition,
}

#[derive(Debug, Default)]
struct ClientBuffer {
    rows: BTreeMap<(u32, u32), BufferedRow>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Readiness {
    Ready,
    Waiting,
    Failed(u32),
}

#[derive(Debug, Clone)]
struct JobWatermark {
    files: Vec<JobFileRow>,
    refreshed_at: Instant,
}

impl JobWatermark {
    fn readiness(&self, source_id: &str, file_ord: u32) -> Readiness {
        let Some(target) = self
            .files
            .iter()
            .find(|file| file.source_id == source_id && file.file_ord == file_ord)
        else {
            return Readiness::Waiting;
        };

        if target.state == "failed" {
            return Readiness::Failed(target.file_ord);
        }
        if target.state != "complete" {
            return Readiness::Waiting;
        }

        for earlier in self
            .files
            .iter()
            .filter(|file| file.source_id == source_id && file.file_ord < file_ord)
        {
            if earlier.state == "failed" {
                return Readiness::Failed(earlier.file_ord);
            }
            if earlier.emitted_rows < earlier.row_count {
                return Readiness::Waiting;
            }
        }

        Readiness::Ready
    }
}

pub struct Partitioner<S: TransactionSink> {
    sink: Arc<S>,
    repository: Arc<dyn PartitionRepository>,
    buffers: HashMap<(String, CanonicalClientId), ClientBuffer>,
    watermarks: HashMap<String, JobWatermark>,
    max_buffered_rows: usize,
    buffered_rows: usize,
    refresh_interval: Duration,
}

impl<S: TransactionSink> Partitioner<S> {
    pub fn new(
        sink: Arc<S>,
        repository: Arc<dyn PartitionRepository>,
        max_buffered_rows: usize,
    ) -> Self {
        Self {
            sink,
            repository,
            buffers: HashMap::new(),
            watermarks: HashMap::new(),
            max_buffered_rows,
            buffered_rows: 0,
            refresh_interval: DEFAULT_REFRESH_INTERVAL,
        }
    }

    pub fn buffered_rows(&self) -> usize {
        self.buffered_rows
    }

    pub fn has_buffered_for(&self, staging_partition: i32) -> bool {
        self.buffers.values().any(|buffer| {
            buffer
                .rows
                .values()
                .any(|row| row.staging.partition == staging_partition)
        })
    }

    pub async fn push(
        &mut self,
        record: &StagingRecord,
        staging: StagingPosition,
    ) -> Result<Vec<EmittedEvent>, PartitionError> {
        let client_id = self
            .repository
            .mint_or_get_client(&record.source_id, record.external_client)
            .await?;
        let transaction = TransactionRecord::from_staging(record, client_id);
        let key = (record.job_id.clone(), client_id);
        let row_key = (record.file_ord, record.row_ord);

        if let Some(buffer) = self.buffers.get(&key) {
            if buffer.rows.contains_key(&row_key) {
                return self.drain_client(&key).await;
            }
        }

        if self.buffered_rows >= self.max_buffered_rows {
            return Err(PartitionError::BufferFull(self.max_buffered_rows));
        }

        self.buffers
            .entry(key.clone())
            .or_default()
            .rows
            .insert(
                row_key,
                BufferedRow {
                    record: transaction,
                    staging,
                },
            );
        self.buffered_rows += 1;

        self.refresh_watermark(&key.0, false).await?;
        self.drain_client(&key).await
    }

    pub async fn tick(&mut self) -> Result<Vec<EmittedEvent>, PartitionError> {
        let jobs: HashSet<String> = self
            .buffers
            .keys()
            .map(|(job_id, _)| job_id.clone())
            .collect();
        for job_id in jobs {
            self.refresh_watermark(&job_id, true).await?;
        }

        let keys: Vec<(String, CanonicalClientId)> =
            self.buffers.keys().cloned().collect();
        let mut emitted = Vec::new();
        for key in keys {
            emitted.extend(self.drain_client(&key).await?);
        }
        Ok(emitted)
    }

    async fn drain_client(
        &mut self,
        key: &(String, CanonicalClientId),
    ) -> Result<Vec<EmittedEvent>, PartitionError> {
        let job_id = key.0.clone();
        let mut emitted = Vec::new();

        loop {
            let next = {
                let Some(buffer) = self.buffers.get(key) else {
                    break;
                };
                let Some((row_key, row)) = buffer.rows.first_key_value() else {
                    break;
                };
                (*row_key, row.clone())
            };

            let Some(watermark) = self.watermarks.get(&job_id) else {
                break;
            };

            match watermark.readiness(&next.1.record.source_id, next.1.record.file_ord) {
                Readiness::Ready => {}
                Readiness::Waiting => break,
                Readiness::Failed(file_ord) => {
                    return Err(PartitionError::JobFileFailed { job_id, file_ord });
                }
            }

            self.buffers
                .get_mut(key)
                .expect("buffer exists")
                .rows
                .remove(&next.0);
            self.buffered_rows -= 1;

            let partition = self.sink.emit(next.1.record.clone()).await?;
            let inserted = self
                .repository
                .record_emitted(
                    &job_id,
                    partition,
                    &[EmittedRow {
                        event_id: next.1.record.event_id.clone(),
                        file_ord: next.1.record.file_ord,
                    }],
                )
                .await?;
            if inserted > 0 {
                if let Some(watermark) = self.watermarks.get_mut(&job_id) {
                    if let Some(file) = watermark
                        .files
                        .iter_mut()
                        .find(|file| file.file_ord == next.1.record.file_ord)
                    {
                        file.emitted_rows += 1;
                    }
                }
            }
            emitted.push(EmittedEvent {
                job_id: job_id.clone(),
                partition,
                event_id: next.1.record.event_id.clone(),
                file_ord: next.1.record.file_ord,
                row_ord: next.1.record.row_ord,
                client_id: next.1.record.client_id,
                staging: next.1.staging,
            });
        }

        if !emitted.is_empty() {
            // Pick up emitted counts written by other partitioner replicas.
            self.refresh_watermark(&job_id, true).await?;
        }

        if self
            .buffers
            .get(key)
            .map(|buffer| buffer.rows.is_empty())
            .unwrap_or(false)
        {
            self.buffers.remove(key);
        }

        Ok(emitted)
    }

    async fn refresh_watermark(
        &mut self,
        job_id: &str,
        force: bool,
    ) -> Result<(), PartitionError> {
        let stale = self
            .watermarks
            .get(job_id)
            .map(|watermark| {
                force || watermark.refreshed_at.elapsed() >= self.refresh_interval
            })
            .unwrap_or(true);
        if !stale {
            return Ok(());
        }

        let files = self.repository.job_files(job_id).await?;
        if files.is_empty() {
            return Err(PartitionError::NoFiles(job_id.to_string()));
        }
        self.watermarks.insert(
            job_id.to_string(),
            JobWatermark {
                files,
                refreshed_at: Instant::now(),
            },
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    #[derive(Default)]
    struct TestRepository {
        clients: Mutex<HashMap<(String, u16), CanonicalClientId>>,
        files: Mutex<HashMap<String, Vec<JobFileRow>>>,
        emitted: Mutex<HashMap<(String, String), EmittedRow>>,
    }

    impl TestRepository {
        fn set_files(&self, job_id: &str, files: Vec<JobFileRow>) {
            self.files
                .lock()
                .expect("files")
                .insert(job_id.to_string(), files);
        }

        fn emitted_count(&self) -> usize {
            self.emitted.lock().expect("emitted").len()
        }
    }

    #[async_trait]
    impl PartitionRepository for TestRepository {
        async fn mint_or_get_client(
            &self,
            source_id: &str,
            external_client: u16,
        ) -> Result<CanonicalClientId, RepoError> {
            let mut clients = self.clients.lock().expect("clients");
            let next = clients.len() as i64 + 1;
            Ok(*clients
                .entry((source_id.to_string(), external_client))
                .or_insert(next))
        }

        async fn job_files(&self, job_id: &str) -> Result<Vec<JobFileRow>, RepoError> {
            Ok(self
                .files
                .lock()
                .expect("files")
                .get(job_id)
                .cloned()
                .unwrap_or_default())
        }

        async fn record_emitted(
            &self,
            job_id: &str,
            _partition: i32,
            rows: &[EmittedRow],
        ) -> Result<u64, RepoError> {
            let mut emitted = self.emitted.lock().expect("emitted");
            let mut inserted = 0;
            for row in rows {
                let key = (job_id.to_string(), row.event_id.clone());
                if emitted.insert(key, row.clone()).is_none() {
                    inserted += 1;
                    let mut files = self.files.lock().expect("files");
                    if let Some(files) = files.get_mut(job_id) {
                        if let Some(file) = files
                            .iter_mut()
                            .find(|file| file.file_ord == row.file_ord)
                        {
                            file.emitted_rows += 1;
                        }
                    }
                }
            }
            Ok(inserted)
        }
    }

    #[derive(Default)]
    struct TestSink {
        records: tokio::sync::Mutex<Vec<TransactionRecord>>,
        partition: i32,
    }

    #[async_trait]
    impl TransactionSink for TestSink {
        async fn emit(&self, record: TransactionRecord) -> Result<i32, PartitionError> {
            self.records.lock().await.push(record);
            Ok(self.partition)
        }
    }

    fn file(file_ord: u32, state: &str, row_count: u64, emitted_rows: u64) -> JobFileRow {
        JobFileRow {
            file_ord,
            source_id: "alpha".to_string(),
            state: state.to_string(),
            row_count,
            emitted_rows,
        }
    }

    fn staging(
        job_id: &str,
        client: u16,
        file_ord: u32,
        row_ord: u32,
        offset: i64,
    ) -> (StagingRecord, StagingPosition) {
        (
            StagingRecord {
                job_id: job_id.to_string(),
                source_id: "alpha".to_string(),
                external_client: client,
                file_hash: "f".repeat(64),
                file_ord,
                row_ord,
                event_id: format!("{job_id}-{file_ord}-{row_ord}"),
                kind: StagingKind::Deposit,
                transaction_id: row_ord + 1,
                amount_scaled: Some(10_000),
            },
            StagingPosition {
                partition: 0,
                offset,
            },
        )
    }

    async fn partitioner(
        repository: Arc<TestRepository>,
        sink: Arc<TestSink>,
        max_buffered_rows: usize,
    ) -> Partitioner<TestSink> {
        Partitioner::new(sink, repository, max_buffered_rows)
    }

    #[tokio::test]
    async fn emits_a_later_file_only_after_the_earlier_file_is_emitted() {
        let repository = Arc::new(TestRepository::default());
        repository.set_files("job", vec![file(1, "complete", 1, 0), file(2, "complete", 1, 0)]);
        let sink = Arc::new(TestSink::default());
        let mut partitioner = partitioner(Arc::clone(&repository), Arc::clone(&sink), 100).await;

        let (later, later_position) = staging("job", 7, 2, 0, 10);
        let emitted = partitioner
            .push(&later, later_position)
            .await
            .expect("push later");
        assert!(emitted.is_empty());

        let (earlier, earlier_position) = staging("job", 7, 1, 0, 9);
        let emitted = partitioner
            .push(&earlier, earlier_position)
            .await
            .expect("push earlier");

        let order: Vec<u32> = emitted.iter().map(|event| event.file_ord).collect();
        assert_eq!(order, vec![1, 2]);
        let records = sink.records.lock().await;
        let order: Vec<u32> = records.iter().map(|record| record.file_ord).collect();
        assert_eq!(order, vec![1, 2]);
        assert_eq!(repository.emitted_count(), 2);
    }

    #[tokio::test]
    async fn waits_until_the_earlier_file_is_complete() {
        let repository = Arc::new(TestRepository::default());
        repository.set_files("job", vec![file(1, "pending", 1, 0), file(2, "complete", 1, 0)]);
        let sink = Arc::new(TestSink::default());
        let mut partitioner = partitioner(Arc::clone(&repository), Arc::clone(&sink), 100).await;

        let (later, later_position) = staging("job", 7, 2, 0, 10);
        partitioner.push(&later, later_position).await.expect("push later");
        assert!(sink.records.lock().await.is_empty());

        repository.set_files("job", vec![file(1, "complete", 1, 0), file(2, "complete", 1, 0)]);
        let (earlier, earlier_position) = staging("job", 7, 1, 0, 9);
        partitioner
            .push(&earlier, earlier_position)
            .await
            .expect("push earlier");

        let emitted = partitioner.tick().await.expect("tick");
        assert!(!emitted.is_empty());
        let records = sink.records.lock().await;
        let order: Vec<u32> = records.iter().map(|record| record.file_ord).collect();
        assert_eq!(order, vec![1, 2]);
    }

    #[tokio::test]
    async fn failed_file_stops_emission() {
        let repository = Arc::new(TestRepository::default());
        repository.set_files("job", vec![file(1, "failed", 1, 0), file(2, "complete", 1, 0)]);
        let sink = Arc::new(TestSink::default());
        let mut partitioner = partitioner(repository, sink, 100).await;

        let (later, position) = staging("job", 7, 2, 0, 10);
        let result = partitioner.push(&later, position).await;

        assert!(matches!(
            result,
            Err(PartitionError::JobFileFailed { file_ord: 1, .. })
        ));
    }

    #[tokio::test]
    async fn rows_are_emitted_in_row_order() {
        let repository = Arc::new(TestRepository::default());
        repository.set_files("job", vec![file(1, "complete", 3, 0)]);
        let sink = Arc::new(TestSink::default());
        let mut partitioner = partitioner(repository, Arc::clone(&sink), 100).await;

        for (row_ord, offset) in [(0, 10), (1, 11), (2, 12)] {
            let (record, position) = staging("job", 7, 1, row_ord, offset);
            partitioner.push(&record, position).await.expect("push");
        }

        let records = sink.records.lock().await;
        let rows: Vec<u32> = records.iter().map(|record| record.row_ord).collect();
        assert_eq!(rows, vec![0, 1, 2]);
    }

    #[tokio::test]
    async fn external_clients_map_to_distinct_canonical_ids() {
        let repository = Arc::new(TestRepository::default());
        repository.set_files("job", vec![file(1, "complete", 2, 0)]);
        let sink = Arc::new(TestSink::default());
        let mut partitioner = partitioner(repository, Arc::clone(&sink), 100).await;

        for (client, offset) in [(7, 10), (8, 11)] {
            let (record, position) = staging("job", client, 1, 0, offset);
            partitioner.push(&record, position).await.expect("push");
        }

        let records = sink.records.lock().await;
        assert_ne!(records[0].client_id, records[1].client_id);
    }

    #[tokio::test]
    async fn buffer_limit_is_enforced() {
        let repository = Arc::new(TestRepository::default());
        repository.set_files("job", vec![file(1, "pending", 2, 0)]);
        let sink = Arc::new(TestSink::default());
        let mut partitioner = partitioner(repository, sink, 1).await;

        let (first, first_position) = staging("job", 7, 1, 0, 10);
        partitioner.push(&first, first_position).await.expect("first");
        let (second, second_position) = staging("job", 7, 1, 1, 11);
        let result = partitioner.push(&second, second_position).await;

        assert!(matches!(result, Err(PartitionError::BufferFull(1))));
    }
}
