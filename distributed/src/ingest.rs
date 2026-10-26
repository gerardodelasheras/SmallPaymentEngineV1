//! Bounded parallel CSV ingestion and staging records.
//!
//! Every valid row becomes a [`StagingRecord`] with a content-addressed event
//! ID. Files are parsed in parallel up to a configured bound. A malformed row is
//! counted and skipped without stopping the rest of the file.

use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::{mpsc, Semaphore};
use tokio::task::JoinSet;
use transaction_processor::transaction::parser::transactions;
use transaction_processor::{ProcessingError, TransactionType};

use crate::manifest::{ValidatedFile, ValidatedManifest};

const STAGING_CHANNEL_CAPACITY: usize = 256;
const COMPLETION_BATCH: usize = 100;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StagingKind {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

impl From<TransactionType> for StagingKind {
    fn from(value: TransactionType) -> Self {
        match value {
            TransactionType::Deposit => StagingKind::Deposit,
            TransactionType::Withdrawal => StagingKind::Withdrawal,
            TransactionType::Dispute => StagingKind::Dispute,
            TransactionType::Resolve => StagingKind::Resolve,
            TransactionType::Chargeback => StagingKind::Chargeback,
        }
    }
}

impl From<StagingKind> for TransactionType {
    fn from(value: StagingKind) -> Self {
        match value {
            StagingKind::Deposit => TransactionType::Deposit,
            StagingKind::Withdrawal => TransactionType::Withdrawal,
            StagingKind::Dispute => TransactionType::Dispute,
            StagingKind::Resolve => TransactionType::Resolve,
            StagingKind::Chargeback => TransactionType::Chargeback,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StagingRecord {
    pub job_id: String,
    pub source_id: String,
    pub external_client: u16,
    pub file_hash: String,
    pub file_ord: u32,
    pub row_ord: u32,
    pub event_id: String,
    pub kind: StagingKind,
    pub transaction_id: u32,
    pub amount_scaled: Option<i64>,
}

impl StagingRecord {
    pub fn staging_key(&self) -> String {
        staging_key(&self.source_id, self.external_client)
    }
}

/// Kafka key for the staging topic: all rows for one source-scoped client land
/// on the same partition and therefore the same partitioner owner.
pub fn staging_key(source_id: &str, external_client: u16) -> String {
    format!("{source_id}:{external_client}")
}

pub fn event_id(source_id: &str, file_hash: &str, row_ord: u32) -> String {
    let mut hasher = Sha256::new();
    hasher.update(source_id.as_bytes());
    hasher.update(b"\n");
    hasher.update(file_hash.as_bytes());
    hasher.update(b"\n");
    hasher.update(row_ord.to_string().as_bytes());
    hex::encode(hasher.finalize())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileCompletion {
    pub source_id: String,
    pub file_ord: u32,
    pub row_count: u64,
}

#[async_trait]
pub trait StagingSink: Send + Sync {
    async fn stage(&self, record: StagingRecord) -> Result<(), IngestError>;

    async fn complete_files(
        &self,
        job_id: &str,
        files: &[FileCompletion],
    ) -> Result<(), IngestError>;
}

#[derive(Debug, Clone, Copy)]
pub struct IngestConfig {
    pub max_concurrent_files: usize,
}

impl Default for IngestConfig {
    fn default() -> Self {
        Self {
            max_concurrent_files: 8,
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct IngestSummary {
    pub files_processed: u64,
    pub rows_staged: u64,
    pub rows_rejected: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum IngestError {
    #[error("manifest error: {0}")]
    Manifest(#[from] crate::manifest::ManifestError),

    #[error("file ordinal {file_ord} is not readable at {}: {source}", path.display())]
    FileOpen {
        file_ord: u32,
        path: PathBuf,
        source: std::io::Error,
    },

    #[error("file ordinal {file_ord} has too many rows")]
    TooManyRows { file_ord: u32 },

    #[error("row parser failed: {0}")]
    Parser(#[from] ProcessingError),

    #[error("staging sink failed: {0}")]
    Staging(String),

    #[error("ingest task failed: {0}")]
    Join(String),

    #[error("ingest configuration is invalid: {0}")]
    Config(String),
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct ParseSummary {
    rows_rejected: u64,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct FileSummary {
    rows_staged: u64,
    rows_rejected: u64,
}

#[derive(Debug)]
struct FileOutcome {
    summary: FileSummary,
    completion: FileCompletion,
}

/// Parse every manifest file in parallel and stage each valid row.
pub async fn ingest<W>(
    manifest: &ValidatedManifest,
    job_id: &str,
    sink: Arc<W>,
    config: IngestConfig,
) -> Result<IngestSummary, IngestError>
where
    W: StagingSink + 'static,
{
    if config.max_concurrent_files == 0 {
        return Err(IngestError::Config(
            "max_concurrent_files must be at least 1".to_string(),
        ));
    }

    let semaphore = Arc::new(Semaphore::new(config.max_concurrent_files));
    let mut tasks = JoinSet::new();
    let mut summary = IngestSummary::default();

    for file in manifest.files.clone() {
        let permit = Arc::clone(&semaphore)
            .acquire_owned()
            .await
            .map_err(|error| IngestError::Join(error.to_string()))?;
        let sink = Arc::clone(&sink);
        let job_id = job_id.to_string();
        tasks.spawn(async move {
            let _permit = permit;
            process_file(file, job_id, sink).await
        });
    }

    let mut completions = Vec::new();
    while let Some(result) = tasks.join_next().await {
        let outcome = result.map_err(|error| IngestError::Join(error.to_string()))??;
        summary.files_processed += 1;
        summary.rows_staged += outcome.summary.rows_staged;
        summary.rows_rejected += outcome.summary.rows_rejected;
        completions.push(outcome.completion);
        if completions.len() >= COMPLETION_BATCH {
            sink.complete_files(job_id, &completions).await?;
            completions.clear();
        }
    }
    if !completions.is_empty() {
        sink.complete_files(job_id, &completions).await?;
    }

    Ok(summary)
}

async fn process_file<W>(
    file: ValidatedFile,
    job_id: String,
    sink: Arc<W>,
) -> Result<FileOutcome, IngestError>
where
    W: StagingSink + 'static,
{
    let (sender, mut receiver) =
        mpsc::channel::<Result<StagingRecord, IngestError>>(STAGING_CHANNEL_CAPACITY);
    let parse_file = file.clone();
    let parse_job = job_id.clone();
    let parser = tokio::task::spawn_blocking(move || parse_file_rows(parse_file, &parse_job, sender));

    let mut rows_staged = 0;
    while let Some(item) = receiver.recv().await {
        match item {
            Ok(record) => {
                sink.stage(record).await?;
                rows_staged += 1;
            }
            Err(error) => return Err(error),
        }
    }

    let parse = parser
        .await
        .map_err(|error| IngestError::Join(error.to_string()))??;

    // Every staged record has been acknowledged. Completion is reported to the
    // driver so it can batch the durable watermark writes.
    Ok(FileOutcome {
        summary: FileSummary {
            rows_staged,
            rows_rejected: parse.rows_rejected,
        },
        completion: FileCompletion {
            source_id: file.source_id,
            file_ord: file.ordinal,
            row_count: rows_staged,
        },
    })
}

fn parse_file_rows(
    file: ValidatedFile,
    job_id: &str,
    sender: mpsc::Sender<Result<StagingRecord, IngestError>>,
) -> Result<ParseSummary, IngestError> {
    let handle = File::open(&file.path).map_err(|source| IngestError::FileOpen {
        file_ord: file.ordinal,
        path: file.path.clone(),
        source,
    })?;
    let rows = transactions(handle)?;

    let mut rows_rejected = 0;
    for (index, row) in rows.enumerate() {
        match row {
            Ok(transaction) => {
                let row_ord = u32::try_from(index)
                    .map_err(|_| IngestError::TooManyRows { file_ord: file.ordinal })?;
                let record = StagingRecord {
                    job_id: job_id.to_string(),
                    source_id: file.source_id.clone(),
                    external_client: transaction.client,
                    file_hash: file.sha256.clone(),
                    file_ord: file.ordinal,
                    row_ord,
                    event_id: event_id(&file.source_id, &file.sha256, row_ord),
                    kind: StagingKind::from(transaction.transaction_type),
                    transaction_id: transaction.transaction_id,
                    amount_scaled: transaction.amount.map(|amount| amount.scaled()),
                };
                if sender.blocking_send(Ok(record)).is_err() {
                    return Err(IngestError::Staging(
                        "staging consumer stopped".to_string(),
                    ));
                }
            }
            Err(error) if error.is_recoverable() => {
                rows_rejected += 1;
            }
            Err(error) => {
                let _ = sender.blocking_send(Err(IngestError::Parser(error)));
                return Ok(ParseSummary { rows_rejected });
            }
        }
    }

    Ok(ParseSummary { rows_rejected })
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;

    use super::*;
    use crate::manifest::{validate, ManifestFileInput, ManifestInput};
    use transaction_processor::Amount;

    #[derive(Default)]
    struct MemorySink {
        records: tokio::sync::Mutex<Vec<StagingRecord>>,
        completions: tokio::sync::Mutex<Vec<(String, String, u32, u64)>>,
    }

    #[async_trait]
    impl StagingSink for MemorySink {
        async fn stage(&self, record: StagingRecord) -> Result<(), IngestError> {
            self.records.lock().await.push(record);
            Ok(())
        }

        async fn complete_files(
            &self,
            job_id: &str,
            files: &[FileCompletion],
        ) -> Result<(), IngestError> {
            let mut completions = self.completions.lock().await;
            for file in files {
                completions.push((
                    job_id.to_string(),
                    file.source_id.clone(),
                    file.file_ord,
                    file.row_count,
                ));
            }
            Ok(())
        }
    }

    impl MemorySink {
        async fn records(&self) -> Vec<StagingRecord> {
            self.records.lock().await.clone()
        }

        async fn completions(&self) -> Vec<(String, String, u32, u64)> {
            self.completions.lock().await.clone()
        }
    }

    fn hash(content: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(content.as_bytes());
        hex::encode(hasher.finalize())
    }

    fn write_file(dir: &Path, name: &str, content: &str) -> (PathBuf, String) {
        let path = dir.join(name);
        fs::write(&path, content).expect("write fixture");
        (path, hash(content))
    }

    fn manifest(files: Vec<(u32, PathBuf, String)>) -> ValidatedManifest {
        validate(ManifestInput {
            source_id: "alpha".to_string(),
            files: files
                .into_iter()
                .map(|(ordinal, path, sha256)| ManifestFileInput {
                    source_id: None,
                    ordinal,
                    path: path.to_string_lossy().into_owned(),
                    sha256,
                })
                .collect(),
        })
        .expect("valid manifest")
    }

    #[tokio::test]
    async fn malformed_row_does_not_stop_the_file() {
        let dir = tempfile::tempdir().expect("temp dir");
        let (path, sha256) = write_file(
            dir.path(),
            "rows.csv",
            "type,client,tx,amount\n\
             deposit,1,1,1.0\n\
             bogus,1,2,1.0\n\
             deposit,1,3,2.0\n",
        );
        let manifest = manifest(vec![(1, path, sha256)]);
        let sink = Arc::new(MemorySink::default());

        let summary = ingest(&manifest, "job-1", Arc::clone(&sink), IngestConfig::default())
            .await
            .expect("ingest");

        assert_eq!(summary.rows_staged, 2);
        assert_eq!(summary.rows_rejected, 1);
        assert_eq!(sink.records().await.len(), 2);
        assert_eq!(sink.completions().await.len(), 1);
        assert_eq!(sink.completions().await[0].3, 2);
    }

    #[tokio::test]
    async fn event_ids_are_content_addressed_across_jobs() {
        let dir = tempfile::tempdir().expect("temp dir");
        let (path, sha256) = write_file(
            dir.path(),
            "rows.csv",
            "type,client,tx,amount\ndeposit,1,1,1.0\n",
        );
        let manifest = manifest(vec![(1, path, sha256)]);
        let sink = Arc::new(MemorySink::default());

        ingest(&manifest, "job-1", Arc::clone(&sink), IngestConfig::default())
            .await
            .expect("first ingest");
        ingest(&manifest, "job-2", Arc::clone(&sink), IngestConfig::default())
            .await
            .expect("second ingest");

        let records = sink.records().await;
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].event_id, records[1].event_id);
        assert_ne!(records[0].job_id, records[1].job_id);
    }

    #[tokio::test]
    async fn rows_keep_their_file_order() {
        let dir = tempfile::tempdir().expect("temp dir");
        let (path, sha256) = write_file(
            dir.path(),
            "rows.csv",
            "type,client,tx,amount\n\
             deposit,1,10,1.0\n\
             withdrawal,1,11,1.0\n\
             deposit,1,12,2.0\n",
        );
        let manifest = manifest(vec![(1, path, sha256)]);
        let sink = Arc::new(MemorySink::default());

        ingest(&manifest, "job-1", Arc::clone(&sink), IngestConfig::default())
            .await
            .expect("ingest");

        let records = sink.records().await;
        assert_eq!(
            records.iter().map(|record| record.row_ord).collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        assert_eq!(records[0].amount_scaled, Some(Amount::parse("1.0").unwrap().scaled()));
    }

    #[tokio::test]
    async fn all_manifest_files_are_staged() {
        let dir = tempfile::tempdir().expect("temp dir");
        let (first, first_hash) = write_file(
            dir.path(),
            "first.csv",
            "type,client,tx,amount\ndeposit,1,1,1.0\n",
        );
        let (second, second_hash) = write_file(
            dir.path(),
            "second.csv",
            "type,client,tx,amount\ndeposit,2,2,2.0\n",
        );
        let manifest = manifest(vec![(1, first, first_hash), (2, second, second_hash)]);
        let sink = Arc::new(MemorySink::default());

        let summary = ingest(
            &manifest,
            "job-1",
            Arc::clone(&sink),
            IngestConfig {
                max_concurrent_files: 2,
            },
        )
        .await
        .expect("ingest");

        assert_eq!(summary.files_processed, 2);
        assert_eq!(summary.rows_staged, 2);
        let mut ordinals: Vec<u32> = sink
            .records()
            .await
            .iter()
            .map(|record| record.file_ord)
            .collect();
        ordinals.sort_unstable();
        assert_eq!(ordinals, vec![1, 2]);
    }

    #[tokio::test]
    async fn missing_file_fails_ingestion() {
        let dir = tempfile::tempdir().expect("temp dir");
        let (path, sha256) = write_file(
            dir.path(),
            "rows.csv",
            "type,client,tx,amount\ndeposit,1,1,1.0\n",
        );
        let manifest = manifest(vec![(1, path.clone(), sha256)]);
        fs::remove_file(&path).expect("remove file");
        let sink = Arc::new(MemorySink::default());

        let result = ingest(&manifest, "job-1", sink, IngestConfig::default()).await;

        assert!(matches!(result, Err(IngestError::FileOpen { file_ord: 1, .. })));
    }

    #[test]
    fn staging_key_is_source_scoped() {
        assert_ne!(staging_key("alpha", 7), staging_key("beta", 7));
        assert_eq!(staging_key("alpha", 7), "alpha:7");
    }
}
