//! Kafka-backed staging sink.
//!
//! Records are keyed by `source_id:external_client`, so every row for one
//! source-scoped client lands on one staging partition and is owned by one
//! partitioner. Event IDs are content-addressed; Kafka may redeliver a record,
//! and the durable ledger dedupes it.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{DeliveryFuture, FutureProducer, FutureRecord};
use tokio::sync::Mutex;

use crate::ingest::{FileCompletion, IngestError, StagingRecord, StagingSink};
use crate::repository::{FileCompletionRow, LedgerRepository};

const CLIENT_ID: &str = "distributed-ingest";

pub struct KafkaStagingSink {
    producer: FutureProducer,
    topic: String,
    pending: Mutex<Vec<DeliveryFuture>>,
    stage_limit: Option<usize>,
    staged: AtomicUsize,
    repository: Arc<dyn LedgerRepository>,
}

impl KafkaStagingSink {
    pub async fn connect(
        bootstrap: &str,
        topic: &str,
        repository: Arc<dyn LedgerRepository>,
    ) -> Result<Self, IngestError> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap)
            .set("client.id", CLIENT_ID)
            .set("message.timeout.ms", "30000")
            // Idempotent production: acks=all plus the idempotence flag.
            .set("enable.idempotence", "true")
            .set("acks", "all")
            .set("partitioner", "murmur2_random")
            .create()
            .map_err(|error| {
                IngestError::Staging(format!("connect to Kafka: {error}"))
            })?;

        Ok(Self {
            producer,
            topic: topic.to_string(),
            pending: Mutex::new(Vec::new()),
            stage_limit: None,
            staged: AtomicUsize::new(0),
            repository,
        })
    }

    /// Test hook: stop after `limit` staged rows, flush them, and fail as if the
    /// ingest process crashed. The file remains incomplete.
    pub fn with_stage_limit(mut self, limit: usize) -> Self {
        self.stage_limit = Some(limit);
        self
    }

    async fn flush_pending(&self) -> Result<(), IngestError> {
        let pending = {
            let mut pending = self.pending.lock().await;
            std::mem::take(&mut *pending)
        };
        for future in pending {
            match future.await {
                Ok(Ok(_)) => {}
                Ok(Err((error, _message))) => {
                    return Err(IngestError::Staging(format!(
                        "deliver staging record: {error}"
                    )));
                }
                Err(_) => {
                    return Err(IngestError::Staging(
                        "staging delivery future was cancelled".to_string(),
                    ));
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl StagingSink for KafkaStagingSink {
    async fn stage(&self, record: StagingRecord) -> Result<(), IngestError> {
        if let Some(limit) = self.stage_limit {
            if self.staged.load(Ordering::SeqCst) >= limit {
                self.flush_pending().await?;
                return Err(IngestError::Staging(format!(
                    "simulated ingest crash after {limit} rows"
                )));
            }
            self.staged.fetch_add(1, Ordering::SeqCst);
        }

        let key = record.staging_key();
        let value = serde_json::to_vec(&record).map_err(|error| {
            IngestError::Staging(format!("serialize staging record: {error}"))
        })?;

        let future = self
            .producer
            .send_result(
                FutureRecord::to(&self.topic)
                    .key(key.as_bytes())
                    .payload(value.as_slice()),
            )
            .map_err(|(error, _message)| {
                IngestError::Staging(format!("queue staging record: {error}"))
            })?;
        self.pending.lock().await.push(future);
        Ok(())
    }

    async fn complete_files(
        &self,
        job_id: &str,
        files: &[FileCompletion],
    ) -> Result<(), IngestError> {
        // Flush every queued record before any file in this batch is marked
        // complete. This preserves "flush before complete" while allowing the
        // producer to batch thousands of rows.
        self.flush_pending().await?;

        let rows: Vec<FileCompletionRow> = files
            .iter()
            .map(|file| FileCompletionRow {
                source_id: file.source_id.clone(),
                file_ord: file.file_ord,
                row_count: file.row_count,
            })
            .collect();
        self.repository
            .mark_files_complete(job_id, &rows)
            .await
            .map_err(|error| IngestError::Staging(error.to_string()))?;
        Ok(())
    }
}
