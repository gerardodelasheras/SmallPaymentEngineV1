//! Kafka-backed staging sink.
//!
//! Records are keyed by `source_id:external_client`, so every row for one
//! source-scoped client lands on one staging partition and is owned by one
//! partitioner. Event IDs are content-addressed; Kafka may redeliver a record,
//! and the durable ledger dedupes it.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;

use crate::ingest::{IngestError, StagingRecord, StagingSink};
use crate::repository::LedgerRepository;

const CLIENT_ID: &str = "distributed-ingest";
const PRODUCE_TIMEOUT: Duration = Duration::from_secs(10);

pub struct KafkaStagingSink {
    producer: FutureProducer,
    topic: String,
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
            .set("message.timeout.ms", "10000")
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
            repository,
        })
    }
}

#[async_trait]
impl StagingSink for KafkaStagingSink {
    async fn stage(&self, record: StagingRecord) -> Result<(), IngestError> {
        let key = record.staging_key();
        let value = serde_json::to_vec(&record).map_err(|error| {
            IngestError::Staging(format!("serialize staging record: {error}"))
        })?;

        let delivery = self
            .producer
            .send(
                FutureRecord::to(&self.topic)
                    .key(key.as_bytes())
                    .payload(value.as_slice()),
                Timeout::After(PRODUCE_TIMEOUT),
            )
            .await;

        delivery.map_err(|(error, _message)| {
            IngestError::Staging(format!("produce to staging topic: {error}"))
        })?;
        Ok(())
    }

    async fn complete_file(
        &self,
        job_id: &str,
        source_id: &str,
        file_ord: u32,
        row_count: u64,
    ) -> Result<(), IngestError> {
        self.repository
            .mark_file_complete(job_id, source_id, file_ord, row_count)
            .await
            .map_err(|error| IngestError::Staging(error.to_string()))
    }
}
