//! Kafka-backed staging sink.
//!
//! Records are keyed by `source_id:external_client`, so every row for one
//! source-scoped client lands on one staging partition and is owned by one
//! partitioner. Event IDs are content-addressed; Kafka may redeliver a record,
//! and the durable ledger dedupes it.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use rskafka::chrono::Utc;
use rskafka::client::partition::{
    Compression, PartitionClient, UnknownTopicHandling,
};
use rskafka::client::{Client, ClientBuilder};
use rskafka::record::Record;
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

use crate::ingest::{IngestError, StagingRecord, StagingSink};
use crate::repository::LedgerRepository;

const CLIENT_ID: &str = "distributed-ingest";

pub struct KafkaStagingSink {
    client: Arc<Client>,
    topic: String,
    partitions: Vec<i32>,
    partition_clients: Mutex<HashMap<i32, Arc<PartitionClient>>>,
    repository: Arc<dyn LedgerRepository>,
}

impl KafkaStagingSink {
    pub async fn connect(
        bootstrap: &str,
        topic: &str,
        repository: Arc<dyn LedgerRepository>,
    ) -> Result<Self, IngestError> {
        let client = ClientBuilder::new(vec![bootstrap.to_string()])
            .client_id(CLIENT_ID)
            .build()
            .await
            .map_err(|error| {
                IngestError::Staging(format!("connect to Kafka: {error}"))
            })?;

        let topics = client.list_topics().await.map_err(|error| {
            IngestError::Staging(format!("list Kafka topics: {error}"))
        })?;
        let topic_metadata = topics
            .into_iter()
            .find(|candidate| candidate.name == topic)
            .ok_or_else(|| {
                IngestError::Staging(format!(
                    "staging topic '{topic}' is missing; run migrate"
                ))
            })?;

        let partitions: Vec<i32> = topic_metadata.partitions.into_iter().collect();
        if partitions.is_empty() {
            return Err(IngestError::Staging(format!(
                "staging topic '{topic}' has no partitions"
            )));
        }

        Ok(Self {
            client: Arc::new(client),
            topic: topic.to_string(),
            partitions,
            partition_clients: Mutex::new(HashMap::new()),
            repository,
        })
    }

    /// Stable key-to-partition mapping. The partitioner consumes every staging
    /// partition and groups by the record key, so this only needs to be stable
    /// within the producer fleet.
    pub fn partition_for(&self, key: &str) -> i32 {
        let mut hasher = Sha256::new();
        hasher.update(key.as_bytes());
        let digest = hasher.finalize();
        let value = u64::from_be_bytes(
            digest[..8].try_into().expect("SHA-256 digest has at least 8 bytes"),
        );
        let index = (value % self.partitions.len() as u64) as usize;
        self.partitions[index]
    }

    async fn partition_client(
        &self,
        partition: i32,
    ) -> Result<Arc<PartitionClient>, IngestError> {
        let mut clients = self.partition_clients.lock().await;
        if let Some(client) = clients.get(&partition) {
            return Ok(Arc::clone(client));
        }

        let client = self
            .client
            .partition_client(
                self.topic.clone(),
                partition,
                UnknownTopicHandling::Retry,
            )
            .await
            .map_err(|error| {
                IngestError::Staging(format!(
                    "open staging partition {partition}: {error}"
                ))
            })?;
        let client = Arc::new(client);
        clients.insert(partition, Arc::clone(&client));
        Ok(client)
    }
}

#[async_trait]
impl StagingSink for KafkaStagingSink {
    async fn stage(&self, record: StagingRecord) -> Result<(), IngestError> {
        let key = record.staging_key();
        let partition = self.partition_for(&key);
        let value = serde_json::to_vec(&record).map_err(|error| {
            IngestError::Staging(format!("serialize staging record: {error}"))
        })?;

        let kafka_record = Record {
            key: Some(key.into_bytes()),
            value: Some(value),
            headers: BTreeMap::new(),
            timestamp: Utc::now(),
        };

        let client = self.partition_client(partition).await?;
        let offsets = client
            .produce(vec![kafka_record], Compression::NoCompression)
            .await
            .map_err(|error| {
                IngestError::Staging(format!(
                    "produce to staging partition {partition}: {error}"
                ))
            })?;

        if offsets.len() != 1 {
            return Err(IngestError::Staging(
                "producer returned no offset".to_string(),
            ));
        }
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
