//! Dead-letter path for events that exhaust their retry budget.
//!
//! A poison event is published with enough metadata for an operator to find the
//! original job, file, and row without replaying the ledger. The worker marks
//! the job failed after publishing; the event is never silently skipped.

use std::time::{Duration, Instant};

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use rdkafka::{Message, Offset, TopicPartitionList};
use serde::{Deserialize, Serialize};

const PRODUCE_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DlqRecord {
    pub event_id: String,
    pub job_id: String,
    pub source_id: String,
    pub file_hash: String,
    pub file_ord: i32,
    pub row_ord: i32,
    pub attempts: u32,
    pub error: String,
}

#[derive(Debug, thiserror::Error)]
pub enum DlqError {
    #[error("kafka error: {0}")]
    Kafka(String),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

pub struct KafkaDlqSink {
    producer: FutureProducer,
    topic: String,
}

impl KafkaDlqSink {
    pub async fn connect(bootstrap: &str, topic: &str) -> Result<Self, DlqError> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap)
            .set("client.id", "distributed-worker-dlq")
            .set("message.timeout.ms", "30000")
            .set("enable.idempotence", "true")
            .set("acks", "all")
            .create()
            .map_err(|error| {
                DlqError::Kafka(format!("connect DLQ producer: {error}"))
            })?;
        Ok(Self {
            producer,
            topic: topic.to_string(),
        })
    }

    pub async fn send(&self, record: &DlqRecord) -> Result<(), DlqError> {
        let payload = serde_json::to_vec(record)?;
        let delivery = self
            .producer
            .send(
                FutureRecord::to(&self.topic)
                    .key(record.event_id.as_bytes())
                    .payload(payload.as_slice()),
                Timeout::After(PRODUCE_TIMEOUT),
            )
            .await;
        delivery
            .map(|_| ())
            .map_err(|(error, _message)| {
                DlqError::Kafka(format!("publish dead-letter record: {error}"))
            })
    }
}

/// Read the dead-letter topic from the beginning.
///
/// This is an operator query, not a service consumer: it assigns partitions
/// directly, reads to the end, and never commits.
pub async fn list(
    bootstrap: &str,
    topic: &str,
    job_filter: Option<&str>,
) -> Result<Vec<DlqRecord>, DlqError> {
    let group = format!(
        "distributed-dlq-list-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_nanos())
            .unwrap_or_default()
    );
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", &group)
        .set("client.id", "distributed-dlq-list")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("enable.partition.eof", "true")
        .set("session.timeout.ms", "6000")
        .set("socket.timeout.ms", "10000")
        .create()
        .map_err(|error| {
            DlqError::Kafka(format!("create DLQ query consumer: {error}"))
        })?;

    let metadata = consumer
        .fetch_metadata(Some(topic), Duration::from_secs(10))
        .map_err(|error| {
            DlqError::Kafka(format!("fetch DLQ metadata: {error}"))
        })?;
    let mut offsets = TopicPartitionList::new();
    for candidate in metadata.topics() {
        if candidate.name() != topic {
            continue;
        }
        for partition in candidate.partitions() {
            offsets
                .add_partition_offset(topic, partition.id(), Offset::Beginning)
                .map_err(|error| {
                    DlqError::Kafka(format!("build DLQ assignment: {error}"))
                })?;
        }
    }
    let partitions = offsets.count();
    if partitions == 0 {
        return Ok(Vec::new());
    }
    consumer.assign(&offsets).map_err(|error| {
        DlqError::Kafka(format!("assign DLQ partitions: {error}"))
    })?;

    let mut records = Vec::new();
    let mut eofs = 0_usize;
    let deadline = Instant::now() + Duration::from_secs(10);
    while eofs < partitions && Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(1), consumer.recv()).await {
            Ok(Ok(message)) => {
                let payload = message.payload().unwrap_or_default();
                let record: DlqRecord = serde_json::from_slice(payload)?;
                if job_filter
                    .map(|job_id| record.job_id == job_id)
                    .unwrap_or(true)
                {
                    records.push(record);
                }
            }
            Ok(Err(KafkaError::PartitionEOF(_))) => eofs += 1,
            Ok(Err(error)) => {
                return Err(DlqError::Kafka(format!("read DLQ: {error}")));
            }
            Err(_) => break,
        }
    }

    consumer
        .commit(&TopicPartitionList::new(), CommitMode::Async)
        .ok();
    Ok(records)
}
