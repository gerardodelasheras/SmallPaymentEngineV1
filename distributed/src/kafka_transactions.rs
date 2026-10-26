//! Kafka-backed transactions sink.
//!
//! Records are keyed by canonical client ID, so a client's events are ordered
//! on one transactions partition and processed by one worker.

use std::time::Duration;

use async_trait::async_trait;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;

use crate::partition::{PartitionError, TransactionRecord, TransactionSink};

const CLIENT_ID: &str = "distributed-partitioner";
const PRODUCE_TIMEOUT: Duration = Duration::from_secs(10);

pub struct KafkaTransactionSink {
    producer: FutureProducer,
    topic: String,
}

impl KafkaTransactionSink {
    pub async fn connect(bootstrap: &str, topic: &str) -> Result<Self, PartitionError> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap)
            .set("client.id", CLIENT_ID)
            .set("message.timeout.ms", "10000")
            .set("enable.idempotence", "true")
            .set("acks", "all")
            .set("partitioner", "murmur2_random")
            .create()
            .map_err(|error| {
                PartitionError::Sink(format!("connect to Kafka: {error}"))
            })?;

        Ok(Self {
            producer,
            topic: topic.to_string(),
        })
    }
}

#[async_trait]
impl TransactionSink for KafkaTransactionSink {
    async fn emit(&self, record: TransactionRecord) -> Result<i32, PartitionError> {
        let key = record.client_id.to_string();
        let value = serde_json::to_vec(&record).map_err(|error| {
            PartitionError::Sink(format!("serialize transaction record: {error}"))
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

        match delivery {
            Ok((partition, _offset)) => Ok(partition),
            Err((error, _message)) => Err(PartitionError::Sink(format!(
                "produce to transactions topic: {error}"
            ))),
        }
    }
}
