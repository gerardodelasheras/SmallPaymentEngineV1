# Client Identity Specification

## Purpose

Canonical, source-scoped BIGINT client identities that are minted once, reused across jobs, and keyed onto Kafka partitions.

## Requirements

### Requirement: Canonical client IDs are source-scoped and stable

The system SHALL assign a globally unique `BIGINT` canonical client ID to each `(source_id, external_client)` pair on first sight and SHALL return the same ID for every later sighting of that pair. Two sources using the same external client value SHALL receive different canonical IDs.

#### Scenario: A new source-scoped client is minted

```gherkin
Given source "alpha" has no mapping for external client 7
When the partitioner maps source "alpha" client 7
Then a canonical BIGINT client ID is assigned
And the mapping is persisted
```

#### Scenario: Re-ingesting the same key returns the same ID

```gherkin
Given source "alpha" client 7 has canonical ID 42
When a later job maps source "alpha" client 7
Then the canonical ID is 42
And no second client row is created
```

#### Scenario: The same external value in another source is a different client

```gherkin
Given source "alpha" client 7 has canonical ID 42
When source "beta" client 7 is mapped
Then it receives a different canonical ID
```

### Requirement: Mint-or-get is atomic under concurrency

The system SHALL resolve concurrent first sightings of the same source-scoped key to exactly one canonical ID, with no duplicate identity rows.

#### Scenario: Concurrent mapping converges

```gherkin
Given two partitioner instances map source "alpha" client 7 at the same time
When both mappings complete
Then both emit the same canonical ID
And exactly one client row exists for that key
```

### Requirement: The partitioner emits canonical Kafka keys

The system SHALL perform external-key mapping before producing to the transactions topic, and every transaction record SHALL use the canonical client ID as its Kafka key. Workers SHALL NOT mint or remap identities.

#### Scenario: A staged event is emitted with the canonical key

```gherkin
Given a staged row for source "alpha" client 7
And source "alpha" client 7 has canonical ID 42
When the partitioner emits the row
Then the transactions record key is 42
And the worker applies it to canonical client 42
```
