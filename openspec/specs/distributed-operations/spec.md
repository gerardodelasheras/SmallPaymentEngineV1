# Distributed Operations Specification

## Purpose

Operationally safe configuration, startup validation, retries, dead letters, health, and metrics for the distributed services.

## Requirements

### Requirement: Configuration is validated at startup

The system SHALL load configuration from documented file and environment sources, SHALL fail fast with a clear error when a required broker, database, topic, or credential is missing, and SHALL NOT log secret values.

#### Scenario: Missing broker configuration fails fast

```gherkin
Given no broker address is configured
When a service starts
Then it exits with a clear configuration error
And no partial job is accepted
```

#### Scenario: Environment overrides file configuration

```gherkin
Given a broker address in the file and a different one in the environment
When the service starts
Then the environment value is used
```

#### Scenario: Secrets are not logged

```gherkin
Given a configured database password
When the service starts and logs its configuration
Then the password is redacted
```

### Requirement: Migrations and topic setup are idempotent

The system SHALL provide a migration command that applies database schema migrations and verifies or creates the required topics. Running it repeatedly SHALL be a no-op after the first successful run.

#### Scenario: Running migrations twice is safe

```gherkin
Given a migrated database and provisioned topics
When the migration command runs again
Then no schema or topic is duplicated
And the command exits successfully
```

### Requirement: Retries are bounded and exhaust to a dead-letter path

The system SHALL bound retries for transient processing failures and SHALL send an event that exhausts its budget to a dead-letter path with its job, file, row, and error metadata. The job SHALL NOT silently skip the event.

#### Scenario: A poison event reaches the dead-letter path

```gherkin
Given an event that fails every retry
When the budget is exhausted
Then the event appears in the dead-letter path with metadata
And the job is marked failed
And no partial export is written
```

### Requirement: Health and metrics expose operational state

The system SHALL expose health and readiness for each service and metrics for consumer lag, dead-letter depth, stalled watermarks, and failed jobs, without exposing secrets.

#### Scenario: A healthy service reports ready

```gherkin
Given a service can reach its broker and database
When readiness is checked
Then it reports ready
```

#### Scenario: A stalled watermark is visible

```gherkin
Given a job's file watermark has not advanced
When the metrics are read
Then the stalled watermark is reported
And the job's failed or paused status is visible
```
