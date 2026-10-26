//! Configuration loading with file + environment precedence.
//!
//! `DISTRIBUTED_CONFIG` points at a JSON file with `kafka_bootstrap`,
//! `database_url`, and `database_password`. Environment variables override the
//! file. Secrets are never emitted verbatim by `config show`.

use std::path::Path;

use serde::ser::SerializeMap;
use serde::{Deserialize, Serialize, Serializer};

#[derive(Debug, Clone, Default, Deserialize)]
pub struct FileConfig {
    pub kafka_bootstrap: Option<String>,
    pub database_url: Option<String>,
    pub database_password: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub kafka_bootstrap: Option<String>,
    pub database_url: Option<String>,
    pub database_password: Option<String>,
    pub topology: &'static str,
}

impl Config {
    pub fn load() -> Result<Self, String> {
        let file = match std::env::var("DISTRIBUTED_CONFIG") {
            Ok(path) => {
                let text = std::fs::read_to_string(Path::new(&path)).map_err(|error| {
                    format!("read configuration file '{path}': {error}")
                })?;
                serde_json::from_str::<FileConfig>(&text).map_err(|error| {
                    format!("parse configuration file '{path}': {error}")
                })?
            }
            Err(_) => FileConfig::default(),
        };

        Ok(Self {
            kafka_bootstrap: env_or("KAFKA_BOOTSTRAP", file.kafka_bootstrap),
            database_url: env_or("DATABASE_URL", file.database_url),
            database_password: env_or("DATABASE_PASSWORD", file.database_password),
            topology: "single-logical-database",
        })
    }

    pub fn require_broker(&self) -> Result<&str, String> {
        self.kafka_bootstrap.as_deref().ok_or_else(|| {
            "missing required broker configuration: KAFKA_BOOTSTRAP".to_string()
        })
    }

    pub fn require_database(&self) -> Result<&str, String> {
        self.database_url.as_deref().ok_or_else(|| {
            "missing required database configuration: DATABASE_URL".to_string()
        })
    }
}

impl Serialize for Config {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        map.serialize_entry("kafka_bootstrap", &self.kafka_bootstrap)?;
        map.serialize_entry(
            "database_url",
            &self.database_url.as_deref().map(redact_url),
        )?;
        map.serialize_entry(
            "database_password",
            &self.database_password.as_ref().map(|_| "***redacted***"),
        )?;
        map.serialize_entry("topology", self.topology)?;
        map.end()
    }
}

fn env_or(name: &str, fallback: Option<String>) -> Option<String> {
    match std::env::var(name) {
        Ok(value) if !value.trim().is_empty() => Some(value),
        _ => fallback,
    }
}

/// Replace the password component of a database URL with `***`.
pub fn redact_url(url: &str) -> String {
    let Some(scheme_end) = url.find("://") else {
        return url.to_string();
    };
    let credentials_start = scheme_end + 3;
    let Some(at) = url[credentials_start..].find('@') else {
        return url.to_string();
    };
    let at = credentials_start + at;
    let Some(colon) = url[credentials_start..at].find(':') else {
        return url.to_string();
    };
    let colon = credentials_start + colon;
    format!("{}***{}", &url[..colon + 1], &url[at..])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_credentials_in_a_database_url() {
        assert_eq!(
            redact_url("postgres://ledger:secret@localhost:5432/ledger"),
            "postgres://ledger:***@localhost:5432/ledger"
        );
        assert_eq!(
            redact_url("mysql://ledger:secret@localhost:3306/ledger"),
            "mysql://ledger:***@localhost:3306/ledger"
        );
        assert_eq!(
            redact_url("postgres://localhost/ledger"),
            "postgres://localhost/ledger"
        );
    }
}
