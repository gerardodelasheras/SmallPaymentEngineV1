//! Manifest model for batch ingestion.
//!
//! A manifest names a source and an ordered set of files. Each file carries a
//! unique ordinal, a SHA-256 content hash, and a location. The manifest hash is
//! a deterministic digest over `(source, ordinal, file hash)` and is used to
//! recognize a resubmitted manifest.

use std::collections::HashSet;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use sha2::{Digest, Sha256};

const HASH_HEX_LEN: usize = 64;
const HASH_BUFFER: usize = 64 * 1024;

#[derive(Debug, Clone, Deserialize)]
pub struct ManifestInput {
    pub source_id: String,
    pub files: Vec<ManifestFileInput>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ManifestFileInput {
    #[serde(default)]
    pub source_id: Option<String>,
    pub ordinal: u32,
    pub path: String,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedFile {
    pub source_id: String,
    pub ordinal: u32,
    pub path: PathBuf,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedManifest {
    pub source_id: String,
    pub manifest_hash: String,
    pub files: Vec<ValidatedFile>,
}

#[derive(Debug, thiserror::Error)]
pub enum ManifestError {
    #[error("manifest JSON is invalid: {0}")]
    InvalidJson(#[from] serde_json::Error),

    #[error("manifest source is empty")]
    EmptySource,

    #[error("manifest contains no files")]
    EmptyManifest,

    #[error("manifest is not readable at {}: {source}", path.display())]
    ManifestRead {
        path: PathBuf,
        source: std::io::Error,
    },

    #[error("duplicate file ordinal {ordinal} for source '{source_id}'")]
    DuplicateOrdinal { source_id: String, ordinal: u32 },

    #[error("file ordinal {ordinal} is missing at {}", path.display())]
    MissingFile { ordinal: u32, path: PathBuf },

    #[error("file ordinal {ordinal} is not readable at {}: {source}", path.display())]
    FileRead {
        ordinal: u32,
        path: PathBuf,
        source: std::io::Error,
    },

    #[error("file ordinal {ordinal} has an invalid SHA-256 value")]
    InvalidHash { ordinal: u32 },

    #[error(
        "file ordinal {ordinal} hash mismatch at {}: expected {expected}, got {actual}",
        path.display()
    )]
    HashMismatch {
        ordinal: u32,
        path: PathBuf,
        expected: String,
        actual: String,
    },
}

/// Validate an in-memory manifest, hashing every file and sorting by
/// `(source, ordinal)`.
pub fn validate(input: ManifestInput) -> Result<ValidatedManifest, ManifestError> {
    if input.source_id.trim().is_empty() {
        return Err(ManifestError::EmptySource);
    }
    if input.files.is_empty() {
        return Err(ManifestError::EmptyManifest);
    }

    let mut seen = HashSet::new();
    let mut files = Vec::with_capacity(input.files.len());

    for file in input.files {
        let source_id = file
            .source_id
            .filter(|source| !source.trim().is_empty())
            .unwrap_or_else(|| input.source_id.clone());

        if !seen.insert(file.ordinal) {
            return Err(ManifestError::DuplicateOrdinal {
                source_id,
                ordinal: file.ordinal,
            });
        }

        if !is_valid_hash(&file.sha256) {
            return Err(ManifestError::InvalidHash {
                ordinal: file.ordinal,
            });
        }

        let path = PathBuf::from(file.path);
        if !path.is_file() {
            return Err(ManifestError::MissingFile {
                ordinal: file.ordinal,
                path,
            });
        }

        let actual = hash_file(&path, file.ordinal)?;
        let expected = file.sha256.to_ascii_lowercase();
        if actual != expected {
            return Err(ManifestError::HashMismatch {
                ordinal: file.ordinal,
                path,
                expected,
                actual,
            });
        }

        files.push(ValidatedFile {
            source_id,
            ordinal: file.ordinal,
            path,
            sha256: actual,
        });
    }

    files.sort_by(|left, right| {
        left.source_id
            .cmp(&right.source_id)
            .then(left.ordinal.cmp(&right.ordinal))
    });

    let manifest_hash = manifest_hash(&files);
    Ok(ValidatedManifest {
        source_id: input.source_id,
        manifest_hash,
        files,
    })
}

/// Read and validate a JSON manifest from disk.
pub fn load(path: &Path) -> Result<ValidatedManifest, ManifestError> {
    let bytes = std::fs::read(path).map_err(|source| ManifestError::ManifestRead {
        path: path.to_path_buf(),
        source,
    })?;
    let input: ManifestInput = serde_json::from_slice(&bytes)?;
    validate(input)
}

fn hash_file(path: &Path, ordinal: u32) -> Result<String, ManifestError> {
    let mut file = File::open(path).map_err(|source| ManifestError::FileRead {
        ordinal,
        path: path.to_path_buf(),
        source,
    })?;

    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; HASH_BUFFER];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|source| ManifestError::FileRead {
                ordinal,
                path: path.to_path_buf(),
                source,
            })?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hex::encode(hasher.finalize()))
}

fn manifest_hash(files: &[ValidatedFile]) -> String {
    let mut hasher = Sha256::new();
    for file in files {
        hasher.update(file.source_id.as_bytes());
        hasher.update(b"\n");
        hasher.update(file.ordinal.to_string().as_bytes());
        hasher.update(b"\n");
        hasher.update(file.sha256.as_bytes());
        hasher.update(b"\n");
    }
    hex::encode(hasher.finalize())
}

fn is_valid_hash(value: &str) -> bool {
    value.len() == HASH_HEX_LEN && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    fn write_file(dir: &Path, name: &str, content: &str) -> (PathBuf, String) {
        let path = dir.join(name);
        fs::write(&path, content).expect("write fixture");
        let mut hasher = Sha256::new();
        hasher.update(content.as_bytes());
        (path, hex::encode(hasher.finalize()))
    }

    fn file(ordinal: u32, path: &Path, sha256: &str) -> ManifestFileInput {
        ManifestFileInput {
            source_id: None,
            ordinal,
            path: path.to_string_lossy().into_owned(),
            sha256: sha256.to_string(),
        }
    }

    #[test]
    fn valid_manifest_sorts_files_and_hashes_content() {
        let dir = tempfile::tempdir().expect("temp dir");
        let (first, first_hash) = write_file(dir.path(), "first.csv", "deposit,1,1,1.0\n");
        let (second, second_hash) = write_file(dir.path(), "second.csv", "deposit,1,2,2.0\n");

        let manifest = validate(ManifestInput {
            source_id: "alpha".to_string(),
            files: vec![
                file(2, &second, &second_hash),
                file(1, &first, &first_hash),
            ],
        })
        .expect("valid manifest");

        assert_eq!(manifest.source_id, "alpha");
        assert_eq!(manifest.files.len(), 2);
        assert_eq!(manifest.files[0].ordinal, 1);
        assert_eq!(manifest.files[0].source_id, "alpha");
        assert_eq!(manifest.files[0].sha256, first_hash);
        assert_eq!(manifest.files[1].ordinal, 2);
        assert_eq!(manifest.manifest_hash.len(), HASH_HEX_LEN);
    }

    #[test]
    fn duplicate_ordinal_is_rejected() {
        let dir = tempfile::tempdir().expect("temp dir");
        let (first, first_hash) = write_file(dir.path(), "first.csv", "deposit,1,1,1.0\n");
        let (second, second_hash) = write_file(dir.path(), "second.csv", "deposit,1,2,2.0\n");

        let result = validate(ManifestInput {
            source_id: "alpha".to_string(),
            files: vec![
                file(1, &first, &first_hash),
                file(1, &second, &second_hash),
            ],
        });

        assert!(matches!(
            result,
            Err(ManifestError::DuplicateOrdinal { ordinal: 1, .. })
        ));
    }

    #[test]
    fn missing_file_is_rejected() {
        let dir = tempfile::tempdir().expect("temp dir");
        let missing = dir.path().join("missing.csv");

        let result = validate(ManifestInput {
            source_id: "alpha".to_string(),
            files: vec![file(1, &missing, &"0".repeat(HASH_HEX_LEN))],
        });

        assert!(matches!(result, Err(ManifestError::MissingFile { ordinal: 1, .. })));
    }

    #[test]
    fn hash_mismatch_is_rejected() {
        let dir = tempfile::tempdir().expect("temp dir");
        let (path, _) = write_file(dir.path(), "first.csv", "deposit,1,1,1.0\n");

        let result = validate(ManifestInput {
            source_id: "alpha".to_string(),
            files: vec![file(1, &path, &"a".repeat(HASH_HEX_LEN))],
        });

        assert!(matches!(result, Err(ManifestError::HashMismatch { ordinal: 1, .. })));
    }

    #[test]
    fn manifest_hash_is_order_independent() {
        let dir = tempfile::tempdir().expect("temp dir");
        let (first, first_hash) = write_file(dir.path(), "first.csv", "deposit,1,1,1.0\n");
        let (second, second_hash) = write_file(dir.path(), "second.csv", "deposit,1,2,2.0\n");

        let forward = validate(ManifestInput {
            source_id: "alpha".to_string(),
            files: vec![
                file(1, &first, &first_hash),
                file(2, &second, &second_hash),
            ],
        })
        .expect("forward");
        let reverse = validate(ManifestInput {
            source_id: "alpha".to_string(),
            files: vec![
                file(2, &second, &second_hash),
                file(1, &first, &first_hash),
            ],
        })
        .expect("reverse");

        assert_eq!(forward.manifest_hash, reverse.manifest_hash);
    }

    #[test]
    fn manifest_hash_changes_when_content_changes() {
        let dir = tempfile::tempdir().expect("temp dir");
        let (first, first_hash) = write_file(dir.path(), "first.csv", "deposit,1,1,1.0\n");
        let (changed, changed_hash) = write_file(dir.path(), "changed.csv", "deposit,1,1,9.0\n");

        let one = validate(ManifestInput {
            source_id: "alpha".to_string(),
            files: vec![file(1, &first, &first_hash)],
        })
        .expect("one file");
        let two = validate(ManifestInput {
            source_id: "alpha".to_string(),
            files: vec![file(1, &changed, &changed_hash)],
        })
        .expect("changed file");

        assert_ne!(one.manifest_hash, two.manifest_hash);
    }

    #[test]
    fn load_reads_the_json_format() {
        let dir = tempfile::tempdir().expect("temp dir");
        let (path, hash) = write_file(dir.path(), "first.csv", "deposit,1,1,1.0\n");
        let json = serde_json::json!({
            "source_id": "alpha",
            "files": [{
                "ordinal": 1,
                "path": path,
                "sha256": hash,
            }],
        });
        let manifest_path = dir.path().join("manifest.json");
        fs::write(&manifest_path, serde_json::to_vec(&json).expect("json"))
            .expect("write manifest");

        let manifest = load(&manifest_path).expect("load manifest");

        assert_eq!(manifest.source_id, "alpha");
        assert_eq!(manifest.files.len(), 1);
    }

    #[test]
    fn empty_manifest_is_rejected() {
        let result = validate(ManifestInput {
            source_id: "alpha".to_string(),
            files: Vec::new(),
        });

        assert!(matches!(result, Err(ManifestError::EmptyManifest)));
    }

    #[test]
    fn invalid_hash_is_rejected() {
        let dir = tempfile::tempdir().expect("temp dir");
        let (path, _) = write_file(dir.path(), "first.csv", "deposit,1,1,1.0\n");

        let result = validate(ManifestInput {
            source_id: "alpha".to_string(),
            files: vec![file(1, &path, "not-a-hash")],
        });

        assert!(matches!(result, Err(ManifestError::InvalidHash { ordinal: 1 })));
    }
}
