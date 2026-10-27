#![forbid(unsafe_code)]

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;

pub const GENESIS_HASH: &str = "GENESIS";
const SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid JSON on line {line}: {source}")]
    InvalidJson {
        line: usize,
        source: serde_json::Error,
    },
    #[error("ledger invariant failed on line {line}: {message}")]
    InvalidLedger { line: usize, message: String },
    #[error("ledger already exists: {0}")]
    AlreadyExists(PathBuf),
    #[error("ledger does not exist: {0}")]
    NotFound(PathBuf),
    #[error("stale ledger head: expected {expected}, found {actual}")]
    StaleHead { expected: String, actual: String },
    #[error("invalid append request: {0}")]
    InvalidRequest(String),
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Stance {
    Support,
    Oppose,
    Neutral,
}

impl std::fmt::Display for Stance {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            Self::Support => "support",
            Self::Oppose => "oppose",
            Self::Neutral => "neutral",
        };
        formatter.write_str(value)
    }
}

impl std::str::FromStr for Stance {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "support" => Ok(Self::Support),
            "oppose" => Ok(Self::Oppose),
            "neutral" => Ok(Self::Neutral),
            _ => Err(format!(
                "unknown stance {value:?}; use support, oppose, or neutral"
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LedgerEvent {
    pub schema_version: u32,
    pub sequence: u64,
    pub timestamp_ms: u64,
    pub id: String,
    pub topic: String,
    pub stance: Stance,
    pub claim: String,
    pub source: String,
    pub author: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub supersedes: Vec<String>,
    pub previous_hash: String,
    pub hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendRequest {
    pub topic: String,
    pub stance: Stance,
    pub claim: String,
    pub source: String,
    pub author: String,
    pub supersedes: Vec<String>,
    pub expected_head: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Verification {
    pub valid: bool,
    pub events: usize,
    pub topics: usize,
    pub unresolved_conflicts: usize,
    pub head: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Snapshot {
    pub schema_version: u32,
    pub generated_at_ms: u64,
    pub ledger_head: String,
    pub event_count: usize,
    pub topics: Vec<TopicSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicSnapshot {
    pub topic: String,
    pub has_unresolved_conflict: bool,
    pub claims: Vec<SnapshotClaim>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotClaim {
    pub id: String,
    pub stance: Stance,
    pub claim: String,
    pub source: String,
    pub author: String,
    pub timestamp_ms: u64,
}

#[derive(Serialize)]
struct HashMaterial<'a> {
    schema_version: u32,
    sequence: u64,
    timestamp_ms: u64,
    id: &'a str,
    topic: &'a str,
    stance: Stance,
    claim: &'a str,
    source: &'a str,
    author: &'a str,
    supersedes: &'a [String],
    previous_hash: &'a str,
}

pub fn initialize(path: impl AsRef<Path>) -> Result<()> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map(|_| ())
        .map_err(|error| {
            if error.kind() == std::io::ErrorKind::AlreadyExists {
                Error::AlreadyExists(path.to_path_buf())
            } else {
                Error::Io(error)
            }
        })
}

pub fn read(path: impl AsRef<Path>) -> Result<Vec<LedgerEvent>> {
    let path = path.as_ref();
    if !path.exists() {
        return Err(Error::NotFound(path.to_path_buf()));
    }
    let content = fs::read_to_string(path)?;
    parse_and_verify(&content)
}

pub fn append(path: impl AsRef<Path>, request: AppendRequest) -> Result<LedgerEvent> {
    validate_request(&request)?;
    let path = path.as_ref();
    if !path.exists() {
        return Err(Error::NotFound(path.to_path_buf()));
    }

    let lock_path = lock_path(path);
    let lock_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(lock_path)?;
    lock_file.lock_exclusive()?;

    let mut ledger_file = OpenOptions::new().read(true).write(true).open(path)?;
    let mut content = String::new();
    ledger_file.read_to_string(&mut content)?;
    let events = parse_and_verify(&content)?;
    let head = events
        .last()
        .map(|event| event.hash.as_str())
        .unwrap_or(GENESIS_HASH);

    if let Some(expected) = &request.expected_head {
        if expected != head {
            return Err(Error::StaleHead {
                expected: expected.clone(),
                actual: head.to_owned(),
            });
        }
    }
    validate_supersedes(&events, &request)?;

    let sequence = events.len() as u64 + 1;
    let mut event = LedgerEvent {
        schema_version: SCHEMA_VERSION,
        sequence,
        timestamp_ms: now_ms()?,
        id: format!("evt-{sequence:06}"),
        topic: request.topic,
        stance: request.stance,
        claim: request.claim,
        source: request.source,
        author: request.author,
        supersedes: request.supersedes,
        previous_hash: head.to_owned(),
        hash: String::new(),
    };
    event.hash = compute_hash(&event)?;

    ledger_file.seek(SeekFrom::End(0))?;
    serde_json::to_writer(&mut ledger_file, &event)?;
    ledger_file.write_all(b"\n")?;
    ledger_file.sync_all()?;
    Ok(event)
}

pub fn verify(path: impl AsRef<Path>) -> Result<Verification> {
    let events = read(path)?;
    let snapshot = snapshot_from_events(&events)?;
    Ok(Verification {
        valid: true,
        events: events.len(),
        topics: snapshot.topics.len(),
        unresolved_conflicts: snapshot
            .topics
            .iter()
            .filter(|topic| topic.has_unresolved_conflict)
            .count(),
        head: head(&events).to_owned(),
    })
}

pub fn snapshot(path: impl AsRef<Path>) -> Result<Snapshot> {
    let events = read(path)?;
    snapshot_from_events(&events)
}

pub fn write_snapshot(snapshot: &Snapshot, output: impl AsRef<Path>) -> Result<()> {
    let output = output.as_ref();
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent)?;
    }
    let temporary = output.with_extension("tmp");
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&temporary)?;
    serde_json::to_writer_pretty(&mut file, snapshot)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    fs::rename(temporary, output)?;
    Ok(())
}

pub fn head(events: &[LedgerEvent]) -> &str {
    events
        .last()
        .map(|event| event.hash.as_str())
        .unwrap_or(GENESIS_HASH)
}

fn parse_and_verify(content: &str) -> Result<Vec<LedgerEvent>> {
    let mut events = Vec::new();
    let mut by_id: HashMap<String, (String, usize)> = HashMap::new();
    let mut superseded = HashSet::new();
    let mut expected_previous = GENESIS_HASH.to_owned();

    for (index, line) in content.lines().enumerate() {
        let line_number = index + 1;
        if line.trim().is_empty() {
            return invalid(line_number, "blank lines are not permitted");
        }
        let event: LedgerEvent =
            serde_json::from_str(line).map_err(|source| Error::InvalidJson {
                line: line_number,
                source,
            })?;
        let expected_sequence = line_number as u64;
        if event.schema_version != SCHEMA_VERSION {
            return invalid(
                line_number,
                format!("unsupported schema version {}", event.schema_version),
            );
        }
        if event.sequence != expected_sequence {
            return invalid(
                line_number,
                format!(
                    "expected sequence {expected_sequence}, found {}",
                    event.sequence
                ),
            );
        }
        let expected_id = format!("evt-{expected_sequence:06}");
        if event.id != expected_id {
            return invalid(
                line_number,
                format!("expected id {expected_id}, found {}", event.id),
            );
        }
        if event.previous_hash != expected_previous {
            return invalid(line_number, "previous hash does not match ledger head");
        }
        let calculated = compute_hash(&event)?;
        if event.hash != calculated {
            return invalid(line_number, "event hash does not match its contents");
        }
        validate_non_empty_event(&event, line_number)?;
        for target in &event.supersedes {
            let Some((topic, target_line)) = by_id.get(target) else {
                return invalid(
                    line_number,
                    format!("superseded event {target} does not precede this event"),
                );
            };
            if topic != &event.topic {
                return invalid(
                    line_number,
                    format!("cannot supersede {target} from a different topic"),
                );
            }
            if !superseded.insert(target.clone()) {
                return invalid(
                    line_number,
                    format!("event {target} was already superseded after line {target_line}"),
                );
            }
        }
        expected_previous.clone_from(&event.hash);
        by_id.insert(event.id.clone(), (event.topic.clone(), line_number));
        events.push(event);
    }
    Ok(events)
}

fn validate_request(request: &AppendRequest) -> Result<()> {
    for (name, value) in [
        ("topic", request.topic.as_str()),
        ("claim", request.claim.as_str()),
        ("source", request.source.as_str()),
        ("author", request.author.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(Error::InvalidRequest(format!("{name} cannot be empty")));
        }
        if value.contains(['\n', '\r']) {
            return Err(Error::InvalidRequest(format!(
                "{name} cannot contain newlines"
            )));
        }
    }
    let unique: BTreeSet<_> = request.supersedes.iter().collect();
    if unique.len() != request.supersedes.len() {
        return Err(Error::InvalidRequest(
            "supersedes cannot contain duplicate ids".to_owned(),
        ));
    }
    Ok(())
}

fn validate_supersedes(events: &[LedgerEvent], request: &AppendRequest) -> Result<()> {
    let by_id: HashMap<_, _> = events.iter().map(|event| (&event.id, event)).collect();
    let already_superseded: HashSet<_> = events
        .iter()
        .flat_map(|event| event.supersedes.iter())
        .collect();
    for target in &request.supersedes {
        let Some(event) = by_id.get(target) else {
            return Err(Error::InvalidRequest(format!(
                "superseded event {target} does not exist"
            )));
        };
        if event.topic != request.topic {
            return Err(Error::InvalidRequest(format!(
                "cannot supersede {target} from topic {:?} with topic {:?}",
                event.topic, request.topic
            )));
        }
        if already_superseded.contains(target) {
            return Err(Error::InvalidRequest(format!(
                "event {target} has already been superseded"
            )));
        }
    }
    Ok(())
}

fn validate_non_empty_event(event: &LedgerEvent, line: usize) -> Result<()> {
    for (name, value) in [
        ("topic", event.topic.as_str()),
        ("claim", event.claim.as_str()),
        ("source", event.source.as_str()),
        ("author", event.author.as_str()),
    ] {
        if value.trim().is_empty() {
            return invalid(line, format!("{name} cannot be empty"));
        }
    }
    Ok(())
}

fn snapshot_from_events(events: &[LedgerEvent]) -> Result<Snapshot> {
    let superseded: HashSet<_> = events
        .iter()
        .flat_map(|event| event.supersedes.iter().map(String::as_str))
        .collect();
    let mut topics: BTreeMap<&str, Vec<&LedgerEvent>> = BTreeMap::new();
    for event in events
        .iter()
        .filter(|event| !superseded.contains(event.id.as_str()))
    {
        topics.entry(&event.topic).or_default().push(event);
    }
    let topics = topics
        .into_iter()
        .map(|(topic, events)| {
            let stances: BTreeSet<_> = events.iter().map(|event| event.stance).collect();
            let has_unresolved_conflict =
                stances.contains(&Stance::Support) && stances.contains(&Stance::Oppose);
            TopicSnapshot {
                topic: topic.to_owned(),
                has_unresolved_conflict,
                claims: events
                    .into_iter()
                    .map(|event| SnapshotClaim {
                        id: event.id.clone(),
                        stance: event.stance,
                        claim: event.claim.clone(),
                        source: event.source.clone(),
                        author: event.author.clone(),
                        timestamp_ms: event.timestamp_ms,
                    })
                    .collect(),
            }
        })
        .collect();

    Ok(Snapshot {
        schema_version: SCHEMA_VERSION,
        generated_at_ms: now_ms()?,
        ledger_head: head(events).to_owned(),
        event_count: events.len(),
        topics,
    })
}

fn compute_hash(event: &LedgerEvent) -> Result<String> {
    let material = HashMaterial {
        schema_version: event.schema_version,
        sequence: event.sequence,
        timestamp_ms: event.timestamp_ms,
        id: &event.id,
        topic: &event.topic,
        stance: event.stance,
        claim: &event.claim,
        source: &event.source,
        author: &event.author,
        supersedes: &event.supersedes,
        previous_hash: &event.previous_hash,
    };
    let bytes = serde_json::to_vec(&material)?;
    Ok(hex::encode(Sha256::digest(bytes)))
}

fn now_ms() -> Result<u64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| Error::InvalidRequest(format!("system clock error: {error}")))?;
    u64::try_from(duration.as_millis())
        .map_err(|_| Error::InvalidRequest("system time exceeds u64 milliseconds".to_owned()))
}

fn lock_path(path: &Path) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(".lock");
    PathBuf::from(value)
}

fn invalid<T>(line: usize, message: impl Into<String>) -> Result<T> {
    Err(Error::InvalidLedger {
        line,
        message: message.into(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn request(topic: &str, stance: Stance, claim: &str) -> AppendRequest {
        AppendRequest {
            topic: topic.to_owned(),
            stance,
            claim: claim.to_owned(),
            source: "https://example.test/evidence".to_owned(),
            author: "test-agent".to_owned(),
            supersedes: Vec::new(),
            expected_head: None,
        }
    }

    #[test]
    fn append_and_verify_a_valid_chain() {
        let directory = tempdir().unwrap();
        let ledger = directory.path().join("context.jsonl");
        initialize(&ledger).unwrap();
        let first = append(&ledger, request("deploy", Stance::Support, "CI passed")).unwrap();
        let second = append(
            &ledger,
            request("deploy", Stance::Oppose, "production smoke test failed"),
        )
        .unwrap();

        assert_eq!(second.previous_hash, first.hash);
        let result = verify(&ledger).unwrap();
        assert!(result.valid);
        assert_eq!(result.events, 2);
        assert_eq!(result.unresolved_conflicts, 1);
        assert_eq!(result.head, second.hash);
    }

    #[test]
    fn detects_content_tampering() {
        let directory = tempdir().unwrap();
        let ledger = directory.path().join("context.jsonl");
        initialize(&ledger).unwrap();
        append(&ledger, request("deploy", Stance::Support, "CI passed")).unwrap();

        let changed = fs::read_to_string(&ledger)
            .unwrap()
            .replace("CI passed", "CI failed");
        fs::write(&ledger, changed).unwrap();

        let error = verify(&ledger).unwrap_err().to_string();
        assert!(error.contains("event hash does not match"));
    }

    #[test]
    fn rejects_a_stale_compare_and_swap_head() {
        let directory = tempdir().unwrap();
        let ledger = directory.path().join("context.jsonl");
        initialize(&ledger).unwrap();
        append(&ledger, request("deploy", Stance::Support, "CI passed")).unwrap();
        let mut stale = request("deploy", Stance::Neutral, "waiting for smoke test");
        stale.expected_head = Some(GENESIS_HASH.to_owned());

        let error = append(&ledger, stale).unwrap_err().to_string();
        assert!(error.contains("stale ledger head"));
    }

    #[test]
    fn snapshot_preserves_opposition_and_hides_superseded_claims() {
        let directory = tempdir().unwrap();
        let ledger = directory.path().join("context.jsonl");
        initialize(&ledger).unwrap();
        let original = append(
            &ledger,
            request("release", Stance::Support, "unit tests passed"),
        )
        .unwrap();
        append(
            &ledger,
            request("release", Stance::Oppose, "hosted CI failed"),
        )
        .unwrap();
        let mut replacement = request(
            "release",
            Stance::Support,
            "unit tests and hosted CI passed",
        );
        replacement.supersedes.push(original.id);
        append(&ledger, replacement).unwrap();

        let result = snapshot(&ledger).unwrap();
        assert_eq!(result.topics.len(), 1);
        let topic = &result.topics[0];
        assert!(topic.has_unresolved_conflict);
        assert_eq!(topic.claims.len(), 2);
        assert!(topic
            .claims
            .iter()
            .any(|claim| claim.stance == Stance::Oppose));
        assert!(!topic
            .claims
            .iter()
            .any(|claim| claim.claim == "unit tests passed"));
    }

    #[test]
    fn rejects_cross_topic_supersession() {
        let directory = tempdir().unwrap();
        let ledger = directory.path().join("context.jsonl");
        initialize(&ledger).unwrap();
        let event = append(&ledger, request("release", Stance::Support, "ready")).unwrap();
        let mut invalid = request("security", Stance::Neutral, "reviewed");
        invalid.supersedes.push(event.id);

        let error = append(&ledger, invalid).unwrap_err().to_string();
        assert!(error.contains("cannot supersede"));
        assert!(error.contains("release"));
        assert!(error.contains("security"));
    }
}
