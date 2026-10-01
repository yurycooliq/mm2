use crate::config::Limits;
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Read, Seek, SeekFrom, Write},
    path::Path,
};

#[derive(Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Command {
    Control {
        id: String,
        expected_version: u64,
        action: String,
    },
    Limits {
        id: String,
        expected_version: u64,
        limits: Limits,
    },
}
impl Command {
    pub fn id(&self) -> &str {
        match self {
            Self::Control { id, .. } | Self::Limits { id, .. } => id,
        }
    }
    pub fn expected_version(&self) -> u64 {
        match self {
            Self::Control {
                expected_version, ..
            }
            | Self::Limits {
                expected_version, ..
            } => *expected_version,
        }
    }
    pub fn validate(&self) -> Result<(), String> {
        if self.id().is_empty()
            || self.id().len() > 64
            || !self
                .id()
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        {
            return Err("command id must contain 1-64 ASCII identifier characters".into());
        }
        match self {
            Self::Control { action, .. }
                if !["pause", "resume", "kill", "reset"].contains(&action.as_str()) =>
            {
                Err("unsupported shadow action".into())
            }
            Self::Limits { limits, .. } => {
                limits.core()?;
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Receipt {
    pub command_id: String,
    pub applied_version: u64,
    pub mode_at_ack: String,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Entry {
    Bootstrap {
        limits: Limits,
    },
    Command {
        command: Command,
        receipt: Receipt,
        operations: Vec<serde_json::Value>,
    },
    Execution {
        operations: Vec<serde_json::Value>,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    sequence: u64,
    previous_hash: String,
    hash: String,
    entry: Entry,
}

pub struct Journal {
    _fence: File,
    file: File,
    bytes: u64,
    max_bytes: u64,
    sequence: u64,
    previous_hash: String,
    bootstrap: Limits,
    history: Vec<(Command, Receipt)>,
    commands: BTreeMap<String, (String, Receipt)>,
    execution: Vec<serde_json::Value>,
}

fn digest(sequence: u64, previous: &str, entry: &Entry) -> Result<String, String> {
    let bytes = serde_json::to_vec(&(sequence, previous, entry))
        .map_err(|_| "cannot encode journal record")?;
    Ok(hex::encode(Sha256::digest(bytes)))
}
fn command_hash(command: &Command) -> Result<String, String> {
    Ok(hex::encode(Sha256::digest(
        serde_json::to_vec(command).map_err(|_| "cannot encode command")?,
    )))
}

impl Journal {
    /// A retained OS lock fences one host's processes on the canonical shared state directory.
    /// Corrupt or partial records refuse startup; acknowledgement follows append + sync_all.
    pub fn open(directory: &Path, max_bytes: u64, limits: Limits) -> Result<Self, String> {
        limits.core()?;
        std::fs::create_dir_all(directory).map_err(|_| "cannot create state directory")?;
        let directory =
            std::fs::canonicalize(directory).map_err(|_| "cannot resolve state directory")?;
        let fence = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(directory.join("engine.lock"))
            .map_err(|_| "cannot open process fence")?;
        fence
            .try_lock_exclusive()
            .map_err(|_| "another process owns this state directory")?;
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(directory.join("commands.jsonl"))
            .map_err(|_| "cannot open command journal")?;
        #[cfg(unix)]
        File::open(&directory)
            .and_then(|directory| directory.sync_all())
            .map_err(|_| "cannot sync state directory")?;
        let bytes = file
            .metadata()
            .map_err(|_| "cannot inspect command journal")?
            .len();
        if bytes > max_bytes {
            return Err("journal exceeds configured byte bound".into());
        }
        let mut sequence = 0;
        let mut previous_hash = "0".repeat(64);
        let mut bootstrap = None;
        let mut history = Vec::new();
        let mut commands = BTreeMap::new();
        let mut execution = Vec::new();
        let mut reader = BufReader::new(&file);
        let mut line = Vec::new();
        while (&mut reader)
            .take(65_537)
            .read_until(b'\n', &mut line)
            .map_err(|_| "cannot read journal")?
            != 0
        {
            if line.len() > 65_536 || !line.ends_with(b"\n") {
                return Err("partial or oversized journal record".into());
            }
            let record: Record =
                serde_json::from_slice(&line).map_err(|_| "corrupt journal record")?;
            if record.sequence != sequence
                || record.previous_hash != previous_hash
                || record.hash != digest(sequence, &previous_hash, &record.entry)?
            {
                return Err("journal sequence or hash chain mismatch".into());
            }
            match record.entry {
                Entry::Bootstrap { limits } if sequence == 0 => {
                    limits.core()?;
                    bootstrap = Some(limits);
                }
                Entry::Command {
                    command,
                    receipt,
                    operations,
                } if sequence > 0 => {
                    command.validate()?;
                    if history.len() >= 10_000
                        || operations.len() > 32
                        || command.id() != receipt.command_id
                        || receipt.applied_version != history.len() as u64 + 1
                        || command.expected_version() != history.len() as u64
                        || !["paused", "shadow", "killed"].contains(&receipt.mode_at_ack.as_str())
                        || commands.contains_key(command.id())
                    {
                        return Err("invalid or duplicate durable command".into());
                    }
                    commands.insert(
                        command.id().to_owned(),
                        (command_hash(&command)?, receipt.clone()),
                    );
                    history.push((command, receipt));
                    execution.extend(operations);
                }
                Entry::Execution { operations }
                    if sequence > 0 && !operations.is_empty() && operations.len() <= 32 =>
                {
                    execution.extend(operations)
                }
                _ => return Err("invalid journal bootstrap order".into()),
            }
            sequence += 1;
            previous_hash = record.hash;
            line.clear();
        }
        drop(reader);
        file.seek(SeekFrom::End(0))
            .map_err(|_| "cannot seek command journal")?;
        let mut journal = Self {
            _fence: fence,
            file,
            bytes,
            max_bytes,
            sequence,
            previous_hash,
            bootstrap: bootstrap.unwrap_or_else(|| limits.clone()),
            history,
            commands,
            execution,
        };
        if sequence == 0 {
            journal.append(Entry::Bootstrap { limits })?;
        }
        Ok(journal)
    }

    pub fn bootstrap(&self) -> &Limits {
        &self.bootstrap
    }
    pub fn history(&self) -> &[(Command, Receipt)] {
        &self.history
    }
    pub fn version(&self) -> u64 {
        self.history.len() as u64
    }
    pub fn bytes(&self) -> u64 {
        self.bytes
    }
    pub fn execution(&self) -> &[serde_json::Value] {
        &self.execution
    }
    pub fn commit_execution(&mut self, operations: Vec<serde_json::Value>) -> Result<(), String> {
        if operations.is_empty() || operations.len() > 32 {
            return Err("invalid execution transaction".into());
        }
        self.append(Entry::Execution {
            operations: operations.clone(),
        })?;
        self.execution.extend(operations);
        Ok(())
    }
    pub fn existing(&self, command: &Command) -> Result<Option<Receipt>, String> {
        match self.commands.get(command.id()) {
            Some((hash, receipt)) if hash == &command_hash(command)? => Ok(Some(receipt.clone())),
            Some(_) => Err("command id already exists with another payload".into()),
            None => Ok(None),
        }
    }
    pub fn commit(&mut self, command: Command, receipt: Receipt) -> Result<(), String> {
        self.commit_with_execution(command, receipt, Vec::new())
    }
    pub fn commit_with_execution(
        &mut self,
        command: Command,
        receipt: Receipt,
        operations: Vec<serde_json::Value>,
    ) -> Result<(), String> {
        command.validate()?;
        if self.commands.len() >= 10_000
            || self.existing(&command)?.is_some()
            || command.expected_version() != self.version()
            || receipt.applied_version != self.version() + 1
            || receipt.command_id != command.id()
            || operations.len() > 32
        {
            return Err("invalid journal command commit".into());
        }
        self.append(Entry::Command {
            command: command.clone(),
            receipt: receipt.clone(),
            operations: operations.clone(),
        })?;
        self.commands.insert(
            command.id().to_owned(),
            (command_hash(&command)?, receipt.clone()),
        );
        self.history.push((command, receipt));
        self.execution.extend(operations);
        Ok(())
    }
    fn append(&mut self, entry: Entry) -> Result<(), String> {
        let hash = digest(self.sequence, &self.previous_hash, &entry)?;
        let mut bytes = serde_json::to_vec(&Record {
            sequence: self.sequence,
            previous_hash: self.previous_hash.clone(),
            hash: hash.clone(),
            entry,
        })
        .map_err(|_| "cannot encode journal record")?;
        bytes.push(b'\n');
        if bytes.len() > 65_536 || self.bytes + bytes.len() as u64 > self.max_bytes {
            return Err("journal capacity reached; expand bound before restart".into());
        }
        self.file
            .write_all(&bytes)
            .and_then(|()| self.file.sync_all())
            .map_err(|_| "durable journal write failed; outcome must be reconciled on restart")?;
        self.bytes += bytes.len() as u64;
        self.sequence += 1;
        self.previous_hash = hash;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn limits() -> Limits {
        Limits {
            initial_collateral_usd: "1000".into(),
            max_imbalance_shares: "25".into(),
            lot_shares: "5".into(),
            half_width: "0.045".into(),
            max_feed_age_ms: 1000,
        }
    }
    #[test]
    fn acknowledged_command_survives_restart_and_reused_identity_conflicts() {
        let dir = tempfile::tempdir().unwrap();
        let command = Command::Control {
            id: "one".into(),
            expected_version: 0,
            action: "kill".into(),
        };
        let receipt = Receipt {
            command_id: "one".into(),
            applied_version: 1,
            mode_at_ack: "killed".into(),
        };
        let mut journal = Journal::open(dir.path(), 1_048_576, limits()).unwrap();
        assert!(Journal::open(dir.path(), 1_048_576, limits()).is_err());
        journal.commit(command.clone(), receipt.clone()).unwrap();
        drop(journal);
        let journal = Journal::open(dir.path(), 1_048_576, limits()).unwrap();
        assert_eq!(journal.version(), 1);
        assert!(journal.existing(&command).unwrap() == Some(receipt));
        assert!(
            journal
                .existing(&Command::Control {
                    id: "one".into(),
                    expected_version: 0,
                    action: "resume".into()
                })
                .is_err()
        );
    }
    #[test]
    fn torn_record_or_modified_hash_refuses_recovery() {
        let dir = tempfile::tempdir().unwrap();
        drop(Journal::open(dir.path(), 1_048_576, limits()).unwrap());
        let path = dir.path().join("commands.jsonl");
        let valid = std::fs::read(&path).unwrap();
        OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"{\"partial\":")
            .unwrap();
        assert!(Journal::open(dir.path(), 1_048_576, limits()).is_err());
        let changed = String::from_utf8(valid).unwrap().replace("1000", "1001");
        std::fs::write(&path, changed).unwrap();
        assert!(Journal::open(dir.path(), 1_048_576, limits()).is_err());
    }
    #[test]
    fn execution_transactions_do_not_advance_owner_cas_and_recover_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let mut journal = Journal::open(dir.path(), 1_048_576, limits()).unwrap();
        let first = serde_json::json!({"kind":"advance","now_ms":100});
        journal.commit_execution(vec![first.clone()]).unwrap();
        assert_eq!(journal.version(), 0);
        let command = Command::Control {
            id: "pause".into(),
            expected_version: 0,
            action: "pause".into(),
        };
        let second = serde_json::json!({"kind":"cancel_all","now_ms":200});
        journal
            .commit_with_execution(
                command,
                Receipt {
                    command_id: "pause".into(),
                    applied_version: 1,
                    mode_at_ack: "paused".into(),
                },
                vec![second.clone()],
            )
            .unwrap();
        journal
            .commit_execution(vec![serde_json::json!({"kind":"advance","now_ms":300})])
            .unwrap();
        assert_eq!(journal.version(), 1);
        drop(journal);
        let recovered = Journal::open(dir.path(), 1_048_576, limits()).unwrap();
        assert_eq!(recovered.version(), 1);
        assert_eq!(&recovered.execution()[..2], &[first, second]);
        assert_eq!(recovered.execution().len(), 3);
    }
}
