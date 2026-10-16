use crate::config::Limits;
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
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

/// All budgets include retained audit evidence. Rotation never deletes a record.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct JournalOptions {
    pub segment_max_bytes: u64,
    pub total_max_bytes: u64,
    pub max_segments: usize,
    pub max_commands: usize,
    pub max_execution_operations: usize,
}

impl Default for JournalOptions {
    fn default() -> Self {
        Self {
            segment_max_bytes: 8 * 1024 * 1024,
            total_max_bytes: 128 * 1024 * 1024,
            max_segments: 128,
            max_commands: 100_000,
            max_execution_operations: 250_000,
        }
    }
}

impl JournalOptions {
    pub fn validate(&self) -> Result<(), String> {
        if !(1024..=64 * 1024 * 1024).contains(&self.segment_max_bytes)
            || !(self.segment_max_bytes..=1024 * 1024 * 1024).contains(&self.total_max_bytes)
            || !(1..=4096).contains(&self.max_segments)
            || !(1..=1_000_000).contains(&self.max_commands)
            || !(1..=1_000_000).contains(&self.max_execution_operations)
        {
            return Err("journal options exceed supported finite resource bounds".into());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct JournalMetrics {
    pub total_bytes: u64,
    pub active_bytes: u64,
    pub manifest_bytes: u64,
    pub archive_segments: usize,
    pub record_count: u64,
    pub owner_commands: usize,
    pub execution_operations: usize,
    pub segment_max_bytes: u64,
    pub total_max_bytes: u64,
    pub max_segments: usize,
    pub max_commands: usize,
    pub max_execution_operations: usize,
    /// Safe research metadata only. None never authorizes reading a mutable tail.
    pub committed_prefix: Option<serde_json::Value>,
}

/// Append-only rotation intents are synced before renaming an acknowledged segment.
/// A complete last intent with its segment still active is safely finished on restart.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Seal {
    segment: usize,
    bytes: u64,
    next_sequence: u64,
    last_hash: String,
    hash: String,
}

impl Seal {
    fn digest(&self) -> Result<String, String> {
        Ok(hex::encode(Sha256::digest(
            serde_json::to_vec(&(
                self.segment,
                self.bytes,
                self.next_sequence,
                &self.last_hash,
            ))
            .map_err(|_| "cannot encode segment seal")?,
        )))
    }
}

pub struct Journal {
    _fence: File,
    directory: PathBuf,
    file: File,
    manifest: File,
    bytes: u64,
    active_bytes: u64,
    manifest_bytes: u64,
    archives: usize,
    options: JournalOptions,
    sequence: u64,
    previous_hash: String,
    bootstrap: Limits,
    history: Vec<(Command, Receipt)>,
    commands: BTreeMap<String, (String, Receipt)>,
    execution: Vec<serde_json::Value>,
    execution_operations: usize,
    retain_replay_buffer: bool,
    poisoned: bool,
    research_ledger_id: String,
    segment_start_sequence: u64,
    segment_previous_hash: String,
    prefix_confirmed: bool,
    #[cfg(test)]
    fail_sync_once: bool,
    #[cfg(test)]
    fail_manifest_sync_once: bool,
}

#[cfg(test)]
thread_local! {
    static FAIL_RECOVERY_MANIFEST_SYNC: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

fn confirm_recovered_manifest(manifest: &File) -> Result<(), String> {
    #[cfg(test)]
    if FAIL_RECOVERY_MANIFEST_SYNC.with(|failure| failure.replace(false)) {
        return Err("cannot confirm recovered journal manifest durability".into());
    }
    manifest
        .sync_all()
        .map_err(|_| "cannot confirm recovered journal manifest durability".into())
}

fn open_file(path: &Path) -> Result<File, String> {
    if path
        .symlink_metadata()
        .is_ok_and(|metadata| metadata.file_type().is_symlink())
    {
        return Err("journal paths must not be symbolic links".into());
    }
    OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)
        .map_err(|_| "cannot open durable journal file".into())
}

fn sync_directory(directory: &Path) -> Result<(), String> {
    #[cfg(unix)]
    File::open(directory)
        .and_then(|directory| directory.sync_all())
        .map_err(|_| "cannot sync state directory")?;
    #[cfg(not(unix))]
    let _ = directory;
    Ok(())
}

fn segment_name(index: usize) -> String {
    format!("commands-{index:08}.jsonl")
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
    #[cfg(test)]
    pub(crate) fn inject_sync_failure(&mut self) {
        self.fail_sync_once = true;
    }

    /// Compatibility entry point: the old byte setting is now the active segment size.
    /// The retained budget is explicit in metrics and remains finite (at most 1 GiB).
    pub fn open(directory: &Path, max_bytes: u64, limits: Limits) -> Result<Self, String> {
        Self::open_with_options(
            directory,
            JournalOptions {
                segment_max_bytes: max_bytes,
                total_max_bytes: max_bytes.saturating_mul(16).min(1024 * 1024 * 1024),
                ..JournalOptions::default()
            },
            limits,
        )
    }

    /// A retained OS lock fences one host's processes on the canonical shared directory.
    /// Partial records/seals and missing archives refuse startup. All old single-file
    /// journals remain byte-for-byte valid and rotate on their next complete append.
    pub fn open_with_options(
        directory: &Path,
        options: JournalOptions,
        limits: Limits,
    ) -> Result<Self, String> {
        Self::open_internal(directory, options, limits, true)
    }

    /// Validate retained evidence without constructing an in-memory execution tape.
    /// Recover execution state with visit_execution() after applying owner history.
    pub fn open_streaming(
        directory: &Path,
        options: JournalOptions,
        limits: Limits,
    ) -> Result<Self, String> {
        Self::open_internal(directory, options, limits, false)
    }

    fn open_internal(
        directory: &Path,
        options: JournalOptions,
        limits: Limits,
        retain_replay_buffer: bool,
    ) -> Result<Self, String> {
        limits.core()?;
        options.validate()?;
        std::fs::create_dir_all(directory).map_err(|_| "cannot create state directory")?;
        let directory =
            std::fs::canonicalize(directory).map_err(|_| "cannot resolve state directory")?;
        let fence = open_file(&directory.join("engine.lock"))?;
        fence
            .try_lock_exclusive()
            .map_err(|_| "another process owns this state directory")?;
        let mut manifest = open_file(&directory.join("journal-manifest.jsonl"))?;
        let manifest_bytes = manifest
            .metadata()
            .map_err(|_| "cannot inspect journal manifest")?
            .len();
        if manifest_bytes > 2 * 1024 * 1024 || manifest_bytes > options.total_max_bytes {
            return Err("journal manifest exceeds configured bound".into());
        }
        let mut seals = Vec::new();
        let mut reader = BufReader::new(&manifest);
        let mut line = Vec::new();
        while (&mut reader)
            .take(1025)
            .read_until(b'\n', &mut line)
            .map_err(|_| "cannot read journal manifest")?
            != 0
        {
            if line.len() > 1024 || !line.ends_with(b"\n") {
                return Err("partial or oversized journal segment seal".into());
            }
            let seal: Seal =
                serde_json::from_slice(&line).map_err(|_| "corrupt journal segment seal")?;
            if seal.segment != seals.len()
                || seals.len() >= options.max_segments
                || seal.bytes == 0
                || seal.next_sequence == 0
                || seal.last_hash.len() != 64
                || seal.hash != seal.digest()?
            {
                return Err("invalid journal segment seal".into());
            }
            seals.push(seal);
            line.clear();
        }
        drop(reader);
        if manifest_bytes != 0 {
            // Complete recovered seals can include bytes from a lost/failed sync. The
            // manifest descriptor must confirm its rename intents before any archive
            // recovery. A later sync of a different active-file descriptor cannot do so.
            confirm_recovered_manifest(&manifest)?;
        }
        manifest
            .seek(SeekFrom::End(0))
            .map_err(|_| "cannot seek journal manifest")?;
        // Reject extra or misnamed evidence; accepting a prefix could forget acknowledged work.
        for entry in std::fs::read_dir(&directory).map_err(|_| "cannot list state directory")? {
            let entry = entry.map_err(|_| "cannot inspect state directory entry")?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with("commands-") {
                let found = (0..seals.len()).any(|index| name == segment_name(index));
                if !found
                    || !entry
                        .file_type()
                        .map_err(|_| "cannot inspect segment")?
                        .is_file()
                {
                    return Err("unsealed, unexpected or linked journal archive".into());
                }
            }
        }
        let active_path = directory.join("commands.jsonl");
        let mut journal = Self {
            _fence: fence,
            directory: directory.clone(),
            file: open_file(&active_path)?,
            manifest,
            bytes: 0,
            active_bytes: 0,
            manifest_bytes,
            archives: 0,
            options,
            sequence: 0,
            previous_hash: "0".repeat(64),
            bootstrap: limits.clone(),
            history: Vec::new(),
            commands: BTreeMap::new(),
            execution: Vec::new(),
            execution_operations: 0,
            retain_replay_buffer,
            poisoned: false,
            research_ledger_id: hex::encode(Sha256::digest(
                format!("mm2-engine-ledger-v1:{}", directory.display()).as_bytes(),
            )),
            segment_start_sequence: 0,
            segment_previous_hash: "0".repeat(64),
            // Startup replay can resolve a complete record with a lost sync acknowledgement.
            // A subsequent normal commit confirms that prefix without an analytics-only fsync.
            prefix_confirmed: false,
            #[cfg(test)]
            fail_sync_once: false,
            #[cfg(test)]
            fail_manifest_sync_once: false,
        };
        for seal in &seals {
            let path = directory.join(segment_name(seal.segment));
            if !path.exists() {
                // An fsynced last seal is the rename intent. Only the matching complete
                // active segment can finish it; missing historical evidence is not guessed.
                if seal.segment + 1 != seals.len() {
                    return Err("journal archive is missing".into());
                }
                journal.read_records(&active_path)?;
                if journal.active_bytes != seal.bytes
                    || journal.sequence != seal.next_sequence
                    || journal.previous_hash != seal.last_hash
                {
                    return Err("incomplete journal rotation cannot be reconciled".into());
                }
                std::fs::rename(&active_path, &path)
                    .map_err(|_| "cannot finish journal rotation")?;
                sync_directory(&directory)?;
                journal.file = open_file(&active_path)?;
                sync_directory(&directory)?;
            } else {
                journal.read_records(&path)?;
                if journal.active_bytes != seal.bytes
                    || journal.sequence != seal.next_sequence
                    || journal.previous_hash != seal.last_hash
                {
                    return Err("journal archive does not match its durable seal".into());
                }
            }
            journal.archives += 1;
            journal.active_bytes = 0;
            journal.segment_start_sequence = journal.sequence;
            journal.segment_previous_hash = journal.previous_hash.clone();
        }
        journal.read_records(&active_path)?;
        if journal.bytes() > journal.options.total_max_bytes {
            return Err("retained journal exceeds configured total byte bound".into());
        }
        journal
            .file
            .seek(SeekFrom::End(0))
            .map_err(|_| "cannot seek journal")?;
        sync_directory(&directory)?;
        if journal.sequence == 0 {
            journal.append(Entry::Bootstrap { limits })?;
        }
        Ok(journal)
    }

    fn read_records(&mut self, path: &Path) -> Result<(), String> {
        if path
            .symlink_metadata()
            .map_err(|_| "cannot inspect journal segment")?
            .file_type()
            .is_symlink()
        {
            return Err("journal segment must not be a symbolic link".into());
        }
        let file = File::open(path).map_err(|_| "cannot read journal segment")?;
        let length = file
            .metadata()
            .map_err(|_| "cannot inspect journal segment")?
            .len();
        if self.bytes().saturating_add(length) > self.options.total_max_bytes {
            return Err("retained journal exceeds configured total byte bound".into());
        }
        let mut reader = BufReader::new(file);
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
            if record.sequence != self.sequence
                || record.previous_hash != self.previous_hash
                || record.hash != digest(self.sequence, &self.previous_hash, &record.entry)?
            {
                return Err("journal sequence or hash chain mismatch".into());
            }
            match record.entry {
                Entry::Bootstrap { limits } if self.sequence == 0 => {
                    limits.core()?;
                    self.bootstrap = limits;
                }
                Entry::Command {
                    command,
                    receipt,
                    operations,
                } if self.sequence > 0 => {
                    command.validate()?;
                    if self.history.len() >= self.options.max_commands
                        || operations.len() > 32
                        || command.id() != receipt.command_id
                        || receipt.applied_version != self.history.len() as u64 + 1
                        || command.expected_version() != self.history.len() as u64
                        || ![
                            "paused",
                            "shadow",
                            "killed",
                            "simulation",
                            "trading",
                            "both",
                            "live",
                        ]
                        .contains(&receipt.mode_at_ack.as_str())
                        || self.commands.contains_key(command.id())
                    {
                        return Err("invalid or duplicate durable command".into());
                    }
                    self.add_operations(operations)?;
                    self.commands.insert(
                        command.id().to_owned(),
                        (command_hash(&command)?, receipt.clone()),
                    );
                    self.history.push((command, receipt));
                }
                Entry::Execution { operations }
                    if self.sequence > 0 && !operations.is_empty() && operations.len() <= 32 =>
                {
                    self.add_operations(operations)?;
                }
                _ => return Err("invalid journal bootstrap order".into()),
            }
            self.sequence += 1;
            self.previous_hash = record.hash;
            line.clear();
        }
        self.bytes += length;
        self.active_bytes = length;
        Ok(())
    }

    fn add_operations(&mut self, operations: Vec<serde_json::Value>) -> Result<(), String> {
        if self.execution_operations.saturating_add(operations.len())
            > self.options.max_execution_operations
        {
            return Err("retained execution operation capacity reached".into());
        }
        self.execution_operations += operations.len();
        if self.retain_replay_buffer {
            self.execution.extend(operations);
        }
        Ok(())
    }

    /// Call only after recovery has consumed execution(). Future records remain on disk.
    /// This frees the startup tape and stops duplicating the audit ledger in actor memory.
    pub fn release_replay_buffer(&mut self) {
        self.execution = Vec::new();
        self.retain_replay_buffer = false;
    }

    /// Ordered, bounded recovery callback. The process fence is retained throughout.
    /// Each ≤64 KiB record is decoded once for this pass; the complete chain is checked
    /// again before accepting the recovered state. Callbacks must not perform IO actions.
    pub fn visit_execution(
        &self,
        mut visitor: impl FnMut(&[serde_json::Value]) -> Result<(), String>,
    ) -> Result<(), String> {
        let mut sequence = 0;
        let mut previous = "0".repeat(64);
        let paths = (0..self.archives)
            .map(|index| self.directory.join(segment_name(index)))
            .chain(std::iter::once(self.directory.join("commands.jsonl")));
        for path in paths {
            if path
                .symlink_metadata()
                .map_err(|_| "cannot inspect replay segment")?
                .file_type()
                .is_symlink()
            {
                return Err("replay segment must not be linked".into());
            }
            let mut reader =
                BufReader::new(File::open(path).map_err(|_| "cannot open replay segment")?);
            let mut line = Vec::new();
            while (&mut reader)
                .take(65_537)
                .read_until(b'\n', &mut line)
                .map_err(|_| "cannot read replay segment")?
                != 0
            {
                if line.len() > 65_536 || !line.ends_with(b"\n") {
                    return Err("partial or oversized replay record".into());
                }
                let record: Record =
                    serde_json::from_slice(&line).map_err(|_| "corrupt replay record")?;
                if record.sequence != sequence
                    || record.previous_hash != previous
                    || record.hash != digest(sequence, &previous, &record.entry)?
                {
                    return Err("replay sequence or hash chain mismatch".into());
                }
                match &record.entry {
                    Entry::Bootstrap { .. } if sequence == 0 => {}
                    Entry::Execution { operations } | Entry::Command { operations, .. }
                        if sequence > 0 =>
                    {
                        visitor(operations)?
                    }
                    _ => return Err("invalid replay bootstrap order".into()),
                }
                sequence += 1;
                previous = record.hash;
                line.clear();
            }
        }
        if sequence != self.sequence || previous != self.previous_hash {
            return Err("retained evidence changed during replay".into());
        }
        Ok(())
    }

    pub fn metrics(&self) -> JournalMetrics {
        JournalMetrics {
            total_bytes: self.bytes(),
            active_bytes: self.active_bytes,
            manifest_bytes: self.manifest_bytes,
            archive_segments: self.archives,
            record_count: self.sequence,
            owner_commands: self.history.len(),
            execution_operations: self.execution_operations,
            segment_max_bytes: self.options.segment_max_bytes,
            total_max_bytes: self.options.total_max_bytes,
            max_segments: self.options.max_segments,
            max_commands: self.options.max_commands,
            max_execution_operations: self.options.max_execution_operations,
            committed_prefix: self.committed_prefix(),
        }
    }

    /// Atomic actor-owned boundary, published only after an existing successful durable append.
    /// The directory fingerprint is a source locator, not account authentication or a global ID.
    pub fn committed_prefix(&self) -> Option<serde_json::Value> {
        if self.poisoned || !self.prefix_confirmed {
            return None;
        }
        Some(serde_json::json!({
            "schema_version": 1,
            "source_kind": "engine",
            "ledger_id": self.research_ledger_id,
            "segment": {
                "name": "commands.jsonl", "index": self.archives,
                "start_sequence": self.segment_start_sequence,
                "previous_hash": self.segment_previous_hash,
            },
            "byte_offset": self.active_bytes,
            "next_sequence": self.sequence,
            "last_hash": self.previous_hash,
            "manifest_bytes": self.manifest_bytes,
        }))
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
        self.bytes + self.manifest_bytes
    }
    pub fn execution(&self) -> &[serde_json::Value] {
        &self.execution
    }
    pub fn commit_execution(&mut self, operations: Vec<serde_json::Value>) -> Result<(), String> {
        if operations.is_empty() || operations.len() > 32 {
            return Err("invalid execution transaction".into());
        }
        if self.execution_operations.saturating_add(operations.len())
            > self.options.max_execution_operations
        {
            return Err("retained execution operation capacity reached".into());
        }
        self.append(Entry::Execution {
            operations: operations.clone(),
        })?;
        self.add_operations(operations)
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
        if self.commands.len() >= self.options.max_commands
            || self.existing(&command)?.is_some()
            || command.expected_version() != self.version()
            || receipt.applied_version != self.version() + 1
            || receipt.command_id != command.id()
            || operations.len() > 32
            || self.execution_operations.saturating_add(operations.len())
                > self.options.max_execution_operations
            || ![
                "paused",
                "shadow",
                "killed",
                "simulation",
                "trading",
                "both",
                "live",
            ]
            .contains(&receipt.mode_at_ack.as_str())
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
        self.add_operations(operations)
    }
    fn append(&mut self, entry: Entry) -> Result<(), String> {
        if self.poisoned {
            return Err("journal writer failed; restart and reconcile before retry".into());
        }
        let hash = digest(self.sequence, &self.previous_hash, &entry)?;
        let mut bytes = serde_json::to_vec(&Record {
            sequence: self.sequence,
            previous_hash: self.previous_hash.clone(),
            hash: hash.clone(),
            entry,
        })
        .map_err(|_| "cannot encode journal record")?;
        bytes.push(b'\n');
        if bytes.len() > 65_536 || bytes.len() as u64 > self.options.segment_max_bytes {
            return Err("journal record exceeds segment or record bound".into());
        }
        let rotation = self.active_bytes + bytes.len() as u64 > self.options.segment_max_bytes;
        let seal_bytes = if rotation {
            if self.archives >= self.options.max_segments {
                return Err(
                    "journal archive segment capacity reached; retained evidence was preserved"
                        .into(),
                );
            }
            let mut seal = Seal {
                segment: self.archives,
                bytes: self.active_bytes,
                next_sequence: self.sequence,
                last_hash: self.previous_hash.clone(),
                hash: String::new(),
            };
            seal.hash = seal.digest()?;
            let mut encoded =
                serde_json::to_vec(&seal).map_err(|_| "cannot encode segment seal")?;
            encoded.push(b'\n');
            Some(encoded)
        } else {
            None
        };
        let added = bytes.len() as u64 + seal_bytes.as_ref().map_or(0, |seal| seal.len() as u64);
        if self.bytes().saturating_add(added) > self.options.total_max_bytes {
            return Err("retained journal capacity reached; evidence was preserved".into());
        }
        if let Some(seal_bytes) = seal_bytes
            && self.rotate(&seal_bytes).is_err()
        {
            self.poisoned = true;
            return Err("durable journal rotation failed; reconcile on restart".into());
        }
        self.poisoned = true;
        let written = self.file.write_all(&bytes);
        #[cfg(test)]
        if written.is_ok() && self.fail_sync_once {
            self.fail_sync_once = false;
            return Err("injected journal fsync failure after complete write".into());
        }
        if written.and_then(|()| self.file.sync_all()).is_err() {
            self.poisoned = true;
            return Err(
                "durable journal write failed; outcome must be reconciled on restart".into(),
            );
        }
        self.bytes += bytes.len() as u64;
        self.active_bytes += bytes.len() as u64;
        self.sequence += 1;
        self.previous_hash = hash;
        self.prefix_confirmed = true;
        self.poisoned = false;
        Ok(())
    }

    fn rotate(&mut self, seal_bytes: &[u8]) -> Result<(), String> {
        self.file
            .sync_all()
            .map_err(|_| "cannot sync active journal")?;
        // The seal is a durable rename intent. Never expose a new writable segment
        // until both intent and archived directory entry are durable.
        self.manifest
            .write_all(seal_bytes)
            .map_err(|_| "cannot persist segment seal")?;
        #[cfg(test)]
        if self.fail_manifest_sync_once {
            self.fail_manifest_sync_once = false;
            return Err("injected manifest fsync failure after complete seal write".into());
        }
        self.manifest
            .sync_all()
            .map_err(|_| "cannot persist segment seal")?;
        self.manifest_bytes += seal_bytes.len() as u64;
        let target = self.directory.join(segment_name(self.archives));
        if target.exists() {
            return Err("journal archive path already exists".into());
        }
        std::fs::rename(self.directory.join("commands.jsonl"), target)
            .map_err(|_| "cannot archive journal segment")?;
        sync_directory(&self.directory)?;
        self.file = open_file(&self.directory.join("commands.jsonl"))?;
        sync_directory(&self.directory)?;
        self.archives += 1;
        self.active_bytes = 0;
        self.segment_start_sequence = self.sequence;
        self.segment_previous_hash = self.previous_hash.clone();
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

    fn small_options() -> JournalOptions {
        JournalOptions {
            segment_max_bytes: 1024,
            total_max_bytes: 64 * 1024,
            max_segments: 32,
            max_commands: 100,
            max_execution_operations: 100,
        }
    }

    fn append_steps(journal: &mut Journal, count: u64) {
        for now in 0..count {
            journal
                .commit_execution(vec![serde_json::json!({"kind":"advance","now_ms":now})])
                .unwrap();
        }
    }

    #[test]
    fn research_prefix_tracks_only_synced_bytes_and_rotated_segment_anchor() {
        let dir = tempfile::tempdir().unwrap();
        let mut journal =
            Journal::open_with_options(dir.path(), small_options(), limits()).unwrap();
        let initial = journal.committed_prefix().unwrap();
        assert_eq!(initial["next_sequence"], 1);
        append_steps(&mut journal, 12);
        let prefix = journal.committed_prefix().unwrap();
        assert!(prefix["segment"]["index"].as_u64().unwrap() > 0);
        assert_eq!(prefix["byte_offset"], journal.active_bytes);
        assert_eq!(prefix["last_hash"], journal.previous_hash);
        assert_eq!(
            prefix["segment"]["start_sequence"],
            journal.segment_start_sequence
        );
        drop(journal);
        let mut recovered =
            Journal::open_with_options(dir.path(), small_options(), limits()).unwrap();
        assert!(recovered.committed_prefix().is_none());
        append_steps(&mut recovered, 1);
        assert!(recovered.committed_prefix().is_some());
    }

    #[test]
    fn research_prefix_is_withheld_after_complete_write_with_failed_sync() {
        let dir = tempfile::tempdir().unwrap();
        let mut journal = Journal::open(dir.path(), 1_048_576, limits()).unwrap();
        let prefix = journal.committed_prefix().unwrap();
        journal.fail_sync_once = true;
        assert!(
            journal
                .commit_execution(vec![serde_json::json!({"kind":"advance","now_ms":7})])
                .is_err()
        );
        assert!(journal.committed_prefix().is_none());
        assert_eq!(journal.sequence, prefix["next_sequence"].as_u64().unwrap());
        assert!(
            std::fs::metadata(dir.path().join("commands.jsonl"))
                .unwrap()
                .len()
                > prefix["byte_offset"].as_u64().unwrap()
        );
        assert!(
            journal
                .commit_execution(vec![serde_json::json!({"kind":"advance","now_ms":8})])
                .is_err()
        );
    }

    #[test]
    fn rotation_preserves_owner_identity_and_execution_order_across_every_segment() {
        let dir = tempfile::tempdir().unwrap();
        let mut journal =
            Journal::open_with_options(dir.path(), small_options(), limits()).unwrap();
        let command = Command::Control {
            id: "durable-owner".into(),
            expected_version: 0,
            action: "pause".into(),
        };
        let receipt = Receipt {
            command_id: command.id().into(),
            applied_version: 1,
            mode_at_ack: "paused".into(),
        };
        journal.commit(command.clone(), receipt.clone()).unwrap();
        append_steps(&mut journal, 12);
        let before = journal.metrics();
        assert!(before.archive_segments >= 3);
        assert!(before.total_bytes > before.segment_max_bytes);
        drop(journal);
        let recovered = Journal::open_with_options(dir.path(), small_options(), limits()).unwrap();
        assert_eq!(recovered.existing(&command).unwrap(), Some(receipt));
        assert_eq!(recovered.metrics().total_bytes, before.total_bytes);
        assert_eq!(recovered.execution().len(), 12);
        for (now, operation) in recovered.execution().iter().enumerate() {
            assert_eq!(operation["now_ms"], now as u64);
        }
    }

    #[test]
    fn old_single_file_migrates_without_rewriting_acknowledged_evidence() {
        let dir = tempfile::tempdir().unwrap();
        let mut journal = Journal::open(dir.path(), 1024 * 1024, limits()).unwrap();
        append_steps(&mut journal, 3);
        drop(journal);
        let original = std::fs::read(dir.path().join("commands.jsonl")).unwrap();
        std::fs::remove_file(dir.path().join("journal-manifest.jsonl")).unwrap();
        let mut journal =
            Journal::open_with_options(dir.path(), small_options(), limits()).unwrap();
        append_steps(&mut journal, 1);
        assert_eq!(
            std::fs::read(dir.path().join(segment_name(0))).unwrap(),
            original
        );
        assert_eq!(journal.execution().len(), 4);
    }

    #[test]
    fn complete_rotation_intent_recovers_both_crash_boundaries() {
        for rename_before_crash in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut journal =
                Journal::open_with_options(dir.path(), small_options(), limits()).unwrap();
            append_steps(&mut journal, 1);
            let mut seal = Seal {
                segment: 0,
                bytes: journal.active_bytes,
                next_sequence: journal.sequence,
                last_hash: journal.previous_hash.clone(),
                hash: String::new(),
            };
            seal.hash = seal.digest().unwrap();
            let mut encoded = serde_json::to_vec(&seal).unwrap();
            encoded.push(b'\n');
            journal.manifest.write_all(&encoded).unwrap();
            journal.manifest.sync_all().unwrap();
            drop(journal);
            if rename_before_crash {
                std::fs::rename(
                    dir.path().join("commands.jsonl"),
                    dir.path().join(segment_name(0)),
                )
                .unwrap();
            }
            let recovered =
                Journal::open_with_options(dir.path(), small_options(), limits()).unwrap();
            assert_eq!(recovered.execution().len(), 1);
            assert_eq!(recovered.metrics().archive_segments, 1);
            assert!(dir.path().join(segment_name(0)).exists());
        }
    }

    #[test]
    fn complete_seal_with_failed_sync_requires_manifest_confirmation_before_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let mut journal =
            Journal::open_with_options(dir.path(), small_options(), limits()).unwrap();
        journal.fail_manifest_sync_once = true;
        let mut committed_operations = 0;
        let mut failed = false;
        for step in 0..64 {
            let operation = vec![serde_json::json!({"kind":"advance","now_ms":step})];
            if journal.commit_execution(operation).is_err() {
                failed = true;
                break;
            }
            committed_operations += 1;
        }
        assert!(failed);
        assert!(journal.poisoned);
        assert!(journal.committed_prefix().is_none());
        assert_eq!(journal.archives, 0);
        assert_eq!(journal.manifest_bytes, 0);
        let sequence = journal.sequence;
        let active = std::fs::read(dir.path().join("commands.jsonl")).unwrap();
        let manifest = std::fs::read(dir.path().join("journal-manifest.jsonl")).unwrap();
        assert!(!manifest.is_empty());
        drop(journal);

        FAIL_RECOVERY_MANIFEST_SYNC.with(|failure| failure.set(true));
        let failed_recovery = Journal::open_with_options(dir.path(), small_options(), limits());
        assert!(
            matches!(failed_recovery, Err(reason) if reason == "cannot confirm recovered journal manifest durability")
        );
        assert_eq!(
            std::fs::read(dir.path().join("commands.jsonl")).unwrap(),
            active
        );
        assert_eq!(
            std::fs::read(dir.path().join("journal-manifest.jsonl")).unwrap(),
            manifest
        );
        assert!(!dir.path().join(segment_name(0)).exists());

        let mut recovered =
            Journal::open_with_options(dir.path(), small_options(), limits()).unwrap();
        assert_eq!(recovered.sequence, sequence);
        assert_eq!(recovered.execution().len(), committed_operations);
        assert_eq!(recovered.archives, 1);
        assert!(recovered.committed_prefix().is_none());
        recovered
            .commit_execution(vec![serde_json::json!({"kind":"advance","now_ms":100})])
            .unwrap();
        let prefix = recovered.committed_prefix().unwrap();
        assert_eq!(prefix["next_sequence"], sequence + 1);
        assert_eq!(prefix["manifest_bytes"], manifest.len() as u64);
        assert_eq!(prefix["segment"]["index"], 1);
        assert_eq!(recovered.execution().len(), committed_operations + 1);
    }

    #[test]
    fn missing_archive_or_partial_manifest_or_archive_refuses_recovery() {
        for fault in ["missing", "manifest", "archive"] {
            let dir = tempfile::tempdir().unwrap();
            let mut journal =
                Journal::open_with_options(dir.path(), small_options(), limits()).unwrap();
            append_steps(&mut journal, 8);
            drop(journal);
            let archive = dir.path().join(segment_name(0));
            match fault {
                "missing" => std::fs::remove_file(archive).unwrap(),
                "manifest" => {
                    OpenOptions::new()
                        .append(true)
                        .open(dir.path().join("journal-manifest.jsonl"))
                        .unwrap()
                        .write_all(b"{\"partial\":")
                        .unwrap();
                }
                _ => {
                    let length = std::fs::metadata(&archive).unwrap().len();
                    OpenOptions::new()
                        .write(true)
                        .open(archive)
                        .unwrap()
                        .set_len(length - 1)
                        .unwrap();
                }
            }
            assert!(Journal::open_with_options(dir.path(), small_options(), limits()).is_err());
        }
    }

    #[test]
    fn retained_budget_and_command_budget_stop_without_discarding_receipts() {
        let dir = tempfile::tempdir().unwrap();
        let mut options = small_options();
        options.total_max_bytes = 2048;
        options.max_commands = 1;
        let mut journal =
            Journal::open_with_options(dir.path(), options.clone(), limits()).unwrap();
        let command = Command::Control {
            id: "one".into(),
            expected_version: 0,
            action: "pause".into(),
        };
        let receipt = Receipt {
            command_id: "one".into(),
            applied_version: 1,
            mode_at_ack: "paused".into(),
        };
        journal.commit(command.clone(), receipt.clone()).unwrap();
        assert!(
            journal
                .commit(
                    Command::Control {
                        id: "two".into(),
                        expected_version: 1,
                        action: "kill".into()
                    },
                    Receipt {
                        command_id: "two".into(),
                        applied_version: 2,
                        mode_at_ack: "killed".into()
                    },
                )
                .is_err()
        );
        while journal
            .commit_execution(vec![serde_json::json!({"kind":"advance","now_ms":1})])
            .is_ok()
        {}
        let bytes = journal.bytes();
        assert!(bytes <= options.total_max_bytes);
        drop(journal);
        let recovered = Journal::open_with_options(dir.path(), options, limits()).unwrap();
        assert_eq!(recovered.bytes(), bytes);
        assert_eq!(recovered.existing(&command).unwrap(), Some(receipt));
    }

    #[test]
    fn replay_buffer_release_bounds_actor_memory_but_preserves_restart_audit() {
        let dir = tempfile::tempdir().unwrap();
        let mut options = small_options();
        options.max_execution_operations = 3;
        let mut journal =
            Journal::open_with_options(dir.path(), options.clone(), limits()).unwrap();
        append_steps(&mut journal, 2);
        journal.release_replay_buffer();
        append_steps(&mut journal, 1);
        assert!(journal.execution().is_empty());
        assert_eq!(journal.metrics().execution_operations, 3);
        assert!(
            journal
                .commit_execution(vec![serde_json::json!({"kind":"advance","now_ms":4})])
                .is_err()
        );
        drop(journal);
        let recovered = Journal::open_with_options(dir.path(), options, limits()).unwrap();
        assert_eq!(recovered.execution().len(), 3);
    }

    #[test]
    fn streaming_recovery_visits_bounded_transactions_without_building_tape() {
        let dir = tempfile::tempdir().unwrap();
        let mut journal =
            Journal::open_with_options(dir.path(), small_options(), limits()).unwrap();
        append_steps(&mut journal, 12);
        drop(journal);
        let mut recovered = Journal::open_streaming(dir.path(), small_options(), limits()).unwrap();
        assert!(recovered.execution().is_empty());
        let mut now = 0;
        recovered
            .visit_execution(|operations| {
                for operation in operations {
                    assert_eq!(operation["now_ms"], now);
                    now += 1;
                }
                Ok(())
            })
            .unwrap();
        assert_eq!(now, 12);
        append_steps(&mut recovered, 1);
        assert!(recovered.execution().is_empty());
        assert_eq!(recovered.metrics().execution_operations, 13);
    }
}
