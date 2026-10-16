use crate::{model::Ledger, settings::LiveSettings};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Read, Write},
    path::{Path, PathBuf},
};

const MAX_RECORD: usize = 1_048_576;
const SEGMENT_BYTES: u64 = 8 * 1024 * 1024;
#[derive(Serialize, Deserialize)]
struct Record {
    sequence: u64,
    previous_hash: String,
    hash: String,
    reason: String,
    ledger: Ledger,
}
fn digest(
    sequence: u64,
    previous_hash: &str,
    reason: &str,
    ledger: &Ledger,
) -> Result<String, String> {
    let bytes = serde_json::to_vec(&(sequence, previous_hash, reason, ledger))
        .map_err(|_| "cannot encode live ledger")?;
    Ok(hex::encode(Sha256::digest(bytes)))
}
pub(crate) struct Journal {
    _lock: File,
    file: File,
    directory: PathBuf,
    pub sequence: u64,
    hash: String,
    pub bytes: u64,
    segment_bytes: u64,
    maximum: u64,
    poisoned: bool,
    research_ledger_id: String,
    account_identity: String,
    segment_start_sequence: u64,
    segment_previous_hash: String,
    prefix_confirmed: bool,
    #[cfg(test)]
    fail_sync_once: bool,
}
impl Journal {
    pub fn open(directory: &Path, settings: &LiveSettings) -> Result<(Self, Ledger), String> {
        std::fs::create_dir_all(directory).map_err(|_| "cannot create live state directory")?;
        let directory =
            std::fs::canonicalize(directory).map_err(|_| "cannot resolve live state directory")?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(directory.join("live.lock"))
            .map_err(|_| "cannot open live fence")?;
        lock.try_lock_exclusive()
            .map_err(|_| "another executor owns this live state directory")?;
        // Do not allow replay against a different account or deployment. Policy may change on restart.
        let identity = serde_json::to_vec(&(
            &settings.clob_url,
            &settings.owner_address,
            &settings.funder_address,
            &settings.signer_address,
            settings.signature_type,
            settings.chain_id,
            settings.protocol,
            &settings.exchange_address,
            &settings.collateral_address,
        ))
        .map_err(|_| "cannot encode public live identity")?;
        let identity = hex::encode(Sha256::digest(identity));
        let identity_path = directory.join("live-identity.json");
        if identity_path.exists() {
            let actual =
                std::fs::read_to_string(&identity_path).map_err(|_| "cannot read live identity")?;
            if actual != identity {
                return Err("live account/deployment/settings identity mismatch".into());
            }
        } else {
            if std::fs::read_dir(&directory)
                .map_err(|_| "cannot inspect live identity directory")?
                .filter_map(Result::ok)
                .any(|entry| entry.file_name().to_string_lossy().ends_with(".jsonl"))
            {
                return Err("live identity missing from retained journal".into());
            }
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(identity_path)
                .map_err(|_| "cannot create live identity")?;
            file.write_all(identity.as_bytes())
                .and_then(|()| file.sync_all())
                .map_err(|_| "cannot persist live identity")?;
        }
        let mut paths = Vec::new();
        for entry in std::fs::read_dir(&directory).map_err(|_| "cannot list live journal")? {
            let entry = entry.map_err(|_| "cannot inspect live journal")?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with("live-") && name.ends_with(".jsonl") {
                paths.push(entry.path());
            }
        }
        paths.sort();
        if paths.len() > 256 {
            return Err("live segment count exceeds bounded retention".into());
        }
        let mut sequence = 0;
        let mut hash = "0".repeat(64);
        let mut ledger = Ledger::default();
        let mut bytes = 0u64;
        for (index, path) in paths.iter().enumerate() {
            let start = path
                .file_name()
                .and_then(|n| n.to_str())
                .and_then(|n| n.strip_prefix("live-"))
                .and_then(|n| n.strip_suffix(".jsonl"))
                .filter(|n| n.len() == 16)
                .and_then(|n| n.parse::<u64>().ok())
                .ok_or("noncanonical live segment name")?;
            if start != sequence {
                return Err("live segment starting sequence mismatch".into());
            }
            let file = File::open(path).map_err(|_| "cannot read live journal segment")?;
            let length = file
                .metadata()
                .map_err(|_| "cannot stat live journal")?
                .len();
            if length == 0 && index + 1 != paths.len() {
                return Err("empty nonfinal live journal segment".into());
            }
            bytes = bytes
                .checked_add(length)
                .ok_or("live journal size overflow")?;
            if bytes > settings.journal_max_bytes {
                return Err("live journal capacity exceeded".into());
            }
            let mut reader = BufReader::new(file);
            loop {
                let mut line = Vec::new();
                let n = Read::by_ref(&mut reader)
                    .take(MAX_RECORD as u64 + 1)
                    .read_until(b'\n', &mut line)
                    .map_err(|_| "cannot read live journal record")?;
                if n == 0 {
                    break;
                }
                if line.len() > MAX_RECORD || line.last() != Some(&b'\n') {
                    return Err("partial/oversized live journal record".into());
                }
                let record: Record =
                    serde_json::from_slice(&line).map_err(|_| "corrupt live journal record")?;
                if record.sequence != sequence
                    || record.previous_hash != hash
                    || record.hash != digest(sequence, &hash, &record.reason, &record.ledger)?
                {
                    return Err("live journal sequence/hash mismatch".into());
                }
                validate_ledger(&record.ledger, settings)?;
                sequence += 1;
                hash = record.hash;
                ledger = record.ledger;
            }
        }
        let segment_start_sequence = paths
            .last()
            .and_then(|path| path.file_stem())
            .and_then(|name| name.to_str())
            .and_then(|name| name.strip_prefix("live-"))
            .and_then(|start| start.parse::<u64>().ok())
            .unwrap_or(sequence);
        // Keep the segment's predecessor anchor without rereading any file on publication.
        let segment_previous_hash = if let Some(path) = paths.last() {
            let mut first = Vec::new();
            let mut reader =
                BufReader::new(File::open(path).map_err(|_| "cannot read live segment anchor")?);
            Read::by_ref(&mut reader)
                .take(MAX_RECORD as u64 + 1)
                .read_until(b'\n', &mut first)
                .map_err(|_| "cannot read live segment anchor")?;
            if first.is_empty() {
                hash.clone()
            } else {
                serde_json::from_slice::<Record>(&first)
                    .map_err(|_| "corrupt live segment anchor")?
                    .previous_hash
            }
        } else {
            hash.clone()
        };
        let path = paths
            .last()
            .cloned()
            .unwrap_or_else(|| directory.join(format!("live-{sequence:016}.jsonl")));
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .append(true)
            .read(true)
            .open(path)
            .map_err(|_| "cannot open live journal segment")?;
        let segment_bytes = file
            .metadata()
            .map_err(|_| "cannot stat live segment")?
            .len();
        sync_directory(&directory)?;
        Ok((
            Self {
                _lock: lock,
                file,
                directory: directory.clone(),
                sequence,
                hash,
                bytes,
                segment_bytes,
                maximum: settings.journal_max_bytes,
                poisoned: false,
                research_ledger_id: hex::encode(Sha256::digest(
                    format!("mm2-live-ledger-v1:{}", directory.display()).as_bytes(),
                )),
                account_identity: identity,
                segment_start_sequence,
                segment_previous_hash,
                prefix_confirmed: false,
                #[cfg(test)]
                fail_sync_once: false,
            },
            ledger,
        ))
    }
    pub fn commit(&mut self, reason: &str, ledger: &Ledger) -> Result<(), String> {
        if self.poisoned {
            return Err("live journal write outcome unknown; checked restart required".into());
        }
        let hash = digest(self.sequence, &self.hash, reason, ledger)?;
        let mut bytes = serde_json::to_vec(&Record {
            sequence: self.sequence,
            previous_hash: self.hash.clone(),
            hash: hash.clone(),
            reason: reason.into(),
            ledger: ledger.clone(),
        })
        .map_err(|_| "cannot encode durable live state")?;
        bytes.push(b'\n');
        if bytes.len() > MAX_RECORD || self.bytes + bytes.len() as u64 > self.maximum {
            return Err("live journal capacity reached; no new external effect permitted".into());
        }
        let emergency = (self.maximum / 4).min(4 * 1024 * 1024);
        if reason == "signed_submission_intent"
            && self.bytes + bytes.len() as u64 + emergency > self.maximum
        {
            return Err("live journal emergency cancel reserve reached; new posts stopped".into());
        }
        // A failed write/fsync may have persisted any prefix, including the complete record.
        // The sequence/hash cannot be reused until replay determines the durable boundary.
        self.poisoned = true;
        if self.segment_bytes > 0 && self.segment_bytes + bytes.len() as u64 > SEGMENT_BYTES {
            self.file = OpenOptions::new()
                .create_new(true)
                .append(true)
                .open(
                    self.directory
                        .join(format!("live-{:016}.jsonl", self.sequence)),
                )
                .map_err(|_| "cannot rotate live journal segment")?;
            sync_directory(&self.directory)?;
            self.segment_bytes = 0;
            self.segment_start_sequence = self.sequence;
            self.segment_previous_hash = self.hash.clone();
        }
        self.file
            .write_all(&bytes)
            .map_err(|_| "durable live journal write failed")?;
        #[cfg(test)]
        if self.fail_sync_once {
            self.fail_sync_once = false;
            return Err("injected live journal fsync failure after complete write".into());
        }
        self.file
            .sync_all()
            .map_err(|_| "durable live journal write failed")?;
        self.sequence += 1;
        self.bytes += bytes.len() as u64;
        self.segment_bytes += bytes.len() as u64;
        self.hash = hash;
        self.prefix_confirmed = true;
        self.poisoned = false;
        Ok(())
    }
    /// Safe committed-prefix metadata; signed state is never included.
    pub fn committed_prefix(&self) -> Option<serde_json::Value> {
        if self.poisoned || !self.prefix_confirmed {
            return None;
        }
        Some(serde_json::json!({
            "schema_version": 1, "source_kind": "live",
            "ledger_id": self.research_ledger_id, "account_identity": self.account_identity,
            "segment": {
                "name": format!("live-{:016}.jsonl", self.segment_start_sequence),
                "start_sequence": self.segment_start_sequence,
                "previous_hash": self.segment_previous_hash,
            },
            "byte_offset": self.segment_bytes,
            "next_sequence": self.sequence, "last_hash": self.hash,
        }))
    }
    pub fn is_poisoned(&self) -> bool {
        self.poisoned
    }
    #[cfg(test)]
    pub fn fail_next_sync(&mut self) {
        self.fail_sync_once = true;
    }
}
fn sync_directory(path: &Path) -> Result<(), String> {
    #[cfg(unix)]
    File::open(path)
        .and_then(|f| f.sync_all())
        .map_err(|_| "cannot sync live directory")?;
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

pub(crate) fn validate_ledger(ledger: &Ledger, settings: &LiveSettings) -> Result<(), String> {
    crate::settlement::validate(ledger, settings)?;
    if ledger.orders.len() > 256
        || ledger.positions.len() > 128
        || ledger.position_floor.len() > 128
        || ledger.receipts.len() > 2048
        || ledger.signed_intents.len() > 256
        || ledger.foreign_order_ids.len() > 256
        || ledger.session_spent_pico.parse::<u128>().is_err()
        || ledger
            .session_spent_pico
            .parse::<u128>()
            .is_ok_and(|spent| spent > 10u128.pow(32))
    {
        return Err("live replay exceeds ledger bounds".into());
    }
    for (id, order) in &ledger.orders {
        if id != &order.order_id
            || !crate::settings::condition(id)
            || order.cumulative_filled_micros > order.quote.quantity_micros
            || !condition_allowed(&order.quote, settings)
            || !ledger.signed_intents.contains_key(id)
        {
            return Err("invalid live order replay identity".into());
        }
        if !ledger
            .position_floor
            .get(&format!(
                "{}:{}",
                order.quote.condition_id, order.quote.token_id
            ))
            .is_some_and(|floor| floor.outcome == order.quote.outcome)
        {
            return Err("owned order has no durable pre-effect position floor".into());
        }
        let quote = &order.quote;
        if quote.slot.is_empty()
            || quote.slot.len() > 64
            || !crate::settings::condition(&quote.condition_id)
            || quote.market_slug.is_empty()
            || quote.market_slug.len() > 128
            || !token(&quote.token_id)
            || !(1..1_000_000).contains(&quote.price_micros)
            || quote.fee_bps > 10_000
            || !(1..=1_000_000_000_000_000).contains(&quote.quantity_micros)
            || !quote.quantity_micros.is_multiple_of(10_000)
            || ![100_000, 10_000, 5000, 2500, 1000, 100].contains(&quote.tick_micros)
            || !quote.price_micros.is_multiple_of(quote.tick_micros)
            || quote.min_order_micros == 0
            || quote.min_order_micros > quote.quantity_micros
            || quote.protocol != 2
            || quote.neg_risk
            || quote.window_start_ms >= quote.window_end_ms
            || quote.window_end_ms.saturating_sub(quote.window_start_ms) > 3_600_000
        {
            return Err("invalid recovered live quote structure".into());
        }
    }
    let mut gross = 0u128;
    for (key, position) in ledger.positions.iter().chain(ledger.position_floor.iter()) {
        let cost = position
            .cost_pico
            .parse::<u128>()
            .map_err(|_| "invalid recovered position cost")?;
        if key != &format!("{}:{}", position.condition_id, position.token_id)
            || !crate::settings::condition(&position.condition_id)
            || !token(&position.token_id)
            || position.quantity_micros > 1_000_000_000_000_000
            || cost > u128::from(position.quantity_micros) * 2_000_000
        {
            return Err("invalid recovered position structure".into());
        }
        gross = gross
            .checked_add(cost)
            .ok_or("recovered gross cost overflow")?;
    }
    Ok(())
}
fn token(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 78
        && !value.starts_with('0')
        && value.bytes().all(|b| b.is_ascii_digit())
        && value
            .parse::<polymarket_client_sdk_v2::types::U256>()
            .is_ok()
}
fn condition_allowed(quote: &crate::LiveQuote, settings: &LiveSettings) -> bool {
    settings.allowed_conditions.contains(&quote.condition_id)
        || settings
            .allowed_market_prefixes
            .iter()
            .any(|prefix| quote.market_slug.starts_with(prefix))
}

#[cfg(test)]
mod research_tests {
    use super::*;
    #[test]
    fn live_watermark_never_exposes_failed_sync_or_signed_material() {
        let dir = tempfile::tempdir().unwrap();
        let settings = LiveSettings::default();
        let (mut journal, mut ledger) = Journal::open(dir.path(), &settings).unwrap();
        assert!(journal.committed_prefix().is_none());
        ledger.signed_intents.insert(
            "private".into(),
            serde_json::json!({"secret":"never-export"}),
        );
        journal.commit("startup_paused_recovery", &ledger).unwrap();
        let prefix = journal.committed_prefix().unwrap();
        assert!(!prefix.to_string().contains("never-export"));
        assert_eq!(prefix["next_sequence"], 1);
        journal.fail_next_sync();
        assert!(journal.commit("owner_control", &ledger).is_err());
        assert!(journal.committed_prefix().is_none());
        assert_eq!(journal.sequence, 1);
        drop(journal);
        let (mut journal, ledger) = Journal::open(dir.path(), &settings).unwrap();
        assert!(journal.committed_prefix().is_none());
        journal.commit("startup_paused_recovery", &ledger).unwrap();
        assert_eq!(journal.committed_prefix().unwrap()["next_sequence"], 3);
    }
}
