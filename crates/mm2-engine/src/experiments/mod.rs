//! Public raw input evidence for isolated experiments. This module has no order API.
use crate::domain::Market;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Read, Write},
    path::Path,
};

pub const MAX_LINE: usize = 1_100_000;
pub const ZERO: &str = "0000000000000000000000000000000000000000000000000000000000000000";
pub fn hash(value: &Value) -> String {
    format!("{:x}", Sha256::digest(serde_json::to_vec(value).unwrap()))
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketWire {
    pub slug: String,
    pub condition_id: String,
    pub up_token: String,
    pub down_token: String,
    pub window_start_ms: u64,
    pub window_end_ms: u64,
    pub tick_micro: u64,
}
impl From<&Market> for MarketWire {
    fn from(m: &Market) -> Self {
        Self {
            slug: m.slug.clone(),
            condition_id: m.condition_id.clone(),
            up_token: m.up_token.clone(),
            down_token: m.down_token.clone(),
            window_start_ms: m.window_start_ms,
            window_end_ms: m.window_end_ms,
            tick_micro: m.tick_micro,
        }
    }
}
impl MarketWire {
    pub fn market(&self) -> Market {
        Market {
            slug: self.slug.clone(),
            condition_id: self.condition_id.clone(),
            up_token: self.up_token.clone(),
            down_token: self.down_token.clone(),
            window_start_ms: self.window_start_ms,
            window_end_ms: self.window_end_ms,
            tick_micro: self.tick_micro,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Input {
    Header {
        schema: u32,
        source: String,
        started_wall_ms: u64,
        max_bytes: u64,
        max_frames: u64,
    },
    Market {
        market: MarketWire,
        metadata: Value,
    },
    Fee {
        condition_id: String,
        raw: Value,
        version: Value,
    },
    Frame {
        feed: String,
        market_slug: Option<String>,
        generation: u64,
        text: String,
    },
    Gap {
        feed: String,
        market_slug: Option<String>,
        reason: String,
    },
    Outcome {
        evidence: crate::paper_resolution::Evidence,
    },
    End {
        reason: String,
        frames: u64,
        gaps: u64,
        complete: bool,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Record {
    pub sequence: u64,
    pub previous_hash: String,
    pub hash: String,
    pub wall_ms: u64,
    pub mono_ms: u64,
    pub input: Input,
}
pub struct Writer {
    out: BufWriter<File>,
    sequence: u64,
    previous: String,
    bytes: u64,
    max_bytes: u64,
}
impl Writer {
    pub fn new(path: &Path, max_bytes: u64) -> Result<Self, String> {
        Ok(Self {
            out: BufWriter::new(
                OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(path)
                    .map_err(|e| e.to_string())?,
            ),
            sequence: 0,
            previous: ZERO.into(),
            bytes: 0,
            max_bytes,
        })
    }
    pub fn append(
        &mut self,
        wall_ms: u64,
        mono_ms: u64,
        input: Input,
        final_record: bool,
    ) -> Result<(), String> {
        let body = json!([self.sequence, self.previous, wall_ms, mono_ms, input]);
        let digest = hash(&body);
        let record = Record {
            sequence: self.sequence,
            previous_hash: self.previous.clone(),
            hash: digest.clone(),
            wall_ms,
            mono_ms,
            input,
        };
        let mut bytes = serde_json::to_vec(&record).map_err(|e| e.to_string())?;
        bytes.push(b'\n');
        if bytes.len() > MAX_LINE
            || self.bytes + bytes.len() as u64
                > self.max_bytes - if final_record { 0 } else { 8192 }
        {
            return Err("capture byte budget exhausted".into());
        }
        self.out
            .write_all(&bytes)
            .and_then(|_| self.out.flush())
            .map_err(|e| e.to_string())?;
        self.bytes += bytes.len() as u64;
        self.sequence += 1;
        self.previous = digest;
        Ok(())
    }
    pub fn finish(&mut self) -> Result<(), String> {
        self.out
            .flush()
            .and_then(|_| self.out.get_ref().sync_all())
            .map_err(|e| e.to_string())
    }
}
pub fn visit(
    path: &Path,
    max_bytes: u64,
    mut accept: impl FnMut(Record) -> Result<(), String>,
) -> Result<Value, String> {
    let file = File::open(path).map_err(|e| e.to_string())?;
    if file.metadata().map_err(|e| e.to_string())?.len() > max_bytes {
        return Err("tape exceeds replay byte budget".into());
    }
    let mut reader = BufReader::new(file);
    let mut expected = 0;
    let mut previous = ZERO.to_string();
    let mut mono = 0;
    let mut ended = false;
    let mut frames = 0_u64;
    let mut gaps = 0_u64;
    loop {
        let mut bytes = Vec::new();
        let count = std::io::Read::by_ref(&mut reader)
            .take((MAX_LINE + 1) as u64)
            .read_until(b'\n', &mut bytes)
            .map_err(|e| e.to_string())?;
        if count == 0 {
            break;
        }
        if count > MAX_LINE || bytes.last() != Some(&b'\n') {
            return Err("partial or oversized tape record".into());
        }
        let record: Record = serde_json::from_slice(&bytes).map_err(|_| "invalid tape record")?;
        if ended
            || record.sequence != expected
            || record.previous_hash != previous
            || record.mono_ms < mono
            || record.hash
                != hash(&json!([
                    record.sequence,
                    record.previous_hash,
                    record.wall_ms,
                    record.mono_ms,
                    record.input
                ]))
        {
            return Err("tape chain/order mismatch".into());
        }
        if (expected == 0 && !matches!(record.input, Input::Header { schema: 1, .. }))
            || (expected > 0 && matches!(record.input, Input::Header { .. }))
        {
            return Err("missing supported tape genesis".into());
        }
        ended = matches!(record.input, Input::End { .. });
        match &record.input {
            Input::Frame { .. } => frames += 1,
            Input::Gap { .. } => gaps += 1,
            Input::End {
                frames: declared_frames,
                gaps: declared_gaps,
                ..
            } if *declared_frames != frames || *declared_gaps != gaps => {
                return Err("capture final counters disagree with tape".into());
            }
            _ => {}
        }
        previous = record.hash.clone();
        mono = record.mono_ms;
        expected += 1;
        accept(record)?;
    }
    if !ended {
        return Err("tape has no final receipt".into());
    }
    Ok(json!({"records":expected,"last_hash":previous,"verified_chain":true}))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn tampering_and_partial_tapes_fail_closed() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("tape");
        let mut w = Writer::new(&p, 65536).unwrap();
        w.append(
            1000,
            0,
            Input::Header {
                schema: 1,
                source: "fixture".into(),
                started_wall_ms: 1000,
                max_bytes: 65536,
                max_frames: 10,
            },
            false,
        )
        .unwrap();
        assert!(visit(&p, 65536, |_| Ok(())).is_err());
        w.append(
            1001,
            1,
            Input::End {
                reason: "fixture".into(),
                frames: 0,
                gaps: 0,
                complete: false,
            },
            true,
        )
        .unwrap();
        w.finish().unwrap();
        assert!(visit(&p, 65536, |_| Ok(())).is_ok());
        let s = std::fs::read_to_string(&p)
            .unwrap()
            .replace("fixture", "changed");
        std::fs::write(&p, s).unwrap();
        assert!(visit(&p, 65536, |_| Ok(())).is_err());
    }
}
