//! One finite, offline CLOB-input recovery proof. No strategy, execution or PnL.
use mm2_engine::{
    config::Limits,
    discovery,
    domain::{self, Core, FeedEvent, Outcome},
    experiments as tape, feeds, ingress,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::Path,
};

const TARGETS: [u64; 5] = [242326, 340865, 486303, 968130, 988826];
const RAW_SHA: &str = "2de0890c6010e062dbf3d7413544c2768d32ba2016a69f1b9a88d9d8d12224d6";
const DIAGNOSTIC_SHA: &str = "dee7450fbbfb6e17fd5030b8b163b97cad2ba0726cc7114b2023ab1ae646d188";
const PLAN_SHA: &str = "c07c1fcd23ae102fe0b6e515cb43a2bca197cd62c12fdfb1c5ffc52178945482";
fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn read(path: &Path, cap: u64) -> Result<Vec<u8>, String> {
    let mut bytes = Vec::new();
    File::open(path)
        .map_err(|_| "input unavailable")?
        .take(cap + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| "input read failed")?;
    if bytes.len() as u64 > cap {
        return Err("input byte cap".into());
    }
    Ok(bytes)
}
fn number(value: &Value, key: &str) -> Result<u64, String> {
    value[key]
        .as_u64()
        .ok_or_else(|| format!("missing numeric {key}"))
}
fn index(outcome: Outcome) -> usize {
    usize::from(outcome == Outcome::Down)
}
fn book_summary(book: &domain::BookSnapshot) -> Value {
    json!({"initialized":book.initialized,"tick_micro":book.tick_micro,"best_bid_micro":book.best_bid_micro,
        "best_ask_micro":book.best_ask_micro,"exchange_ms":book.exchange_ms,"observed_mono_ms":book.observed_mono_ms})
}
#[derive(Clone, Copy)]
struct BookEvidence {
    exchange: u64,
    received: u64,
    sequence: u64,
}
struct Trial {
    floors: [u64; 2],
    books: [Option<BookEvidence>; 2],
    fault_mono: u64,
    close: u64,
    max_age: u64,
}
impl Trial {
    fn new(floors: [u64; 2], fault_mono: u64, close: u64, max_age: u64) -> Self {
        Self {
            floors,
            books: [None, None],
            fault_mono,
            close,
            max_age,
        }
    }
    fn before_deadline(&self, wall: u64, mono: u64) -> bool {
        mono.checked_sub(self.fault_mono)
            .is_some_and(|age| age < self.max_age)
            && wall < self.close
    }
    fn incremental(&mut self, outcome: Outcome, exchange: u64) {
        let i = index(outcome);
        self.floors[i] = self.floors[i].max(exchange);
        if self.books[i].is_some_and(|book| book.exchange <= exchange) {
            self.books[i] = None;
        }
    }
    fn snapshot(
        &mut self,
        outcome: Outcome,
        exchange: u64,
        received: u64,
        wall: u64,
        mono: u64,
        sequence: u64,
    ) -> bool {
        let i = index(outcome);
        let prior = self.floors[i];
        self.floors[i] = prior.max(exchange);
        if exchange <= prior
            || !self.before_deadline(wall, mono)
            || exchange > wall
            || received > mono
            || wall - exchange >= self.max_age
            || mono - received >= self.max_age
        {
            if self.books[i].is_some_and(|book| book.exchange <= self.floors[i]) {
                self.books[i] = None;
            }
            return false;
        }
        self.books[i] = Some(BookEvidence {
            exchange,
            received,
            sequence,
        });
        true
    }
    fn complete(&self, wall: u64, mono: u64) -> bool {
        self.before_deadline(wall, mono)
            && self.books.iter().all(|book| {
                book.is_some_and(|b| {
                    b.exchange <= wall
                        && b.received <= mono
                        && wall - b.exchange < self.max_age
                        && mono - b.received < self.max_age
                })
            })
    }
}
fn result(
    sequence: u64,
    status: &str,
    reason: &str,
    certificate: bool,
    trial: Option<&Trial>,
) -> Value {
    json!({"target_sequence":sequence,"status":status,"reason":reason,"clob_prestate_certificate_matches":certificate,
        "strict_error":"clob_exchange_clock_regressed","original_fatal_and_gap_quality_preserved":true,
        "quality_eligible":false,"quote_admission":"NONE_INPUT_PROOF_ONLY","cancel_and_possible_fills":"NOT_VALIDATED",
        "capital_loss_and_drawdown_accounting":"NOT_VALIDATED","reference_and_production_parity":"UNKNOWN",
        "retained_filter_floors":trial.map(|t|t.floors),"qualifying_book_sequences":trial.map(|t|t.books.map(|b|b.map(|b|b.sequence)))})
}
fn prove(
    case: &Value,
    records: &[tape::Record],
    witness: &Value,
    limits: &Limits,
) -> Result<Value, String> {
    let target = &case["target"];
    let sequence = number(target, "sequence")?;
    let fault = records
        .iter()
        .find(|r| r.sequence == sequence)
        .ok_or("target absent")?;
    let predecessor = records
        .iter()
        .find(|r| r.sequence + 1 == sequence)
        .ok_or("predecessor absent")?;
    if fault.hash != target["record_hash"]
        || predecessor.hash != target["predecessor_record_hash"]
        || fault.previous_hash != predecessor.hash
    {
        return Err("target hash binding".into());
    }
    if fault.mono_ms != number(target, "fault_mono_ms")?
        || fault.mono_ms.checked_add(1000) != Some(number(target, "exclusive_deadline_mono_ms")?)
        || limits.max_feed_age_ms != 1000
    {
        return Err("fixed exclusive deadline binding".into());
    }
    if witness["origin"]["record_hash"] != target["record_hash"]
        || witness["origin"]["frame_sha256"] != target["frame_sha256"]
        || witness["origin"]["source_generation"] != target["source_generation"]
        || witness["error_class"] != "clob_exchange_clock_regressed"
        || witness["origin"]["record_mono_ms"] != fault.mono_ms
        || witness["origin"]["record_wall_ms"] != fault.wall_ms
        || witness["origin"]["source_feed"] != "clob"
    {
        return Err("sealed witness binding".into());
    }
    let market_record = records
        .iter()
        .find_map(|r| {
            if let tape::Input::Market { market, metadata } = &r.input {
                Some((market, metadata))
            } else {
                None
            }
        })
        .ok_or("market metadata absent")?;
    let market = discovery::parse_market(market_record.1, market_record.0.window_start_ms)?;
    let slug = target["market_slug"].as_str().ok_or("slug absent")?;
    if market.slug != slug
        || serde_json::to_value(tape::MarketWire::from(&market)).map_err(|_| "market encoding")?
            != serde_json::to_value(market_record.0).map_err(|_| "market encoding")?
    {
        return Err("market identity binding".into());
    }
    let mut core = Core::new(limits.core()?)?;
    core.set_market(market.clone())?;
    let mut recovery = feeds::Recovery::new([0; 2]);
    let mut native = None;
    // The full paired predecessor is the only CLOB seed. Do not apply cached
    // ticks from a different generation, nor claim full historical parity.
    let frame = |r: &tape::Record| -> Result<(u64, String), String> {
        match &r.input {
            tape::Input::Frame {
                feed,
                market_slug,
                generation,
                text,
            } if feed == "clob" && market_slug.as_deref() == Some(slug) => {
                Ok((*generation, text.clone()))
            }
            _ => Err("expected exact same-market CLOB frame".into()),
        }
    };
    let (generation, seed) = frame(predecessor)?;
    let (bad_generation, bad) = frame(fault)?;
    if generation != bad_generation
        || generation != number(target, "source_generation")?
        || digest(bad.as_bytes()) != target["frame_sha256"]
    {
        return Err("frame/generation binding".into());
    }
    let seed_events =
        feeds::parse_clob(&seed, &market, predecessor.mono_ms).map_err(|_| "seed parse failure")?;
    if seed_events.len() != 2
        || !seed_events
            .iter()
            .all(|e| matches!(e, FeedEvent::Snapshot { .. }))
    {
        return Ok(result(
            sequence,
            "UNKNOWN",
            "PREDECESSOR_NOT_TWO_FULL_BOOKS",
            false,
            None,
        ));
    }
    for event in recovery.filter(seed_events) {
        if ingress::apply_with_native_snapshots(&mut core, event, &mut native).is_err() {
            return Ok(result(
                sequence,
                "UNKNOWN",
                "SEED_STRICT_APPLY_FAILED",
                false,
                None,
            ));
        }
    }
    let parsed =
        feeds::parse_clob(&bad, &market, fault.mono_ms).map_err(|_| "target parse failure")?;
    let events = recovery.filter(parsed);
    if events.len() != 1 || !matches!(events[0], FeedEvent::Delta { .. }) {
        return Ok(result(
            sequence,
            "UNKNOWN",
            "TARGET_BATCH_NOT_ONE_DELTA",
            false,
            None,
        ));
    }
    let event = &events[0];
    let snapshot = core.snapshot();
    let context = &witness["context"];
    let mut certificate = serde_json::to_value(core.feed_clock_context(event))
        .map_err(|_| "clock encoding")?
        == context["accepted_clock_watermarks"]
        && book_summary(&snapshot.up_book) == context["pre_error_books"]["up"]
        && book_summary(&snapshot.down_book) == context["pre_error_books"]["down"];
    if let FeedEvent::Delta {
        condition_id,
        changes,
        ..
    } = event
    {
        let known = context["event"]["first_changes"]
            .as_array()
            .ok_or("sealed quantities absent")?;
        certificate &= changes.len() == known.len() && changes.len() <= 4;
        for (change, prior) in changes.iter().zip(known) {
            let outcome = if change.outcome == Outcome::Up {
                "up"
            } else {
                "down"
            };
            let side = if change.side == domain::BookSide::Bid {
                "bid"
            } else {
                "ask"
            };
            certificate &= prior["lookup_valid"] == true
                && prior["outcome"] == outcome
                && prior["side"] == side
                && prior["price_micro"] == change.level.price_micro
                && prior["quantity_micro"] == change.level.quantity_micro;
            certificate &= serde_json::to_value(
                core.level_quantity(
                    condition_id,
                    change.outcome,
                    change.side,
                    change.level.price_micro,
                )
                .map_err(|_| "level identity")?,
            )
            .map_err(|_| "level encoding")?
                == prior["pre_error_current_level"];
        }
    }
    let accepted = core.feed_clock_context(event).ok_or("CLOB clock absent")?;
    let floors = accepted.book_exchange_ms.map(|value| value.unwrap_or(0));
    certificate &= accepted.tick_exchange_ms.iter().all(Option::is_none)
        && floors == recovery.exchange_watermarks();
    if !certificate {
        return Ok(result(
            sequence,
            "UNKNOWN",
            "CLOB_PRESTATE_OR_FILTER_FLOOR_NOT_RECREATED",
            false,
            None,
        ));
    }
    if ingress::apply_with_native_snapshots(&mut core, event.clone(), &mut native)
        .err()
        .as_deref()
        != Some("CLOB exchange clock regressed")
    {
        return Ok(result(
            sequence,
            "UNKNOWN",
            "ORIGINAL_STRICT_ERROR_NOT_REPRODUCED",
            false,
            None,
        ));
    }
    let mut trial = Trial::new(
        floors,
        fault.mono_ms,
        market.window_end_ms,
        limits.max_feed_age_ms,
    );
    for record in records.iter().filter(|r| r.sequence > sequence) {
        if !trial.before_deadline(record.wall_ms, record.mono_ms) {
            return Ok(result(
                sequence,
                "ABORT",
                "DEADLINE_OR_CLOSE",
                true,
                Some(&trial),
            ));
        }
        match &record.input {
            tape::Input::Gap { market_slug, .. }
                if market_slug.as_deref().is_none_or(|s| s == slug) =>
            {
                return Ok(result(
                    sequence,
                    "ABORT",
                    "RELEVANT_RECORDED_GAP",
                    true,
                    Some(&trial),
                ));
            }
            tape::Input::Frame {
                feed,
                market_slug,
                generation: next_generation,
                text,
            } if feed == "clob" && market_slug.as_deref() == Some(slug) => {
                if *next_generation != generation {
                    return Ok(result(
                        sequence,
                        "ABORT",
                        "OBSERVED_GENERATION_CHANGE",
                        true,
                        Some(&trial),
                    ));
                }
                let parsed = match feeds::parse_clob(text, &market, record.mono_ms) {
                    Ok(events) => events,
                    Err(_) => {
                        return Ok(result(
                            sequence,
                            "ABORT",
                            "NATIVE_PARSE_FAILURE",
                            true,
                            Some(&trial),
                        ));
                    }
                };
                let before = recovery.exchange_watermarks();
                if before != trial.floors {
                    return Ok(result(
                        sequence,
                        "UNKNOWN",
                        "FILTER_FLOOR_BEFORE_FRAME_MISMATCH",
                        true,
                        Some(&trial),
                    ));
                }
                // Filter the complete frame exactly once. Its hidden state may
                // look ahead within this frame; qualifying comparisons use the
                // retained sequential floor before each admitted event instead.
                let admitted = recovery.filter(parsed);
                for event in admitted {
                    match &event {
                        FeedEvent::Snapshot {
                            outcome,
                            exchange_ms,
                            observed_mono_ms,
                            ..
                        } => {
                            let mut checked = core.clone();
                            if checked.apply(event.clone()).is_err()
                                || *exchange_ms > record.wall_ms
                                || *observed_mono_ms > record.mono_ms
                            {
                                return Ok(result(
                                    sequence,
                                    "ABORT",
                                    "INVALID_FULL_BOOK",
                                    true,
                                    Some(&trial),
                                ));
                            }
                            if trial.snapshot(
                                *outcome,
                                *exchange_ms,
                                *observed_mono_ms,
                                record.wall_ms,
                                record.mono_ms,
                                record.sequence,
                            ) && core.apply(event).is_err()
                            {
                                return Ok(result(
                                    sequence,
                                    "ABORT",
                                    "ANOTHER_STRICT_CORE_ERROR",
                                    true,
                                    Some(&trial),
                                ));
                            }
                        }
                        FeedEvent::Delta {
                            exchange_ms,
                            changes,
                            ..
                        } => {
                            for change in changes {
                                trial.incremental(change.outcome, *exchange_ms);
                            }
                        }
                        FeedEvent::Tick {
                            outcome,
                            exchange_ms,
                            ..
                        } => {
                            trial.incremental(*outcome, *exchange_ms);
                            if core.apply(event).is_err() {
                                return Ok(result(
                                    sequence,
                                    "ABORT",
                                    "ANOTHER_STRICT_CORE_ERROR",
                                    true,
                                    Some(&trial),
                                ));
                            }
                        }
                        _ => {}
                    }
                }
                if recovery.exchange_watermarks() != trial.floors {
                    return Ok(result(
                        sequence,
                        "UNKNOWN",
                        "FILTER_FLOOR_AFTER_FRAME_MISMATCH",
                        true,
                        Some(&trial),
                    ));
                }
                if trial.complete(record.wall_ms, record.mono_ms) {
                    return Ok(result(
                        sequence,
                        "RECOVERED_STILL_INVALID",
                        "TWO_ACTUAL_NATIVE_BOOKS_WITHIN_BOUNDS",
                        true,
                        Some(&trial),
                    ));
                }
            }
            _ => {}
        }
    }
    Ok(result(
        sequence,
        "ABORT",
        "NO_TWO_QUALIFYING_BOOKS_IN_COMPLETE_EXCLUSIVE_INTERVAL",
        true,
        Some(&trial),
    ))
}
fn main() -> Result<(), String> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() != 5 {
        return Err("usage: local_recovery_proof FIXTURE_DIR FROZEN_PLAN.json DIAGNOSTIC_REPORT.json NEW_REPORT.json".into());
    }
    let folder = Path::new(&args[1]);
    let manifest_bytes = read(
        &folder.join("COMPACT_LOCAL_FIXTURE_MANIFEST.json"),
        1_048_576,
    )?;
    let manifest: Value = serde_json::from_slice(&manifest_bytes).map_err(|_| "manifest JSON")?;
    if manifest["status"] != "EXTRACTION_UNIT_FAILED_FINAL_MANIFEST_CAP_LOCAL_FIXTURES_VALIDATED"
        || manifest["derivation_label"] != "DERIVED_LOCALLY_AFTER_METADATA_CAP_FAILURE"
        || manifest["source_tape_sha256"] != RAW_SHA
        || manifest["source_diagnostic_report_sha256"] != DIAGNOSTIC_SHA
        || manifest["remote_attempt_success"] != false
        || manifest["local_derived_only"] != true
        || manifest["new_raw_passes"] != 0
    {
        return Err("fixture source binding".into());
    }
    let plan_bytes = read(Path::new(&args[2]), 65536)?;
    let diagnostic_bytes = read(Path::new(&args[3]), 2_097_152)?;
    if digest(&plan_bytes) != PLAN_SHA || digest(&diagnostic_bytes) != DIAGNOSTIC_SHA {
        return Err("sealed plan/diagnostic hash mismatch".into());
    }
    let plan: Value = serde_json::from_slice(&plan_bytes).map_err(|_| "plan JSON")?;
    let limits: Limits =
        serde_json::from_value(plan["limits"].clone()).map_err(|_| "limits JSON")?;
    let diagnostic: Value =
        serde_json::from_slice(&diagnostic_bytes).map_err(|_| "diagnostic JSON")?;
    let cases = manifest["cases"].as_array().ok_or("case list absent")?;
    if cases.len() != 5 {
        return Err("exact five fixed cases required".into());
    }
    let mut rows = Vec::new();
    let mut total_bytes = 0;
    for (case, expected) in cases.iter().zip(TARGETS) {
        if number(&case["target"], "sequence")? != expected {
            return Err("fixed case order/identity".into());
        }
        let filename = format!("case-{expected}.jsonl");
        if case["filename"] != filename {
            return Err("fixture filename binding".into());
        }
        let bytes = read(&folder.join(filename), 3_145_728)?;
        total_bytes += bytes.len();
        if total_bytes > 15_728_640
            || digest(&bytes) != case["sha256"]
            || bytes.len() as u64 != number(case, "bytes")?
        {
            return Err("fixture byte/hash bound".into());
        }
        let mut records = Vec::new();
        let mut last = None;
        for line in bytes
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
        {
            if records.len() >= 4096 {
                return Err("fixture record cap".into());
            }
            let record: tape::Record = serde_json::from_slice(line).map_err(|_| "record JSON")?;
            if last.is_some_and(|(seq, mono)| record.sequence <= seq || record.mono_ms < mono)
                || record.hash
                    != tape::hash(&json!([
                        record.sequence,
                        record.previous_hash,
                        record.wall_ms,
                        record.mono_ms,
                        record.input
                    ]))
            {
                return Err("sparse original-record hash/order".into());
            }
            last = Some((record.sequence, record.mono_ms));
            records.push(record);
        }
        if records.len() as u64 != number(case, "record_count")? {
            return Err("fixture record count".into());
        }
        let witness = diagnostic["ingress_diagnostics"]["witnesses"]
            .as_array()
            .ok_or("witness list absent")?
            .iter()
            .find(|w| w["origin"]["record_sequence"] == expected)
            .ok_or("fixed witness absent")?;
        rows.push(prove(case, &records, witness, &limits)?);
    }
    let report = json!({"schema":1,"purpose":"ONE_FINITE_LOCAL_INPUT_RECOVERY_PROOF_NOT_FOR_SELECTION","network":"none",
        "source_tape_sha256":RAW_SHA,"plan_sha256":PLAN_SHA,"diagnostic_sha256":DIAGNOSTIC_SHA,"fixture_manifest_sha256":digest(&manifest_bytes),
        "cases":rows,"all_five_recovered":rows.iter().all(|r|r["status"]=="RECOVERED_STILL_INVALID"),
        "source_records_individual_hashes_verified":true,"sparse_fixture_is_full_native_chain":false,
        "fixture_extraction_unit_status":"FAILED_FINAL_MANIFEST_CAP_LOCAL_METADATA_DERIVED","full_interval_coverage":"CONDITIONAL_ON_BOUND_SINGLE_PASS_EXTRACTOR",
        "default_or_replay_execution_modified":false,"original_quality_failures_forgiven":false,"strategy_or_PnL_run":false,
        "quoting_cancel_possible_fills_and_accounting_validated":false,"production_parity":"UNKNOWN","selection_or_promotion_authorized":false});
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&args[4])
        .map_err(|_| "new report unavailable")?;
    serde_json::to_writer_pretty(&mut output, &report).map_err(|_| "report encoding")?;
    output.write_all(b"\n").map_err(|_| "report write")?;
    output.sync_all().map_err(|_| "report sync")?;
    println!(
        "{}",
        json!({"cases":5,"all_five_recovered":report["all_five_recovered"],"strategy_or_PnL_run":false})
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn recovery_needs_two_new_books_and_later_incremental_invalidates_old_credit() {
        let mut trial = Trial::new([100, 100], 10, 1000, 1000);
        assert!(!trial.snapshot(Outcome::Up, 100, 11, 101, 11, 1));
        assert!(trial.snapshot(Outcome::Up, 101, 12, 102, 12, 2));
        assert!(!trial.complete(102, 12));
        trial.incremental(Outcome::Up, 102);
        assert!(trial.books[0].is_none());
        assert!(!trial.snapshot(Outcome::Up, 102, 13, 103, 13, 3));
        assert!(trial.snapshot(Outcome::Up, 103, 14, 104, 14, 4));
        assert!(trial.snapshot(Outcome::Down, 104, 15, 105, 15, 5));
        assert!(trial.complete(105, 15));
    }
    #[test]
    fn equality_deadline_close_and_stale_book_never_prove_recovery() {
        let mut trial = Trial::new([100, 100], 10, 5000, 1000);
        assert!(!trial.snapshot(Outcome::Up, 1009, 1010, 1010, 1010, 1));
        let mut trial = Trial::new([100, 100], 10, 200, 1000);
        assert!(!trial.snapshot(Outcome::Up, 199, 20, 200, 20, 1));
        let mut trial = Trial::new([100, 100], 10, 5000, 1000);
        assert!(!trial.snapshot(Outcome::Up, 101, 11, 1101, 12, 1));
        assert!(!trial.snapshot(Outcome::Up, 101, 15, 102, 14, 1));
    }
    #[test]
    fn nonqualifying_snapshot_invalidates_old_credit_and_completion_freshness_is_strict() {
        let mut trial = Trial::new([100, 100], 10, 5000, 1000);
        assert!(trial.snapshot(Outcome::Up, 101, 11, 102, 11, 1));
        assert!(trial.snapshot(Outcome::Down, 102, 12, 103, 12, 2));
        assert!(!trial.complete(1101, 13));
        assert!(!trial.snapshot(Outcome::Up, 103, 14, 1103, 14, 3));
        assert!(trial.books[0].is_none());
    }
    #[test]
    fn observed_filter_floor_is_read_only_and_survives_regressed_delta() {
        let mut recovery = feeds::Recovery::new([0; 2]);
        for outcome in [Outcome::Up, Outcome::Down] {
            recovery.filter(vec![FeedEvent::Snapshot {
                condition_id: "fixture".into(),
                outcome,
                exchange_ms: 100,
                observed_mono_ms: 1,
                bids: vec![domain::Level {
                    price_micro: 490000,
                    quantity_micro: 1,
                }],
                asks: vec![domain::Level {
                    price_micro: 510000,
                    quantity_micro: 1,
                }],
            }]);
        }
        let before = recovery.exchange_watermarks();
        assert_eq!(before, [100, 100]);
        assert!(recovery.ready());
        let events = recovery.filter(vec![FeedEvent::Delta {
            condition_id: "fixture".into(),
            exchange_ms: 99,
            observed_mono_ms: 2,
            changes: vec![domain::Change {
                outcome: Outcome::Up,
                side: domain::BookSide::Bid,
                level: domain::Level {
                    price_micro: 490000,
                    quantity_micro: 2,
                },
            }],
        }]);
        assert_eq!(events.len(), 1);
        assert_eq!(recovery.exchange_watermarks(), before);
        assert!(recovery.ready());
    }
}
