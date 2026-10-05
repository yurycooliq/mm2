use super::*;
use crate::settlement::{OwnedPosition, SettlementMarket};
use async_trait::async_trait;
use tokio::sync::Mutex;

struct ChainVenue {
    directory: PathBuf,
    broadcasts: Mutex<Vec<(String, u64, String)>>,
}
#[async_trait]
impl Venue for ChainVenue {
    async fn prepare(&self, _: LiveQuote) -> Result<PreparedOrder, String> {
        Err("no quote fixture".into())
    }
    async fn post(&self, _: PreparedOrder) -> Result<PostResult, String> {
        panic!("no funded order in chain tests")
    }
    async fn cancel(&self, _: Vec<String>) -> Result<Vec<String>, String> {
        Ok(vec![])
    }
    async fn reconcile(&self, _: Vec<LiveOrder>) -> Result<AccountObservation, String> {
        Err("injected account lag".into())
    }
    async fn heartbeat(&self) -> Result<(), String> {
        Ok(())
    }
    async fn listen(&self, _: mpsc::Sender<VenueNotice>, _: watch::Receiver<bool>) {}
    async fn submit_chain(&self, p: PreparedChainTransaction) -> Result<Option<String>, String> {
        if let Some(relay) = &p.relayer {
            assert!(
                relay.dispatched,
                "relayer dispatch must be durable before POST"
            );
            let body = std::fs::read_dir(&self.directory)
                .unwrap()
                .filter_map(Result::ok)
                .filter(|e| e.path().extension().is_some_and(|v| v == "jsonl"))
                .map(|e| std::fs::read_to_string(e.path()).unwrap())
                .collect::<Vec<_>>()
                .join("\n");
            assert!(body.contains("relayer_dispatch_before_send_once"));
            assert!(body.contains("\"dispatched\":true"));
        }
        let journaled = std::fs::read_dir(&self.directory)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|e| e.path().extension().is_some_and(|v| v == "jsonl"))
            .any(|e| {
                let body = std::fs::read_to_string(e.path()).unwrap();
                body.contains("signed_chain_intent_before_broadcast")
                    && body.contains(&p.raw_transaction)
                    && body.contains(&p.hash)
            });
        assert!(
            journaled,
            "raw transaction must be synced before any broadcast"
        );
        self.broadcasts
            .lock()
            .await
            .push((p.hash, p.nonce, p.raw_transaction));
        Err("accepted but ACK lost".into())
    }
    async fn lookup_chain(&self, _: PreparedChainTransaction) -> Result<ChainObservation, String> {
        Ok(ChainObservation::Unknown)
    }
}
async fn fixture(
    directory: &std::path::Path,
) -> (Actor, Arc<ChainVenue>, PreparedChainTransaction) {
    fixture_kind(directory, false).await
}
async fn fixture_kind(
    directory: &std::path::Path,
    deposit: bool,
) -> (Actor, Arc<ChainVenue>, PreparedChainTransaction) {
    let plan = ChainPlan {
        id: "fixture-closeout".into(),
        condition_id: format!("0x{}", "a".repeat(64)),
        token_ids: ["1".into(), "2".into()],
        operation: ChainOperation::Merge {
            amount_micros: 5_000_000,
        },
    };
    let (mut settings, prepared) = if deposit {
        crate::relay::fixture_prepared(plan, [5_000_000; 2]).await
    } else {
        crate::chain::fixture_prepared(plan, [5_000_000; 2]).await
    };
    if settings.chain.is_none() {
        settings.chain = Some(ChainSettings::default());
    }
    let mut ledger = Ledger {
        phase: LivePhase::Running,
        collateral_micros: 95_100_000,
        allowance_micros: 100_000_000,
        initial_collateral_micros: Some(100_000_000),
        session_spent_pico: "4900000000000".into(),
        daily_loss_micros: 4_900_000,
        loss_day: now_ms() / 86_400_000,
        ..Ledger::default()
    };
    for (token, outcome) in [("1", mm2_core::Outcome::Up), ("2", mm2_core::Outcome::Down)] {
        let key = format!("{}:{token}", prepared.plan.condition_id);
        let position = LivePosition {
            condition_id: prepared.plan.condition_id.clone(),
            token_id: token.into(),
            outcome,
            quantity_micros: 5_000_000,
            cost_pico: "2450000000000".into(),
            mergeable: true,
            redeemable: false,
        };
        ledger.positions.insert(key.clone(), position.clone());
        ledger.position_floor.insert(key.clone(), position);
        ledger.owned_positions.insert(
            key,
            OwnedPosition {
                quantity_micros: 5_000_000,
                cost_pico: "2450000000000".into(),
                day: ledger.loss_day,
                daily_quantity_micros: 5_000_000,
            },
        );
    }
    ledger.closeout_markets.insert(
        prepared.plan.condition_id.clone(),
        SettlementMarket {
            token_ids: ["1".into(), "2".into()],
            window_end_ms: now_ms() - 1000,
        },
    );
    let (mut journal, _) = Journal::open(directory, &settings).unwrap();
    journal.commit("fixture_account", &ledger).unwrap();
    let (_, commands) = mpsc::channel(1);
    let (_, quotes) = watch::channel(Quotes {
        quotes: vec![],
        now_ms: 0,
        sequence: 0,
    });
    let (_, events) = mpsc::channel(1);
    let (results_tx, results) = mpsc::channel(32);
    let (stop_tx, _) = watch::channel(false);
    let snapshot = snapshot(
        &ledger,
        &settings,
        &journal,
        PublicationState {
            connected: true,
            account: Some(now_ms()),
            heartbeat: Some(now_ms()),
            reconciled: true,
            coalesced: 0,
        },
    );
    let (publish, _) = watch::channel(Arc::new(snapshot));
    let venue = Arc::new(ChainVenue {
        directory: directory.into(),
        broadcasts: Mutex::new(vec![]),
    });
    let actor = Actor {
        settings: Arc::new(settings),
        venue: venue.clone(),
        ledger,
        journal: Some(journal),
        commands,
        quotes,
        events,
        results,
        results_tx,
        stop_tx,
        listener: tokio::spawn(async {}),
        workers: JoinSet::new(),
        desired: BTreeMap::new(),
        quote_received: None,
        quote_clock_ms: 0,
        quote_sequence: 0,
        coalesced: 0,
        account_reconciled: true,
        connected: true,
        last_account: Some(now_ms()),
        last_heartbeat: Some(now_ms()),
        preparing: false,
        posting: BTreeSet::new(),
        cancelling: BTreeSet::new(),
        reconciling: false,
        heartbeating: false,
        next_prepare: Instant::now(),
        next_cancel: Instant::now(),
        next_reconcile: Instant::now() + Duration::from_secs(60),
        next_heartbeat: Instant::now() + Duration::from_secs(60),
        next_publish: Instant::now(),
        prepare_attempts: 0,
        prepare_failures: 0,
        last_prepare_error: None,
        publish,
        shutdown: None,
        commands_open: true,
        events_open: true,
        quotes_open: true,
        chain_busy: false,
        chain_query_next: true,
        next_chain: Instant::now(),
        last_chain_error: None,
        chain_effect_epoch: 0,
        chain_last_condition: None,
    };
    (actor, venue, prepared)
}
fn receipt(p: &PreparedChainTransaction) -> ChainReceipt {
    ChainReceipt {
        transaction_hash: p.hash.clone(),
        block_number: 100,
        block_hash: format!("0x{}", "b".repeat(64)),
        finalized_block_number: 120,
        success: true,
        after_tokens_micros: [0, 0],
        after_collateral_micros: p.before_collateral_micros + 5_000_000,
        collateral_delta_micros: 5_000_000,
        consumed_tokens_micros: [5_000_000, 5_000_000],
        payout_numerators: [0, 0],
        payout_denominator: 0,
        gas_cost_wei: "1000".into(),
        intent_hash: None,
        sponsored_gas_cost_wei: None,
    }
}
#[tokio::test]
async fn lost_chain_ack_reserves_and_retry_uses_exact_durable_hash_and_nonce() {
    let dir = tempfile::tempdir().unwrap();
    let (mut actor, venue, prepared) = fixture(dir.path()).await;
    actor
        .result(ResultEvent::ChainPrepared(Box::new((
            prepared.plan.clone(),
            Ok(prepared.clone()),
        ))))
        .await;
    let event = tokio::time::timeout(Duration::from_secs(5), actor.results.recv())
        .await
        .unwrap()
        .unwrap();
    actor.result(event).await;
    assert_eq!(
        actor.ledger.chain_pending.as_ref().unwrap().hash,
        prepared.hash
    );
    assert_eq!(actor.ledger.returned_collateral_micros, 0);
    assert_eq!(actor.ledger.positions.len(), 2);
    actor
        .control("pause-chain".into(), ControlAction::Pause)
        .await
        .unwrap();
    actor
        .result(ResultEvent::ChainObserved(Ok(ChainObservation::Unknown)))
        .await;
    actor.next_chain = Instant::now();
    actor.pulse().await;
    let event = tokio::time::timeout(Duration::from_secs(5), actor.results.recv())
        .await
        .unwrap()
        .unwrap();
    actor.result(event).await;
    assert_eq!(
        venue.broadcasts.lock().await.len(),
        1,
        "pause permits queries, never rebroadcast"
    );
    actor.last_account = Some(now_ms());
    actor.last_heartbeat = Some(now_ms());
    actor.publish();
    actor
        .control("resume-chain".into(), ControlAction::Resume)
        .await
        .unwrap();
    actor.next_chain = Instant::now();
    actor.pulse().await;
    let event = tokio::time::timeout(Duration::from_secs(5), actor.results.recv())
        .await
        .unwrap()
        .unwrap();
    actor.result(event).await;
    let broadcasts = venue.broadcasts.lock().await;
    assert_eq!(broadcasts.len(), 2);
    assert_eq!(broadcasts[0], broadcasts[1]);
    let wire = serde_json::to_string(actor.publish.borrow().as_ref()).unwrap();
    assert!(!wire.contains(&prepared.raw_transaction));
}
#[tokio::test]
async fn finalized_effect_discards_inflight_old_account_and_credits_only_once() {
    let dir = tempfile::tempdir().unwrap();
    let (mut actor, _, prepared) = fixture(dir.path()).await;
    let old_positions = actor.ledger.positions.values().cloned().collect();
    actor.ledger.chain_pending = Some(prepared.clone());
    actor
        .result(ResultEvent::ChainObserved(Ok(ChainObservation::Finalized(
            receipt(&prepared),
        ))))
        .await;
    assert_eq!(actor.ledger.returned_collateral_micros, 5_000_000);
    assert!(actor.ledger.positions.is_empty());
    assert_eq!(actor.chain_effect_epoch, 1);
    actor
        .result(ResultEvent::Reconciled(
            0,
            Ok(AccountObservation {
                positions: old_positions,
                collateral_micros: 95_100_000,
                allowance_micros: 100_000_000,
                ..Default::default()
            }),
        ))
        .await;
    assert!(actor.ledger.positions.is_empty());
    assert!(!actor.account_reconciled);
    actor
        .result(ResultEvent::Reconciled(
            1,
            Ok(AccountObservation {
                collateral_micros: 100_100_000,
                allowance_micros: 100_000_000,
                ..Default::default()
            }),
        ))
        .await;
    assert_eq!(available_collateral(&actor.ledger), 100_000_000);
    let mut duplicate = actor.ledger.clone();
    assert!(duplicate.apply_chain_receipt(&receipt(&prepared)).is_err());
    assert_eq!(duplicate.returned_collateral_micros, 5_000_000);
}
#[tokio::test]
async fn journal_sync_fault_cannot_be_reset_or_retain_heartbeats() {
    let dir = tempfile::tempdir().unwrap();
    let (mut actor, _, _) = fixture(dir.path()).await;
    actor.journal.as_mut().unwrap().fail_next_sync();
    assert!(
        actor
            .persist("unknown_durability", actor.ledger.clone())
            .await
            .is_err()
    );
    assert_eq!(actor.ledger.phase, LivePhase::Faulted);
    assert!(
        actor
            .control("bad-reset".into(), ControlAction::Reset)
            .await
            .unwrap_err()
            .contains("poisoned")
    );
    actor.next_heartbeat = Instant::now();
    actor.pulse().await;
    assert!(!actor.heartbeating);
}
#[tokio::test]
async fn finalized_failure_halts_and_keeps_cash_uncredited() {
    let dir = tempfile::tempdir().unwrap();
    let (mut actor, _, prepared) = fixture(dir.path()).await;
    actor.ledger.chain_pending = Some(prepared.clone());
    let mut failed = receipt(&prepared);
    failed.success = false;
    failed.consumed_tokens_micros = [0, 0];
    failed.collateral_delta_micros = 0;
    actor
        .result(ResultEvent::ChainObserved(Ok(ChainObservation::Finalized(
            failed,
        ))))
        .await;
    assert_eq!(actor.ledger.phase, LivePhase::Faulted);
    assert_eq!(actor.ledger.returned_collateral_micros, 0);
    assert_eq!(actor.ledger.positions.len(), 2);
    assert!(actor.ledger.chain_pending.is_none());
    assert_eq!(actor.ledger.session_gas_cost_wei, "1000");
}
#[tokio::test]
async fn pending_signed_transaction_survives_journal_replay_without_resume() {
    let dir = tempfile::tempdir().unwrap();
    let (mut actor, venue, prepared) = fixture(dir.path()).await;
    actor.ledger.chain_pending = Some(prepared.clone());
    actor
        .journal
        .as_mut()
        .unwrap()
        .commit("signed_chain_intent_before_broadcast", &actor.ledger)
        .unwrap();
    let settings = actor.settings.as_ref().clone();
    actor.listener.abort();
    drop(actor);
    let handle = LiveRuntime::start_with_venue(settings.clone(), dir.path().into(), venue.clone())
        .await
        .unwrap();
    assert_eq!(handle.snapshot().phase, LivePhase::Paused);
    assert_eq!(
        handle.snapshot().chain_pending_hash.as_deref(),
        Some(prepared.hash.as_str())
    );
    handle.shutdown().await.unwrap();
    assert!(venue.broadcasts.lock().await.is_empty());
    let (journal, ledger) = Journal::open(dir.path(), &settings).unwrap();
    assert_eq!(
        ledger.chain_pending.unwrap().raw_transaction,
        prepared.raw_transaction
    );
    drop(journal);
}

#[tokio::test]
async fn deposit_lost_ack_and_restart_never_resend_and_hide_signed_body() {
    let dir = tempfile::tempdir().unwrap();
    let (mut actor, venue, prepared) = fixture_kind(dir.path(), true).await;
    actor
        .result(ResultEvent::ChainPrepared(Box::new((
            prepared.plan.clone(),
            Ok(prepared.clone()),
        ))))
        .await;
    let event = tokio::time::timeout(Duration::from_secs(5), actor.results.recv())
        .await
        .unwrap()
        .unwrap();
    actor.result(event).await;
    assert!(
        actor
            .ledger
            .chain_pending
            .as_ref()
            .unwrap()
            .relayer
            .as_ref()
            .unwrap()
            .dispatched
    );
    for _ in 0..3 {
        actor
            .result(ResultEvent::ChainObserved(Ok(ChainObservation::Unknown)))
            .await;
        actor.next_chain = Instant::now();
        actor.pulse().await;
        let event = tokio::time::timeout(Duration::from_secs(5), actor.results.recv())
            .await
            .unwrap()
            .unwrap();
        actor.result(event).await;
    }
    assert_eq!(venue.broadcasts.lock().await.len(), 1);
    let serialized = serde_json::to_string(actor.publish.borrow().as_ref()).unwrap();
    assert!(!serialized.contains(&prepared.relayer.as_ref().unwrap().signature));
    assert!(!serialized.contains("depositWalletParams"));
    assert_eq!(
        actor.publish.borrow().chain_pending_kind.as_deref(),
        Some("batch")
    );
    let settings = actor.settings.clone();
    actor.journal.take();
    let (_journal, recovered) = Journal::open(dir.path(), &settings).unwrap();
    assert!(recovered.chain_pending.unwrap().relayer.unwrap().dispatched);
}
#[tokio::test]
async fn deposit_dispatch_sync_failure_prevents_post_and_reset() {
    let dir = tempfile::tempdir().unwrap();
    let (mut actor, venue, prepared) = fixture_kind(dir.path(), true).await;
    let mut next = actor.ledger.clone();
    next.chain_pending = Some(prepared.clone());
    actor
        .persist("signed_chain_intent_before_broadcast", next)
        .await
        .unwrap();
    actor.journal.as_mut().unwrap().fail_next_sync();
    actor.launch_chain_submit(prepared).await;
    assert!(venue.broadcasts.lock().await.is_empty());
    assert!(actor.journal.as_ref().unwrap().is_poisoned());
    assert!(
        actor
            .control("reset-poison-deposit".into(), ControlAction::Reset)
            .await
            .is_err()
    );
}
#[tokio::test]
async fn deposit_receipt_uses_actual_hash_and_zero_owner_gas_once() {
    let dir = tempfile::tempdir().unwrap();
    let (mut actor, _, mut prepared) = fixture_kind(dir.path(), true).await;
    prepared.relayer.as_mut().unwrap().dispatched = true;
    actor.ledger.chain_pending = Some(prepared.clone());
    let mut observed = receipt(&prepared);
    observed.transaction_hash = format!("0x{}", "c".repeat(64));
    observed.intent_hash = Some(prepared.hash.clone());
    observed.gas_cost_wei = "0".into();
    observed.sponsored_gas_cost_wei = Some("1000".into());
    actor
        .result(ResultEvent::ChainObserved(Ok(ChainObservation::Finalized(
            observed.clone(),
        ))))
        .await;
    assert_eq!(actor.ledger.returned_collateral_micros, 5_000_000);
    assert_eq!(actor.ledger.session_gas_cost_wei, "0");
    assert_eq!(
        actor.ledger.chain_receipts[0].transaction_hash,
        observed.transaction_hash
    );
    assert_ne!(
        actor.ledger.chain_receipts[0].transaction_hash,
        prepared.hash
    );
    assert!(actor.ledger.apply_chain_receipt(&observed).is_err());
}
#[tokio::test]
async fn deposit_expiry_releases_batch_without_credit_and_requires_reset() {
    let dir = tempfile::tempdir().unwrap();
    let (mut actor, _, mut prepared) = fixture_kind(dir.path(), true).await;
    prepared.relayer.as_mut().unwrap().dispatched = true;
    let relay = prepared.relayer.as_ref().unwrap();
    let expiry = crate::relay::RelayExpiry {
        intent_hash: prepared.hash.clone(),
        block_number: 200,
        block_hash: format!("0x{}", "d".repeat(64)),
        finalized_timestamp_seconds: relay.deadline + 1,
        wallet_nonce: prepared.nonce,
        retained_tokens_micros: prepared.before_tokens_micros,
        collateral_micros: 99_000_000,
    };
    actor.ledger.chain_pending = Some(prepared.clone());
    let positions = serde_json::to_string(&actor.ledger.positions).unwrap();
    let spend = actor.ledger.session_spent_pico.clone();
    let daily = actor.ledger.daily_loss_micros;
    let mut forged = expiry.clone();
    forged.wallet_nonce += 1;
    assert!(actor.ledger.apply_chain_expiry(&forged).is_err());
    forged = expiry.clone();
    forged.finalized_timestamp_seconds = relay.deadline;
    assert!(actor.ledger.apply_chain_expiry(&forged).is_err());
    forged = expiry.clone();
    forged.retained_tokens_micros[0] -= 1;
    assert!(actor.ledger.apply_chain_expiry(&forged).is_err());
    actor
        .result(ResultEvent::ChainObserved(Ok(ChainObservation::Expired(
            expiry,
        ))))
        .await;
    assert!(actor.ledger.chain_pending.is_none());
    assert_eq!(actor.ledger.chain_expiries.len(), 1);
    assert_eq!(actor.ledger.phase, LivePhase::Faulted);
    assert_eq!(actor.ledger.returned_collateral_micros, 0);
    assert!(actor.ledger.chain_receipts.is_empty());
    assert_eq!(actor.ledger.session_spent_pico, spend);
    assert_eq!(actor.ledger.daily_loss_micros, daily);
    assert_eq!(
        serde_json::to_string(&actor.ledger.positions).unwrap(),
        positions
    );
    assert!(
        actor
            .control("resume-expired-without-reset".into(), ControlAction::Resume)
            .await
            .is_err()
    );
}
