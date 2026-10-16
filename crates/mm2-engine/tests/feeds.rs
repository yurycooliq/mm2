use mm2_engine::{
    domain::{FeedEvent, FeedSource, Market, Outcome},
    feeds::{
        MAX_MESSAGE_BYTES, enqueue_frame, parse_binance, parse_clob, parse_micro, validate_token,
    },
};
use serde_json::json;
use tokio::sync::{mpsc, watch};

fn market() -> Market {
    Market {
        slug: "btc-updown-5m-1790856000".into(),
        condition_id: format!("0x{}", "11".repeat(32)),
        up_token: "73327808031010435596903409498962573760099256223010321762163147989598638640221"
            .into(),
        down_token: "32895151594675070760394649172955470220743603895601698982678584912731739131164"
            .into(),
        window_start_ms: 1790856000000,
        window_end_ms: 1790856300000,
        tick_micro: 1000,
    }
}

fn snapshot(market: &Market) -> serde_json::Value {
    json!({"event_type":"book","market":market.condition_id,"asset_id":market.up_token,
        "timestamp":"1790856000123","bids":[{"price":"0.400000","size":"1.234567"}],
        "asks":[{"price":"0.600000","size":"10"}]})
}

fn change(market: &Market) -> serde_json::Value {
    json!({"event_type":"price_change","market":market.condition_id,"timestamp":"1790856000124",
        "price_changes":[{"asset_id":market.up_token,"price":"0.400000","size":"0","side":"BUY","best_bid":"0.3","best_ask":"0.6"},
            {"asset_id":market.down_token,"price":"0.500000","size":"2.5","side":"SELL"}]})
}

fn binance() -> serde_json::Value {
    json!({"e":"trade","E":1790856000999_u64,"T":1790856000123_u64,"s":"BTCUSDT","t":123,
        "p":"100123.12000000","q":"0.00000001","m":false,"M":true})
}

#[test]
fn canonical_integer_units_and_token_256_bounds_are_exact() {
    assert_eq!(parse_micro("0.40000000").unwrap(), 400000);
    assert_eq!(parse_micro("1.234567").unwrap(), 1234567);
    for value in ["0.0000001", "1e-3", "-0.1", " 1", "18446744073710"] {
        assert!(parse_micro(value).is_err());
    }
    let maximum = "115792089237316195423570985008687907853269984665640564039457584007913129639935";
    assert!(validate_token(maximum).is_ok());
    assert!(
        validate_token(
            "115792089237316195423570985008687907853269984665640564039457584007913129639936"
        )
        .is_err()
    );
    for value in ["001", "-1", "1e2", "", "0xabc"] {
        assert!(validate_token(value).is_err());
    }
}

#[test]
fn snapshots_preserve_source_clock_and_independent_local_receipt() {
    let selected = market();
    let events = parse_clob(&snapshot(&selected).to_string(), &selected, 17).unwrap();
    let FeedEvent::Snapshot {
        outcome,
        exchange_ms,
        observed_mono_ms,
        bids,
        asks,
        ..
    } = &events[0]
    else {
        panic!("snapshot expected");
    };
    assert_eq!(*outcome, Outcome::Up);
    assert_eq!(*exchange_ms, 1790856000123);
    assert_eq!(*observed_mono_ms, 17);
    assert_eq!(bids[0].quantity_micro, 1234567);
    assert_eq!(asks[0].price_micro, 600000);
}

#[test]
fn complete_delta_is_atomic_and_zero_is_a_level_removal() {
    let selected = market();
    let events = parse_clob(&change(&selected).to_string(), &selected, 18).unwrap();
    assert_eq!(events.len(), 1);
    let FeedEvent::Delta { changes, .. } = &events[0] else {
        panic!("atomic delta expected");
    };
    assert_eq!(changes.len(), 2);
    assert_eq!(changes[0].level.quantity_micro, 0);
    assert_eq!(changes[1].outcome, Outcome::Down);
    let mut invalid = change(&selected);
    invalid["price_changes"][1]["size"] = json!(0.1);
    assert!(parse_clob(&invalid.to_string(), &selected, 19).is_err());
    let frame = json!([snapshot(&selected), invalid]);
    assert!(parse_clob(&frame.to_string(), &selected, 20).is_err());
}

#[test]
fn foreign_tokens_are_filtered_and_wrong_selected_mapping_fails() {
    let selected = market();
    let mut other = snapshot(&selected);
    other["market"] = json!(format!("0x{}", "22".repeat(32)));
    other["asset_id"] = json!("123");
    assert!(
        parse_clob(&other.to_string(), &selected, 1)
            .unwrap()
            .is_empty()
    );
    other["asset_id"] = json!(selected.up_token);
    assert!(parse_clob(&other.to_string(), &selected, 2).is_err());
    other["market"] = json!(selected.condition_id);
    other["asset_id"] = json!("123");
    assert!(parse_clob(&other.to_string(), &selected, 3).is_err());
}

#[test]
fn invalid_snapshot_duplicates_numeric_decimals_and_overlarge_messages_fail() {
    let selected = market();
    let mut invalid = snapshot(&selected);
    invalid["bids"] = json!([{"price":"0.4","size":"1"},{"price":"0.400000","size":"2"}]);
    assert!(parse_clob(&invalid.to_string(), &selected, 1).is_err());
    invalid = snapshot(&selected);
    invalid["asks"][0]["price"] = json!(0.6);
    assert!(parse_clob(&invalid.to_string(), &selected, 1).is_err());
    assert!(parse_clob(&"x".repeat(MAX_MESSAGE_BYTES + 1), &selected, 1).is_err());
    assert!(parse_binance(&"x".repeat(MAX_MESSAGE_BYTES + 1), 1).is_err());
}

#[test]
fn public_trades_do_not_become_our_fills_and_ticks_keep_actual_versions() {
    let selected = market();
    let trade = json!({"event_type":"last_trade_price","market":selected.condition_id,"asset_id":selected.up_token,
        "timestamp":"1790856000125","price":"0.5","size":"10","side":"BUY","fee_rate_bps":"700"});
    assert!(
        parse_clob(&trade.to_string(), &selected, 1)
            .unwrap()
            .is_empty()
    );
    let tick = json!({"event_type":"tick_size_change","market":selected.condition_id,"asset_id":selected.down_token,
        "timestamp":"1790856000126","old_tick_size":"0.01","new_tick_size":"0.001"});
    let events = parse_clob(&tick.to_string(), &selected, 2).unwrap();
    let FeedEvent::Tick {
        outcome,
        old_tick_micro,
        new_tick_micro,
        ..
    } = &events[0]
    else {
        panic!("tick expected");
    };
    assert_eq!(*outcome, Outcome::Down);
    assert_eq!(*old_tick_micro, Some(10000));
    assert_eq!(*new_tick_micro, 1000);
}

#[test]
fn lifecycle_without_clock_is_never_a_synthetic_book_update() {
    let selected = market();
    let created = json!({"event_type":"new_market","market":selected.condition_id,"slug":selected.slug,
        "assets_ids":[selected.up_token,selected.down_token],"outcomes":["Up","Down"]});
    assert!(
        parse_clob(&created.to_string(), &selected, 1)
            .unwrap()
            .is_empty()
    );
    let resolved = json!({"event_type":"market_resolved","market":selected.condition_id,
        "assets_ids":[selected.up_token,selected.down_token],"winning_asset_id":selected.down_token,"winning_outcome":"Down"});
    let events = parse_clob(&resolved.to_string(), &selected, 2).unwrap();
    assert!(matches!(
        &events[0],
        FeedEvent::Disconnect {
            source: FeedSource::Clob,
            ..
        }
    ));
}

#[test]
fn binance_uses_trade_clock_and_never_claims_oracle_identity() {
    let events = parse_binance(&binance().to_string(), 23).unwrap();
    let FeedEvent::Reference {
        price_micro,
        exchange_ms,
        observed_mono_ms,
    } = &events[0]
    else {
        panic!("reference expected");
    };
    assert_eq!(*price_micro, 100123120000);
    assert_eq!(*exchange_ms, 1790856000123);
    assert_eq!(*observed_mono_ms, 23);
    for update in [
        json!({"s":"ETHUSDT"}),
        json!({"p":100123.12}),
        json!({"T":1790856000123.5}),
        json!({"q":"0"}),
    ] {
        let mut invalid = binance();
        for (name, value) in update.as_object().unwrap() {
            invalid[name] = value.clone();
        }
        assert!(parse_binance(&invalid.to_string(), 24).is_err());
    }
}

#[tokio::test(start_paused = true)]
async fn queue_timeout_latches_fault_and_preserves_the_admitted_prefix() {
    let (tx, mut rx) = mpsc::channel(1);
    let (fault, state) = watch::channel(None);
    let (_stop, mut shutdown) = watch::channel(false);
    let events = vec![
        FeedEvent::Disconnect {
            source: FeedSource::Clob,
            reason: "first".into(),
        },
        FeedEvent::Disconnect {
            source: FeedSource::Binance,
            reason: "second".into(),
        },
    ];
    assert!(
        enqueue_frame(&tx, events, &fault, &mut shutdown)
            .await
            .is_err()
    );
    assert_eq!(
        state.borrow().as_deref(),
        Some("feed backpressure deadline exceeded")
    );
    assert!(matches!(
        rx.try_recv().unwrap(),
        FeedEvent::Disconnect {
            source: FeedSource::Clob,
            ..
        }
    ));
    assert!(rx.try_recv().is_err());
    drop(rx);
    assert!(
        enqueue_frame(
            &tx,
            vec![FeedEvent::Disconnect {
                source: FeedSource::Clob,
                reason: "closed".into()
            }],
            &fault,
            &mut shutdown,
        )
        .await
        .is_err()
    );
    assert_eq!(
        state.borrow().as_deref(),
        Some("feed backpressure deadline exceeded")
    );
}
