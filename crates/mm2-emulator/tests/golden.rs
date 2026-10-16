use mm2_emulator::{Tape, run_tape};
#[test]
fn corrected_contract_golden_tapes() {
    for text in [
        include_str!("../../../fixtures/execution/shared-queue.json"),
        include_str!("../../../fixtures/execution/cancel-fill-race.json"),
        include_str!("../../../fixtures/execution/cancel-before-replace.json"),
        include_str!("../../../fixtures/execution/post-only-arrival.json"),
        include_str!("../../../fixtures/execution/budget-clipping.json"),
    ] {
        let value: serde_json::Value = serde_json::from_str(text).unwrap();
        let expected = &value["expected"];
        let tape: Tape = serde_json::from_value(value.clone()).unwrap();
        let session = run_tape(&tape).unwrap();
        session.validate().unwrap();
        let snapshot = session.snapshot();
        // These dated V1/V2 tapes must keep their original execution semantics.
        // New causality telemetry is explicit rather than rewriting the fixtures.
        assert_eq!(snapshot.venue_stats.causality_excluded_prints, 0);
        for order in &snapshot.venue_orders {
            assert!(
                order
                    .posted_at_ms
                    .is_some_and(|posted| posted <= snapshot.now_ms)
            );
        }
        assert_eq!(
            snapshot.account.up_inventory_micros,
            expected["up_inventory_micros"].as_u64().unwrap(),
            "{}",
            tape.name
        );
        assert_eq!(
            snapshot.account.down_inventory_micros,
            expected["down_inventory_micros"].as_u64().unwrap(),
            "{}",
            tape.name
        );
        assert_eq!(
            snapshot.account.spent_pico,
            expected["spent_pico"].as_str().unwrap(),
            "{}",
            tape.name
        );
        assert_eq!(
            snapshot.account.reserved_pico,
            expected["reserved_pico"].as_str().unwrap(),
            "{}",
            tape.name
        );
        assert_eq!(
            snapshot.account.available_pico,
            expected["available_pico"].as_str().unwrap(),
            "{}",
            tape.name
        );
        assert_eq!(
            snapshot.venue_stats.accepted_posts,
            expected["accepted_posts"].as_u64().unwrap(),
            "{}",
            tape.name
        );
        assert_eq!(
            snapshot.venue_stats.cancelled_orders,
            expected["cancelled_orders"].as_u64().unwrap(),
            "{}",
            tape.name
        );
        assert_eq!(
            snapshot.venue_stats.rejected_posts,
            expected["rejected_posts"].as_u64().unwrap(),
            "{}",
            tape.name
        );
    }
}
