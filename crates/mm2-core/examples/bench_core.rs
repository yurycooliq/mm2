use mm2_core::{Core, DesiredQuote, Observation, Outcome, RiskLimits};
use std::{hint::black_box, time::Instant};
fn main() {
    let mut core = Core::new(RiskLimits::default()).unwrap();
    let quotes = vec![
        DesiredQuote {
            slot: "up".into(),
            outcome: Outcome::Up,
            price_micros: 490_000,
            quantity_micros: 5_000_000,
        },
        DesiredQuote {
            slot: "down".into(),
            outcome: Outcome::Down,
            price_micros: 490_000,
            quantity_micros: 5_000_000,
        },
    ];
    core.quote(0, quotes.clone()).unwrap();
    for id in [1, 2] {
        core.observe(
            1,
            Observation {
                order_id: id,
                cumulative_filled_micros: 0,
                terminal: false,
            },
        )
        .unwrap();
    }
    for n in 0..10_000 {
        black_box(core.quote(n, quotes.clone()).unwrap());
    }
    let mut samples = Vec::with_capacity(100_000);
    for n in 0..100_000 {
        let started = Instant::now();
        black_box(core.quote(n, black_box(quotes.clone())).unwrap());
        samples.push(started.elapsed().as_nanos() as u64);
    }
    samples.sort_unstable();
    println!(
        "{}",
        serde_json::json!({"scope":"two_slot_core_quote_including_input_clone_and_timer","samples":samples.len(),"units":"ns","p50":samples[50_000],"p95":samples[95_000],"p99":samples[99_000],"max":samples[99_999],"network":false,"disk":false,"telemetry":false})
    );
}
