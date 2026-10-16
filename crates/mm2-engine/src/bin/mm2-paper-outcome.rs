//! Operator read-only diagnostic. This binary has no control or funded operations.
use mm2_engine::{
    paper_resolution::{self, Target},
    tls,
};

#[tokio::main]
async fn main() -> Result<(), String> {
    tls::initialize()?;
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() != 2 {
        return Err("usage: mm2-paper-outcome <condition-id> <window-start-seconds>".into());
    }
    let start_ms = args[1]
        .parse::<u64>()
        .map_err(|_| "invalid market start")?
        .checked_mul(1000)
        .ok_or("window overflow")?;
    let target = Target {
        condition_id: args[0].clone(),
        start_ms,
        end_ms: start_ms.checked_add(300_000).ok_or("window overflow")?,
    };
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        paper_resolution::observe(&target),
    )
    .await
    .map_err(|_| "public outcome diagnostic deadline")??;
    println!(
        "{}",
        serde_json::to_string(
            &serde_json::json!({"read_only":true,"real_receipt":null,"evidence":result})
        )
        .map_err(|_| "evidence encoding failed")?
    );
    Ok(())
}
