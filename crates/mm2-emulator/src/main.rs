use mm2_emulator::{Tape, run_tape};
fn main() {
    if let Err(error) = run() {
        eprintln!("mm2-emulator: {error}");
        std::process::exit(1);
    }
}
fn run() -> Result<(), String> {
    let mut args = std::env::args().skip(1);
    let path = args.next().ok_or("usage: mm2-emulator <event-tape.json>")?;
    if args.next().is_some() {
        return Err("usage: mm2-emulator <event-tape.json>".into());
    }
    let metadata = std::fs::metadata(&path).map_err(|e| e.to_string())?;
    if metadata.len() > 2 * 1024 * 1024 {
        return Err("event tape exceeds 2 MiB".into());
    }
    let bytes = std::fs::read(path).map_err(|e| e.to_string())?;
    let tape: Tape = serde_json::from_slice(&bytes).map_err(|e| e.to_string())?;
    let result = run_tape(&tape)?;
    println!(
        "{}",
        serde_json::to_string_pretty(&result.snapshot()).map_err(|e| e.to_string())?
    );
    Ok(())
}
