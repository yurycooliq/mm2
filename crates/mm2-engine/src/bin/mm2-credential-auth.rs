//! Private stdin/stdout utility: no secret argv, logs, key creation or funded calls.
use mm2_live::credentials::{ExistingCredentialRequest, MAX_INPUT_BYTES};
use std::{io::Write, time::Duration};
use tokio::io::AsyncReadExt;

const REVIEWED_SIGNER: &str = "0xa2300f310b59df6d0dec94576776ce5435bd0b3f";

fn fail(class: &'static str) -> ! {
    eprintln!("{class}");
    std::process::exit(1);
}

fn main() {
    if std::env::args_os().len() != 1 {
        fail("clob_invalid_arguments");
    }
    if mm2_engine::tls::initialize().is_err() {
        fail("clob_tls_initialization_failed");
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap_or_else(|_| fail("clob_runtime_failed"));
    let payload = runtime.block_on(async {
        let mut raw = Vec::new();
        let mut input = tokio::io::stdin().take((MAX_INPUT_BYTES + 1) as u64);
        match tokio::time::timeout(Duration::from_secs(10), input.read_to_end(&mut raw)).await {
            Ok(Ok(_)) => (),
            _ => fail("clob_input_failed"),
        }
        let request = ExistingCredentialRequest::parse(&raw, REVIEWED_SIGNER);
        raw.fill(0);
        let request = request.unwrap_or_else(|error| fail(error.public_class()));
        request
            .derive_existing()
            .await
            .unwrap_or_else(|error| fail(error.public_class()))
    });
    if std::io::stdout().lock().write_all(&payload).is_err() {
        fail("clob_output_failed");
    }
}
