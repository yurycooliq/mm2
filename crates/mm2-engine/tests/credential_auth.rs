//! CLI failure paths must never echo stdin secrets or touch the fixed remote API.
use serde_json::json;
use std::{
    io::Write,
    process::{Command, Stdio},
    time::Duration,
};

const FIXTURE_KEY: &str = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
const REVIEWED_OWNER: &str = "0xa2300f310b59df6d0dec94576776ce5435bd0b3f";

async fn rejected(raw: Vec<u8>, expected: &str) {
    let output = tokio::task::spawn_blocking(move || {
        let mut child = Command::new(env!("CARGO_BIN_EXE_mm2-credential-auth"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child.stdin.take().unwrap().write_all(&raw).unwrap();
        child.wait_with_output().unwrap()
    });
    let output = tokio::time::timeout(Duration::from_secs(5), output)
        .await
        .expect("invalid credential stdin must exit before any network request")
        .unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert_eq!(
        String::from_utf8(output.stderr).unwrap(),
        format!("{expected}\n")
    );
}

#[tokio::test]
async fn private_stdin_cli_rejects_invalid_payload_nonce_and_mismatched_saved_key() {
    let base = json!({"private_key":FIXTURE_KEY,"signer_address":REVIEWED_OWNER,"nonce":0});
    rejected(serde_json::to_vec(&base).unwrap(), "clob_identity_mismatch").await;
    let mut value = base.clone();
    value["nonce"] = json!(1);
    rejected(serde_json::to_vec(&value).unwrap(), "clob_invalid_input").await;
    let mut value = base.clone();
    value["private_key"] = json!("private-input-marker");
    rejected(serde_json::to_vec(&value).unwrap(), "clob_invalid_input").await;
    let mut value = base.clone();
    value["url"] = json!("https://private-input-marker.invalid");
    rejected(serde_json::to_vec(&value).unwrap(), "clob_invalid_input").await;
    rejected(
        b"private-input-marker is not JSON".to_vec(),
        "clob_invalid_input",
    )
    .await;
    rejected(vec![b'x'; 4097], "clob_invalid_input").await;
}

#[test]
fn secret_bearing_cli_arguments_are_never_echoed() {
    let output = Command::new(env!("CARGO_BIN_EXE_mm2-credential-auth"))
        .arg("private-argv-marker")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert_eq!(output.stderr, b"clob_invalid_arguments\n");
}
