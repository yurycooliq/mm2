use super::*;
use alloy::primitives::{Signature, address};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

// Public Hardhat fixture key from the pinned SDK's authentication vector.
const KEY: &str = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
const OWNER: &str = "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266";
const TIME: u64 = 1_791_266_400;
const API_KEY: &str = "7b1e2d60-6f9a-4dd7-8f3e-21b8f94c77a2";
const SECRET: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";

fn request() -> ExistingCredentialRequest {
    ExistingCredentialRequest::parse(
        &serde_json::to_vec(&json!({"private_key":KEY,"signer_address":OWNER,"nonce":0})).unwrap(),
        OWNER,
    )
    .unwrap()
}

fn credentials() -> Vec<u8> {
    serde_json::to_vec(&json!({"apiKey":API_KEY,"secret":SECRET,"passphrase":"fixture-passphrase"}))
        .unwrap()
}

struct Fixture {
    endpoint: String,
    requests: Arc<Mutex<Vec<String>>>,
    task: tokio::task::JoinHandle<()>,
}

impl Fixture {
    async fn new(responses: Vec<(u16, Vec<u8>, bool)>) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let observed = requests.clone();
        let location = endpoint.clone();
        let task = tokio::spawn(async move {
            let mut responses = responses.into_iter();
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut raw = Vec::new();
                while !raw.ends_with(b"\r\n\r\n") && raw.len() < 8192 {
                    let mut byte = [0];
                    if socket.read(&mut byte).await.unwrap() == 0 {
                        break;
                    }
                    raw.push(byte[0]);
                }
                observed
                    .lock()
                    .unwrap()
                    .push(String::from_utf8(raw).unwrap());
                let (status, body, chunked) =
                    responses
                        .next()
                        .unwrap_or((500, b"unexpected request".to_vec(), false));
                let header = if chunked {
                    format!(
                        "HTTP/1.1 {status} Fixture\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n"
                    )
                } else {
                    format!(
                        "HTTP/1.1 {status} Fixture\r\nContent-Length: {}\r\nLocation: {location}/must-not-follow\r\nConnection: close\r\n\r\n",
                        body.len()
                    )
                };
                if socket.write_all(header.as_bytes()).await.is_err() {
                    continue;
                }
                if chunked {
                    for chunk in body.chunks(4096) {
                        if socket
                            .write_all(format!("{:x}\r\n", chunk.len()).as_bytes())
                            .await
                            .is_err()
                            || socket.write_all(chunk).await.is_err()
                            || socket.write_all(b"\r\n").await.is_err()
                        {
                            break;
                        }
                    }
                    let _ = socket.write_all(b"0\r\n\r\n").await;
                } else {
                    let _ = socket.write_all(&body).await;
                }
            }
        });
        Self {
            endpoint,
            requests,
            task,
        }
    }

    fn assert_gets(&self, paths: &[&str]) {
        let requests = self.requests.lock().unwrap();
        assert_eq!(requests.len(), paths.len());
        for (request, path) in requests.iter().zip(paths) {
            assert!(request.starts_with(&format!("GET {path} HTTP/1.1\r\n")));
            assert!(!request.to_ascii_lowercase().contains("content-length:"));
            assert!(!request.contains(KEY));
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[tokio::test]
async fn official_sdk_signature_vector_and_polygon_authentication_domain() {
    let signer = PrivateKeySigner::from_str(KEY).unwrap();
    let hash = attestation_hash(signer.address(), 10_000_000, 23, 80_002);
    assert_eq!(
        signer.sign_hash(&hash).await.unwrap().to_string(),
        "0xf62319a987514da40e57e2f4d7529f7bac38f0355bd88bb5adbb3768d80de6c1682518e0af677d5260366425f4361e7b70c25ae232aff0ab2331e2b164a1aedc1b"
    );

    let headers = authentication_headers(&signer, TIME).await.unwrap();
    let signature = Signature::from_str(headers["POLY_SIGNATURE"].to_str().unwrap()).unwrap();
    let actual = attestation_hash(signer.address(), TIME, 0, 137);
    assert_eq!(
        signature.recover_address_from_prehash(&actual).unwrap(),
        signer.address()
    );
    assert_ne!(
        signature
            .recover_address_from_prehash(&attestation_hash(signer.address(), TIME, 0, 1))
            .unwrap(),
        signer.address()
    );
    assert_ne!(
        signature
            .recover_address_from_prehash(&attestation_hash(signer.address(), TIME, 1, 137))
            .unwrap(),
        signer.address()
    );
    let wrong_domain = ClobAuth {
        address: signer.address(),
        timestamp: TIME.to_string(),
        nonce: U256::ZERO,
        message: ATTESTATION.into(),
    }
    .eip712_signing_hash(&eip712_domain! {name:"ClobAuthDomain",version:"2",chain_id:137,});
    assert_ne!(
        signature
            .recover_address_from_prehash(&wrong_domain)
            .unwrap(),
        signer.address()
    );
    assert_eq!(headers["POLY_NONCE"], "0");
    assert!(headers["POLY_SIGNATURE"].is_sensitive());
    assert!(!format!("{headers:?}").contains(&signature.to_string()));
}

#[tokio::test]
async fn existing_derivation_uses_only_two_gets_and_returns_private_helper_payload() {
    let fixture = Fixture::new(vec![
        (200, TIME.to_string().into_bytes(), false),
        (200, credentials(), false),
    ])
    .await;
    let raw = request().derive_at(&fixture.endpoint, true).await.unwrap();
    let result: Value = serde_json::from_slice(&raw).unwrap();
    assert_eq!(
        result,
        json!({"schema":1,"kind":"clob","api_key":API_KEY,"api_secret":SECRET,"api_passphrase":"fixture-passphrase"})
    );
    assert!(raw.len() <= MAX_OUTPUT_BYTES);
    fixture.assert_gets(&["/time", "/auth/derive-api-key"]);
    let observed = fixture.requests.lock().unwrap();
    let derive = observed[1].to_ascii_lowercase();
    assert!(derive.contains(&format!("poly_address: {OWNER}\r\n")));
    assert!(derive.contains("poly_nonce: 0\r\n"));
    assert!(derive.contains(&format!("poly_timestamp: {TIME}\r\n")));
    assert!(derive.contains("poly_signature: 0x"));
    assert!(!observed[0].to_ascii_lowercase().contains("poly_signature:"));
}

#[tokio::test]
async fn production_transport_refuses_plaintext_before_any_request() {
    let fixture = Fixture::new(Vec::new()).await;
    assert_eq!(
        request()
            .derive_at(&fixture.endpoint, false)
            .await
            .unwrap_err(),
        CredentialError::Transport
    );
    fixture.assert_gets(&[]);
}

#[tokio::test]
async fn missing_rejected_and_redirected_existing_keys_never_fall_back_or_leak() {
    for (status, expected) in [
        (404, CredentialError::ExistingAbsent),
        (401, CredentialError::AuthRejected),
        (403, CredentialError::AuthRejected),
        (400, CredentialError::DeriveRejected),
        (302, CredentialError::DeriveRejected),
        (500, CredentialError::DeriveRejected),
    ] {
        let private_body = format!("wire-private {KEY} {SECRET} {API_KEY}").into_bytes();
        let fixture = Fixture::new(vec![
            (200, TIME.to_string().into_bytes(), false),
            (status, private_body, false),
        ])
        .await;
        let error = request()
            .derive_at(&fixture.endpoint, true)
            .await
            .unwrap_err();
        assert_eq!(error, expected);
        assert!(!format!("{error:?} {}", error.public_class()).contains(KEY));
        assert!(!error.public_class().contains(SECRET));
        fixture.assert_gets(&["/time", "/auth/derive-api-key"]);
    }
}

#[tokio::test]
async fn invalid_or_redirected_server_time_stops_before_signing_or_derivation() {
    for (status, body) in [
        (200, b"null".to_vec()),
        (200, b"1791266400000".to_vec()),
        (200, b"-1".to_vec()),
        (302, TIME.to_string().into_bytes()),
    ] {
        let fixture = Fixture::new(vec![(status, body, false)]).await;
        assert_eq!(
            request()
                .derive_at(&fixture.endpoint, true)
                .await
                .unwrap_err(),
            CredentialError::TimeInvalid
        );
        fixture.assert_gets(&["/time"]);
    }
}

#[tokio::test]
async fn malformed_and_oversized_credentials_are_bounded_without_raw_errors() {
    let duplicate = format!(
        "{{\"apiKey\":\"{API_KEY}\",\"secret\":\"{SECRET}\",\"passphrase\":\"fixture-passphrase\",\"secret\":\"{SECRET}\"}}"
    );
    let mut unexpected: Value = serde_json::from_slice(&credentials()).unwrap();
    unexpected["created"] = json!(true);
    for (body, chunked, error) in [
        (
            b"{\"secret\":\"private-malformed\"}".to_vec(),
            false,
            CredentialError::ResponseInvalid,
        ),
        (
            duplicate.into_bytes(),
            false,
            CredentialError::ResponseInvalid,
        ),
        (
            serde_json::to_vec(&unexpected).unwrap(),
            false,
            CredentialError::ResponseInvalid,
        ),
        (
            vec![b'a'; MAX_RESPONSE_BYTES + 1],
            false,
            CredentialError::ResponseTooLarge,
        ),
        (
            vec![b'a'; MAX_RESPONSE_BYTES + 1],
            true,
            CredentialError::ResponseTooLarge,
        ),
    ] {
        let fixture = Fixture::new(vec![
            (200, TIME.to_string().into_bytes(), false),
            (200, body, chunked),
        ])
        .await;
        assert_eq!(
            request()
                .derive_at(&fixture.endpoint, true)
                .await
                .unwrap_err(),
            error
        );
        fixture.assert_gets(&["/time", "/auth/derive-api-key"]);
    }
}

#[test]
fn strict_input_rejects_ambiguous_fields_identity_and_other_nonce_sets() {
    let base = json!({"private_key":KEY,"signer_address":OWNER,"nonce":0});
    for field in ["private_key", "signer_address", "nonce"] {
        let mut input = base.clone();
        input.as_object_mut().unwrap().remove(field);
        assert!(matches!(
            ExistingCredentialRequest::parse(&serde_json::to_vec(&input).unwrap(), OWNER),
            Err(CredentialError::InvalidInput)
        ));
    }
    for nonce in [
        json!(1),
        json!(-1),
        json!(true),
        json!("0"),
        json!(u64::MAX),
    ] {
        let mut input = base.clone();
        input["nonce"] = nonce;
        assert!(matches!(
            ExistingCredentialRequest::parse(&serde_json::to_vec(&input).unwrap(), OWNER),
            Err(CredentialError::InvalidInput)
        ));
    }
    let duplicate = format!(
        "{{\"private_key\":\"{KEY}\",\"signer_address\":\"{OWNER}\",\"nonce\":0,\"nonce\":0}}"
    );
    assert!(matches!(
        ExistingCredentialRequest::parse(duplicate.as_bytes(), OWNER),
        Err(CredentialError::InvalidInput)
    ));
    let mut input = base.clone();
    input["endpoint"] = json!("https://attacker.invalid");
    assert!(matches!(
        ExistingCredentialRequest::parse(&serde_json::to_vec(&input).unwrap(), OWNER),
        Err(CredentialError::InvalidInput)
    ));
    let mut input = base.clone();
    input["signer_address"] = json!("0x0000000000000000000000000000000000000001");
    assert!(matches!(
        ExistingCredentialRequest::parse(&serde_json::to_vec(&input).unwrap(), OWNER),
        Err(CredentialError::IdentityMismatch)
    ));
    let raw = serde_json::to_vec(&base).unwrap();
    assert!(matches!(
        ExistingCredentialRequest::parse(
            &raw,
            &format!(
                "{:#x}",
                address!("0x0000000000000000000000000000000000000001")
            )
        ),
        Err(CredentialError::IdentityMismatch)
    ));
    for key in ["0".repeat(64), "f".repeat(64)] {
        let mut input = base.clone();
        input["private_key"] = json!(key);
        assert!(matches!(
            ExistingCredentialRequest::parse(&serde_json::to_vec(&input).unwrap(), OWNER),
            Err(CredentialError::InvalidInput)
        ));
    }
    assert!(matches!(
        ExistingCredentialRequest::parse(&vec![b' '; MAX_INPUT_BYTES + 1], OWNER),
        Err(CredentialError::InvalidInput)
    ));
}

#[test]
fn private_payload_canonicalizes_only_valid_existing_credentials() {
    let parse = |value: Value| serde_json::from_value::<WireCredentials>(value).unwrap();
    let mut value: Value = serde_json::from_slice(&credentials()).unwrap();
    value["secret"] = json!(SECRET.trim_end_matches('='));
    let result: Value =
        serde_json::from_slice(&encode_payload(parse(value.clone())).unwrap()).unwrap();
    assert_eq!(result["api_secret"], SECRET);
    for (field, bad) in [
        ("apiKey", "00000000-0000-0000-0000-000000000000"),
        ("secret", "not-a-valid-key"),
        ("secret", "AAAAAAAAAAAAAAAAAAAAAB"),
        ("passphrase", "unsafe$literal"),
        ("passphrase", ""),
    ] {
        let mut invalid = value.clone();
        invalid[field] = json!(bad);
        assert_eq!(
            encode_payload(parse(invalid)).unwrap_err(),
            CredentialError::ResponseInvalid
        );
    }
    let partial_padding = URL_SAFE.encode([0_u8; 31]);
    for malformed in [
        format!("{SECRET}="),
        partial_padding[..partial_padding.len() - 1].into(),
    ] {
        let mut invalid = value.clone();
        invalid["secret"] = json!(malformed);
        assert_eq!(
            encode_payload(parse(invalid)).unwrap_err(),
            CredentialError::ResponseInvalid
        );
    }
}
