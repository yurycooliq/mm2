//! Existing CLOB credentials only. The caller must initialize its TLS provider.
//! This utility signs wallet-control authentication, never an order or transaction.
use alloy::{
    primitives::{Address, B256, U256},
    signers::{Signer, local::PrivateKeySigner},
    sol,
    sol_types::{SolStruct, eip712_domain},
};
use base64::{
    Engine as _,
    engine::general_purpose::{URL_SAFE, URL_SAFE_NO_PAD},
};
use polymarket_client_sdk_v2::auth::{ExposeSecret, SecretString, Uuid};
use reqwest::header::{HeaderMap, HeaderValue};
use serde::{Deserialize, Serialize};
use std::{str::FromStr, time::Duration};

pub const MAX_INPUT_BYTES: usize = 4096;
pub const MAX_OUTPUT_BYTES: usize = 4096;
const MAX_RESPONSE_BYTES: usize = 65_536;
const CLOB_ENDPOINT: &str = "https://clob.polymarket.com";
const ATTESTATION: &str = "This message attests that I control the given wallet";

sol! {
    struct ClobAuth {
        address address;
        string timestamp;
        uint256 nonce;
        string message;
    }
}

/// Errors contain only fixed public classes; no wire data or signing material.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CredentialError {
    InvalidInput,
    IdentityMismatch,
    Transport,
    TimeInvalid,
    Signature,
    ExistingAbsent,
    AuthRejected,
    DeriveRejected,
    ResponseInvalid,
    ResponseTooLarge,
}

impl CredentialError {
    pub fn public_class(self) -> &'static str {
        match self {
            Self::InvalidInput => "clob_invalid_input",
            Self::IdentityMismatch => "clob_identity_mismatch",
            Self::Transport => "clob_transport_failed",
            Self::TimeInvalid => "clob_server_time_invalid",
            Self::Signature => "clob_auth_signature_failed",
            Self::ExistingAbsent => "clob_existing_credentials_absent",
            Self::AuthRejected => "clob_auth_rejected",
            Self::DeriveRejected => "clob_existing_derivation_rejected",
            Self::ResponseInvalid => "clob_response_invalid",
            Self::ResponseTooLarge => "clob_response_too_large",
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    private_key: String,
    signer_address: String,
    nonce: u32,
}

/// Deliberately has no Debug/Display implementation exposing the signing key.
pub struct ExistingCredentialRequest {
    signer: PrivateKeySigner,
}

impl ExistingCredentialRequest {
    pub fn parse(raw: &[u8], expected_signer: &str) -> Result<Self, CredentialError> {
        if raw.len() > MAX_INPUT_BYTES || !raw.is_ascii() {
            return Err(CredentialError::InvalidInput);
        }
        let input: Input =
            serde_json::from_slice(raw).map_err(|_| CredentialError::InvalidInput)?;
        let private_key = SecretString::from(input.private_key);
        let key = private_key
            .expose_secret()
            .strip_prefix("0x")
            .unwrap_or(private_key.expose_secret());
        if input.nonce != 0 || key.len() != 64 || !key.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(CredentialError::InvalidInput);
        }
        let expected = strict_address(expected_signer)?;
        if strict_address(&input.signer_address)? != expected {
            return Err(CredentialError::IdentityMismatch);
        }
        let signer = PrivateKeySigner::from_str(key)
            .map_err(|_| CredentialError::InvalidInput)?
            .with_chain_id(Some(137));
        if signer.address() != expected {
            return Err(CredentialError::IdentityMismatch);
        }
        Ok(Self { signer })
    }

    /// Two bounded GET requests to one fixed TLS-verified endpoint. No fallback.
    /// The private JSON result belongs only in a private provisioning pipe.
    pub async fn derive_existing(&self) -> Result<Vec<u8>, CredentialError> {
        self.derive_at(CLOB_ENDPOINT, false).await
    }

    async fn derive_at(&self, endpoint: &str, fixture: bool) -> Result<Vec<u8>, CredentialError> {
        let client = reqwest::Client::builder()
            .user_agent("mm2-credential-auth/0.1")
            .https_only(!fixture)
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|_| CredentialError::Transport)?;
        tokio::time::timeout(Duration::from_secs(25), async {
            let time = client
                .get(format!("{endpoint}/time"))
                .send()
                .await
                .map_err(|_| CredentialError::Transport)?;
            if time.status() != reqwest::StatusCode::OK {
                return Err(CredentialError::TimeInvalid);
            }
            let time = bounded_body(time).await?;
            let timestamp: u64 =
                serde_json::from_slice(&time).map_err(|_| CredentialError::TimeInvalid)?;
            // Unix seconds, rather than milliseconds, and a bounded supported era.
            if !(1_577_836_800..=4_102_444_800).contains(&timestamp) {
                return Err(CredentialError::TimeInvalid);
            }
            let headers = authentication_headers(&self.signer, timestamp).await?;
            let response = client
                .get(format!("{endpoint}/auth/derive-api-key"))
                .headers(headers)
                .send()
                .await
                .map_err(|_| CredentialError::Transport)?;
            match response.status() {
                reqwest::StatusCode::OK => (),
                reqwest::StatusCode::NOT_FOUND => return Err(CredentialError::ExistingAbsent),
                reqwest::StatusCode::UNAUTHORIZED | reqwest::StatusCode::FORBIDDEN => {
                    return Err(CredentialError::AuthRejected);
                }
                _ => return Err(CredentialError::DeriveRejected),
            }
            let wire: WireCredentials = serde_json::from_slice(&bounded_body(response).await?)
                .map_err(|_| CredentialError::ResponseInvalid)?;
            encode_payload(wire)
        })
        .await
        .map_err(|_| CredentialError::Transport)?
    }
}

fn strict_address(value: &str) -> Result<Address, CredentialError> {
    if value.len() != 42
        || !value.starts_with("0x")
        || !value.as_bytes()[2..].iter().all(u8::is_ascii_hexdigit)
    {
        return Err(CredentialError::InvalidInput);
    }
    let address = Address::from_str(value).map_err(|_| CredentialError::InvalidInput)?;
    if address.is_zero() {
        return Err(CredentialError::InvalidInput);
    }
    Ok(address)
}

fn attestation_hash(address: Address, timestamp: u64, nonce: u32, chain_id: u64) -> B256 {
    ClobAuth {
        address,
        timestamp: timestamp.to_string(),
        nonce: U256::from(nonce),
        message: ATTESTATION.into(),
    }
    .eip712_signing_hash(
        &eip712_domain! { name: "ClobAuthDomain", version: "1", chain_id: chain_id, },
    )
}

async fn authentication_headers(
    signer: &PrivateKeySigner,
    timestamp: u64,
) -> Result<HeaderMap, CredentialError> {
    let signature = signer
        .sign_hash(&attestation_hash(signer.address(), timestamp, 0, 137))
        .await
        .map_err(|_| CredentialError::Signature)?;
    let mut signature =
        HeaderValue::from_str(&signature.to_string()).map_err(|_| CredentialError::Signature)?;
    signature.set_sensitive(true);
    let mut headers = HeaderMap::new();
    headers.insert(
        "POLY_ADDRESS",
        HeaderValue::from_str(&format!("{:#x}", signer.address()))
            .map_err(|_| CredentialError::Signature)?,
    );
    headers.insert(
        "POLY_TIMESTAMP",
        HeaderValue::from_str(&timestamp.to_string()).map_err(|_| CredentialError::Signature)?,
    );
    headers.insert("POLY_NONCE", HeaderValue::from_static("0"));
    headers.insert("POLY_SIGNATURE", signature);
    Ok(headers)
}

async fn bounded_body(mut response: reqwest::Response) -> Result<Vec<u8>, CredentialError> {
    if response
        .content_length()
        .is_some_and(|n| n > MAX_RESPONSE_BYTES as u64)
    {
        return Err(CredentialError::ResponseTooLarge);
    }
    let mut body = Vec::new();
    while let Some(part) = response
        .chunk()
        .await
        .map_err(|_| CredentialError::Transport)?
    {
        if body.len().saturating_add(part.len()) > MAX_RESPONSE_BYTES {
            return Err(CredentialError::ResponseTooLarge);
        }
        body.extend_from_slice(&part);
    }
    Ok(body)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WireCredentials {
    #[serde(rename = "apiKey")]
    api_key: String,
    secret: String,
    passphrase: String,
}

#[derive(Serialize)]
struct ProvisionPayload {
    schema: u8,
    kind: &'static str,
    api_key: String,
    api_secret: String,
    api_passphrase: String,
}

fn encode_payload(wire: WireCredentials) -> Result<Vec<u8>, CredentialError> {
    let key = Uuid::from_str(&wire.api_key).map_err(|_| CredentialError::ResponseInvalid)?;
    if key.is_nil()
        || wire.api_key.len() != 36
        || key.to_string() != wire.api_key.to_ascii_lowercase()
        || wire.passphrase.is_empty()
        || wire.passphrase.len() > 256
        || !wire
            .passphrase
            .bytes()
            .all(|b| (33..=126).contains(&b) && !b"$'\"\\`".contains(&b))
    {
        return Err(CredentialError::ResponseInvalid);
    }
    let unpadded = wire.secret.trim_end_matches('=');
    if !(16..=512).contains(&unpadded.len())
        || wire.secret.len() - unpadded.len() > 2
        || !unpadded
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b))
    {
        return Err(CredentialError::ResponseInvalid);
    }
    let decoded = URL_SAFE_NO_PAD
        .decode(unpadded)
        .map_err(|_| CredentialError::ResponseInvalid)?;
    if !(16..=256).contains(&decoded.len()) || URL_SAFE_NO_PAD.encode(&decoded) != unpadded {
        return Err(CredentialError::ResponseInvalid);
    }
    let canonical_secret = URL_SAFE.encode(&decoded);
    if wire.secret != unpadded && wire.secret != canonical_secret {
        return Err(CredentialError::ResponseInvalid);
    }
    let payload = serde_json::to_vec(&ProvisionPayload {
        schema: 1,
        kind: "clob",
        api_key: key.to_string(),
        api_secret: canonical_secret,
        api_passphrase: wire.passphrase,
    })
    .map_err(|_| CredentialError::ResponseInvalid)?;
    if payload.len() > MAX_OUTPUT_BYTES {
        return Err(CredentialError::ResponseTooLarge);
    }
    Ok(payload)
}

#[cfg(test)]
#[path = "credentials_tests.rs"]
mod tests;
