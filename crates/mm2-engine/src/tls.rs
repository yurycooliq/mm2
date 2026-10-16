//! The application owns the process-wide TLS provider before network workers start.

/// Reqwest and the venue SDK enable different Rustls providers. Their unified
/// feature set cannot choose an implicit default for WebSocket connectors.
/// Install the pinned AWS-LC provider without changing certificate verification.
pub fn initialize() -> Result<(), String> {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .map_err(|_| "TLS crypto provider was already installed before engine startup".into())
}
