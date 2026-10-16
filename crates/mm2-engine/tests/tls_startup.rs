//! Isolated process tests exercise the production TLS provider and WSS connector.
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn dual_provider_graph_initializes_default_wss_tls_without_panicking() {
    // Both implementations really are present in this production dependency graph.
    assert!(
        !rustls::crypto::ring::default_provider()
            .cipher_suites
            .is_empty()
    );
    assert!(
        !rustls::crypto::aws_lc_rs::default_provider()
            .cipher_suites
            .is_empty()
    );
    assert!(rustls::crypto::CryptoProvider::get_default().is_none());
    mm2_engine::tls::initialize().unwrap();
    let _verified_config = rustls::ClientConfig::builder()
        .with_root_certificates(rustls::RootCertStore::empty())
        .with_no_client_auth();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("wss://{}/ws/market", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut header = [0; 5];
        socket.read_exact(&mut header).await.unwrap();
        // An actual TLS peer rejection must return an error, rather than panic
        // while constructing the connector. No trust or hostname check is disabled.
        socket.write_all(&[21, 3, 3, 0, 2, 2, 40]).await.unwrap();
        socket.shutdown().await.unwrap();
        header
    });
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        tokio_tungstenite::connect_async(&endpoint),
    )
    .await
    .expect("default WSS TLS handshake exceeded the fixture deadline");
    let header = tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(header[0], 22, "peer must receive a TLS handshake record");
    assert_eq!(header[1], 3);
    assert!(matches!(
        result,
        Err(tokio_tungstenite::tungstenite::Error::Io(_))
            | Err(tokio_tungstenite::tungstenite::Error::Tls(_))
    ));
}

#[tokio::test]
async fn healthcheck_cli_preserves_its_loopback_http_contract() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        let app = axum::Router::new().route("/healthz", axum::routing::get(|| async { "ready" }));
        axum::serve(listener, app).await.unwrap();
    });
    let child = tokio::task::spawn_blocking(move || {
        std::process::Command::new(env!("CARGO_BIN_EXE_mm2-engine"))
            .args(["healthcheck", "--port", &port.to_string()])
            .output()
            .unwrap()
    });
    let output = tokio::time::timeout(Duration::from_secs(5), child)
        .await
        .expect("healthcheck CLI exceeded its bounded HTTP deadline")
        .unwrap();
    server.abort();
    let _ = server.await;
    assert!(
        output.status.success(),
        "healthcheck must accept a healthy loopback server: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}
