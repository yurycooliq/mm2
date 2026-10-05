//! Offline vectors from the official MIT-licensed SDK 0.7.0, tests/order.rs.
//! Source revision: 222143d321eba97d5711a848265eb9aab3bc7ff4.
use super::*;

// Public Hardhat fixture key from the official SDK. Never owner runtime material.
const FIXTURE_KEY: &str = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
const EXPECTED_SIGNATURE: &str = concat!(
    "0xa3a093c83b6c20c83355c16ce94c92e6e9fcbdeb840618cc74f6c57a42ad145b",
    "2b98db73d2c73cbf1f2b6af288566ae81960ddbc3a13921027358a8bff3be6ff1c",
    "a440cbd865bc0c6243d7a8df9a8bf48a8827b0a4abbb61c30e96d305423af148",
    "d23d42d3ad94e65d78258cecaf8dcbaddac0f73dc085040f2c12bb595dd83804",
    "4f726465722875696e743235362073616c742c61646472657373206d616b65722c",
    "61646472657373207369676e65722c75696e7432353620746f6b656e49642c75",
    "696e74323536206d616b6572416d6f756e742c75696e743235362074616b6572",
    "416d6f756e742c75696e743820736964652c75696e7438207369676e61747572",
    "65547970652c75696e743235362074696d657374616d702c6279746573333220",
    "6d657461646174612c62797465733332206275696c6465722900ba"
);

fn signer() -> PrivateKeySigner {
    PrivateKeySigner::from_str(FIXTURE_KEY)
        .unwrap()
        .with_chain_id(Some(sdk::AMOY))
}

fn vector() -> (OrderV2, Eip712Domain) {
    let wallet = address("0x1111111111111111111111111111111111111111").unwrap();
    let mut order = OrderV2::default();
    order.salt = U256::from(479_249_096_354_u64);
    order.maker = wallet;
    order.signer = wallet;
    order.tokenId = U256::from(1234_u64);
    order.makerAmount = U256::from(100_000_000_u64);
    order.takerAmount = U256::from(50_000_000_u64);
    order.side = Side::Buy as u8;
    order.signatureType = SignatureType::Poly1271 as u8;
    order.timestamp = U256::from(1_710_000_000_000_u64);
    order.metadata = B256::ZERO;
    order.builder = B256::ZERO;
    let domain = Eip712Domain {
        name: Some("Polymarket CTF Exchange".into()),
        version: Some("2".into()),
        chain_id: Some(U256::from(sdk::AMOY)),
        verifying_contract: sdk::contract_config(sdk::AMOY, false).unwrap().exchange_v2,
        ..Default::default()
    };
    (order, domain)
}

#[test]
fn official_wrapped_vector_binds_owner_without_plain_order_hash_recovery() {
    let (order, domain) = vector();
    let signature = OrderSignature::Wrapped(EXPECTED_SIGNATURE.into());
    validate_sdk_signature(&order, &signature, &domain, signer().address()).unwrap();
    let bytes = hex::decode(&EXPECTED_SIGNATURE[2..]).unwrap();
    assert_eq!(bytes.len(), 317);
    let inner = Signature::from_raw(&bytes[..65]).unwrap();
    assert_ne!(
        inner
            .recover_address_from_prehash(&order.eip712_signing_hash(&domain))
            .unwrap(),
        signer().address()
    );
    assert_ne!(
        poly1271_digest(&order, &domain).unwrap(),
        order.eip712_signing_hash(&domain)
    );
}

#[tokio::test]
async fn official_sdk_type3_keeps_auth_eoa_and_serializes_exact_wrapped_signature() {
    let (order, domain) = vector();
    let signer = signer();
    // Supplied fixture credentials and cached token metadata make this entirely offline.
    let key = Uuid::nil();
    let client = Client::new("http://127.0.0.1:1", Config::default())
        .unwrap()
        .authentication_builder(&signer)
        .credentials(Credentials::new(key, "fixture".into(), "fixture".into()))
        .funder(order.maker)
        .signature_type(SignatureType::Poly1271)
        .authenticate()
        .await
        .unwrap();
    assert_eq!(client.address(), signer.address());
    assert_ne!(client.address(), order.signer);
    client.set_neg_risk(order.tokenId, false);
    let id = order.eip712_signing_hash(&domain);
    let signed = client
        .sign(
            &signer,
            sdk::clob::types::SignableOrder::builder()
                .payload(OrderPayload::new(order, U256::ZERO))
                .order_type(OrderType::GTC)
                .post_only(true)
                .build(),
        )
        .await
        .unwrap();
    assert_eq!(signed.signature.to_string(), EXPECTED_SIGNATURE);
    assert_eq!(signed.owner, key);
    let wire = serde_json::to_value(&signed).unwrap();
    assert_eq!(wire["order"]["maker"], wire["order"]["signer"]);
    assert_eq!(wire["order"]["signatureType"], 3);
    assert_eq!(wire["order"]["signature"], EXPECTED_SIGNATURE);
    assert_eq!(
        signed.payload.as_v2().unwrap().eip712_signing_hash(&domain),
        id
    );
    validate_sdk_signature(
        signed.payload.as_v2().unwrap(),
        &signed.signature,
        &domain,
        signer.address(),
    )
    .unwrap();
}

#[tokio::test]
async fn sdk_builder_type3_binds_deposit_units_and_rejects_v1() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    for version in [1, 2] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let host = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            while !request.ends_with(b"\r\n\r\n") {
                let mut byte = [0_u8; 1];
                assert_eq!(stream.read(&mut byte).await.unwrap(), 1);
                request.push(byte[0]);
                assert!(request.len() <= 4096);
            }
            assert!(request.starts_with(b"GET /version HTTP/1.1\r\n"));
            let body = format!("{{\"version\":{version}}}");
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
        });
        let signer = signer();
        let (vector, domain) = vector();
        let client = Client::new(&host, Config::default())
            .unwrap()
            .authentication_builder(&signer)
            .credentials(Credentials::new(
                Uuid::nil(),
                "fixture".into(),
                "fixture".into(),
            ))
            .funder(vector.maker)
            .signature_type(SignatureType::Poly1271)
            .authenticate()
            .await
            .unwrap();
        client.set_tick_size(vector.tokenId, sdk::clob::types::TickSize::Hundredth);
        client.set_neg_risk(vector.tokenId, false);
        let built = tokio::time::timeout(
            Duration::from_secs(2),
            client
                .limit_order()
                .token_id(vector.tokenId)
                .price(decimal(490_000))
                .size(decimal(5_000_000))
                .side(Side::Buy)
                .post_only(true)
                .build(),
        )
        .await
        .unwrap();
        if version == 1 {
            assert!(built.is_err());
        } else {
            let built = built.unwrap();
            let order = built.payload.as_v2().unwrap();
            assert_eq!(order.maker, vector.maker);
            assert_eq!(order.signer, vector.maker);
            assert_eq!(order.signatureType, 3);
            assert_eq!(order.makerAmount, U256::from(2_450_000));
            assert_eq!(order.takerAmount, U256::from(5_000_000));
            let signed = client.sign(&signer, built).await.unwrap();
            validate_sdk_signature(
                signed.payload.as_v2().unwrap(),
                &signed.signature,
                &domain,
                signer.address(),
            )
            .unwrap();
        }
        tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .unwrap()
            .unwrap();
    }
}

#[test]
fn deposit_wrapper_rejects_foreign_owner_and_changed_order_or_domain() {
    let (order, domain) = vector();
    let signature = OrderSignature::Wrapped(EXPECTED_SIGNATURE.into());
    assert!(validate_sdk_signature(&order, &signature, &domain, order.signer).is_err());
    let mut changed = order.clone();
    changed.makerAmount += U256::from(1);
    assert!(validate_sdk_signature(&changed, &signature, &domain, signer().address()).is_err());
    changed = order.clone();
    changed.signer = signer().address();
    assert!(validate_sdk_signature(&changed, &signature, &domain, signer().address()).is_err());
    let mut changed_domain = domain.clone();
    changed_domain.chain_id = Some(U256::from(137));
    assert!(
        validate_sdk_signature(&order, &signature, &changed_domain, signer().address()).is_err()
    );
    changed_domain = domain.clone();
    changed_domain.verifying_contract = Some(order.signer);
    assert!(
        validate_sdk_signature(&order, &signature, &changed_domain, signer().address()).is_err()
    );
}

#[test]
fn deposit_wrapper_rejects_each_modified_component_and_noncanonical_encoding() {
    let (order, domain) = vector();
    let original = hex::decode(&EXPECTED_SIGNATURE[2..]).unwrap();
    for index in [0, 32, 64, 65, 97, 129, 315, 316] {
        let mut bytes = original.clone();
        bytes[index] ^= 1;
        let signature = OrderSignature::Wrapped(format!("0x{}", hex::encode(bytes)));
        assert!(validate_sdk_signature(&order, &signature, &domain, signer().address()).is_err());
    }
    for encoded in [
        EXPECTED_SIGNATURE[..EXPECTED_SIGNATURE.len() - 2].to_owned(),
        format!("{EXPECTED_SIGNATURE}00"),
        EXPECTED_SIGNATURE.to_uppercase(),
        format!("0x{}", "g".repeat(634)),
    ] {
        assert!(
            validate_sdk_signature(
                &order,
                &OrderSignature::Wrapped(encoded),
                &domain,
                signer().address()
            )
            .is_err()
        );
    }
}

#[test]
fn deposit_wrapper_rejects_malleable_high_s_even_when_owner_recovery_matches() {
    let (order, domain) = vector();
    let mut bytes = hex::decode(&EXPECTED_SIGNATURE[2..]).unwrap();
    let curve_order =
        U256::from_str("0xfffffffffffffffffffffffffffffffebaaedce6af48a03bbfd25e8cd0364141")
            .unwrap();
    let s = U256::from_be_slice(&bytes[32..64]);
    bytes[32..64].copy_from_slice(&(curve_order - s).to_be_bytes::<32>());
    bytes[64] = if bytes[64] == 27 { 28 } else { 27 };
    let inner = Signature::from_raw(&bytes[..65]).unwrap();
    assert!(inner.normalize_s().is_some());
    assert_eq!(
        inner
            .recover_address_from_prehash(&poly1271_digest(&order, &domain).unwrap())
            .unwrap(),
        signer().address()
    );
    let wrapped = OrderSignature::Wrapped(format!("0x{}", hex::encode(bytes)));
    assert!(validate_sdk_signature(&order, &wrapped, &domain, signer().address()).is_err());
}

#[tokio::test]
async fn signature_representation_cannot_cross_eoa_and_deposit_wallet_modes() {
    let (mut order, domain) = vector();
    let signature = signer()
        .sign_hash(&order.eip712_signing_hash(&domain))
        .await
        .unwrap();
    assert!(
        validate_sdk_signature(&order, &signature.into(), &domain, signer().address()).is_err()
    );
    order.signatureType = 0;
    order.maker = signer().address();
    order.signer = signer().address();
    let signature = signer()
        .sign_hash(&order.eip712_signing_hash(&domain))
        .await
        .unwrap();
    validate_sdk_signature(&order, &signature.into(), &domain, signer().address()).unwrap();
    assert!(
        validate_sdk_signature(
            &order,
            &OrderSignature::Wrapped(EXPECTED_SIGNATURE.into()),
            &domain,
            signer().address()
        )
        .is_err()
    );
}

#[test]
fn deposit_settings_require_distinct_controller_account_and_explicit_relayer() {
    let contract = sdk::contract_config(137, false).unwrap();
    let mut settings = LiveSettings {
        owner_authorized: true,
        owner_address: "0x1111111111111111111111111111111111111111".into(),
        funder_address: "0x1111111111111111111111111111111111111111".into(),
        signer_address: format!("{:#x}", signer().address()),
        signature_type: 3,
        exchange_address: format!("{:#x}", contract.exchange_v2.unwrap()),
        collateral_address: format!("{:#x}", contract.collateral),
        allowed_market_prefixes: vec!["btc-updown-5m-".into()],
        ..Default::default()
    };
    assert!(settings.validate().is_err());
    settings.chain = Some(crate::chain::ChainSettings::default());
    assert!(settings.validate().is_err());
    settings.chain.as_mut().unwrap().relayer = Some(crate::relay::RelaySettings::default());
    settings.validate().unwrap();
    settings.signer_address = settings.funder_address.clone();
    assert!(settings.validate().is_err());
    settings.signer_address = format!("{:#x}", signer().address());
    settings.owner_address = settings.signer_address.clone();
    assert!(settings.validate().is_err());
    settings.signature_type = 4;
    assert!(settings.validate().is_err());
}

fn five_minute_fixture() -> (LiveQuote, serde_json::Value, MarketResponse) {
    // Publicly observed exact Gamma window and coarse CLOB date; mock-only token identifiers.
    let quote = LiveQuote {
        slot: "up".into(),
        condition_id: "0xfde14311948d3ed4fd27e731bcf122fec67b59136041acc6ecda905682e1f22b".into(),
        market_slug: "btc-updown-5m-1791208800".into(),
        token_id: "1234".into(),
        outcome: Outcome::Up,
        price_micros: 490_000,
        quantity_micros: 5_000_000,
        window_start_ms: 1_791_208_800_000,
        window_end_ms: 1_791_209_100_000,
        tick_micros: 10_000,
        min_order_micros: 5_000_000,
        fee_bps: 0,
        protocol: 2,
        neg_risk: false,
    };
    let raw = serde_json::json!({
        "conditionId": quote.condition_id, "slug": quote.market_slug,
        "active":true,"closed":false,"acceptingOrders":true,"negRisk":false,
        "eventStartTime":"2026-10-05T14:00:00Z", "endDate":"2026-10-05T14:05:00Z",
        "clobTokenIds":"[\"1234\",\"5678\"]", "outcomes":"[\"Up\",\"Down\"]",
        "cryptoMarketConfig":{"asset":"btc","duration":"5m","twapEnabled":true,"twapLookbackSeconds":60}
    });
    let market = MarketResponse::builder()
        .enable_order_book(true)
        .active(true)
        .closed(false)
        .archived(false)
        .accepting_orders(true)
        .minimum_order_size(Decimal::from(5))
        .minimum_tick_size(decimal(10_000))
        .condition_id(B256::from_str(&quote.condition_id).unwrap())
        .question("public window fixture")
        .description("")
        .market_slug(&quote.market_slug)
        .end_date_iso(
            chrono::DateTime::parse_from_rfc3339("2026-10-05T00:00:00Z")
                .unwrap()
                .with_timezone(&chrono::Utc),
        )
        .seconds_delay(0)
        .maker_base_fee(Decimal::ZERO)
        .taker_base_fee(Decimal::ZERO)
        .notifications_enabled(false)
        .neg_risk(false)
        .icon("")
        .image("")
        .rewards(
            sdk::clob::types::response::Rewards::builder()
                .min_size(Decimal::ZERO)
                .max_spread(Decimal::ZERO)
                .build(),
        )
        .is_50_50_outcome(true)
        .tokens(vec![
            sdk::clob::types::response::Token::builder()
                .token_id(U256::from(1234))
                .outcome("Up")
                .price(decimal(490_000))
                .winner(false)
                .build(),
            sdk::clob::types::response::Token::builder()
                .token_id(U256::from(5678))
                .outcome("Down")
                .price(decimal(490_000))
                .winner(false)
                .build(),
        ])
        .tags(vec![])
        .build();
    (quote, raw, market)
}

#[test]
fn observed_clob_midnight_date_does_not_replace_exact_gamma_five_minute_expiry() {
    let (mut quote, mut raw, mut market) = five_minute_fixture();
    for step in [0, 1] {
        if step == 1 {
            quote.window_start_ms += 300_000;
            quote.window_end_ms += 300_000;
            quote.market_slug = "btc-updown-5m-1791209100".into();
            quote.condition_id =
                "0x4d2055c4819aa536a4ae3995c0ed4543c3aae380067c8e4484c0328c09748cf3".into();
            raw["slug"] = quote.market_slug.clone().into();
            raw["conditionId"] = quote.condition_id.clone().into();
            raw["eventStartTime"] = "2026-10-05T14:05:00Z".into();
            raw["endDate"] = "2026-10-05T14:10:00Z".into();
            market.market_slug = quote.market_slug.clone();
            market.condition_id = Some(B256::from_str(&quote.condition_id).unwrap());
        }
        let gamma = gamma_identity(&raw, &quote).unwrap();
        assert_ne!(
            market.end_date_iso.unwrap().timestamp_millis() as u64,
            quote.window_end_ms
        );
        validate_clob_market(&market, &gamma, &quote).unwrap();
    }
}

#[test]
fn gamma_window_gate_rejects_wrong_identity_time_tokens_status_and_reference() {
    let (quote, raw, market) = five_minute_fixture();
    for (field, value) in [
        ("slug", serde_json::json!("btc-updown-5m-1791209100")),
        (
            "conditionId",
            serde_json::json!(format!("0x{}", "1".repeat(64))),
        ),
        ("endDate", serde_json::json!("2026-10-05T00:00:00Z")),
        ("eventStartTime", serde_json::json!("2026-10-05T14:05:00Z")),
        ("active", serde_json::json!(false)),
        ("closed", serde_json::json!(true)),
        ("acceptingOrders", serde_json::json!(false)),
        ("negRisk", serde_json::json!(true)),
        ("clobTokenIds", serde_json::json!(["5678", "1234"])),
        ("clobTokenIds", serde_json::json!(["1234", "1234"])),
        ("clobTokenIds", serde_json::json!(["01234", "5678"])),
        ("outcomes", serde_json::json!(["Down", "Up"])),
        (
            "cryptoMarketConfig",
            serde_json::json!({"asset":"btc","duration":"5m","twapEnabled":false,"twapLookbackSeconds":60}),
        ),
    ] {
        let mut changed = raw.clone();
        changed[field] = value;
        assert!(
            gamma_identity(&changed, &quote)
                .and_then(|gamma| validate_clob_market(&market, &gamma, &quote))
                .is_err(),
            "{field}"
        );
    }
    let mut changed_quote = quote.clone();
    changed_quote.window_end_ms += 1;
    assert!(gamma_identity(&raw, &changed_quote).is_err());
    changed_quote = quote.clone();
    changed_quote.window_start_ms += 1;
    assert!(gamma_identity(&raw, &changed_quote).is_err());
}

#[test]
fn clob_gate_retains_accepting_state_and_both_gamma_outcome_identities() {
    let (quote, raw, market) = five_minute_fixture();
    let gamma = gamma_identity(&raw, &quote).unwrap();
    for fault in 0..6 {
        let mut changed = market.clone();
        match fault {
            0 => changed.closed = true,
            1 => changed.accepting_orders = false,
            2 => changed.tokens[1].token_id = U256::from(9999),
            3 => changed.tokens[0].outcome = "Down".into(),
            4 => changed.condition_id = Some(B256::ZERO),
            _ => changed.market_slug = "btc-updown-5m-1791209100".into(),
        }
        assert!(validate_clob_market(&changed, &gamma, &quote).is_err());
    }
}
