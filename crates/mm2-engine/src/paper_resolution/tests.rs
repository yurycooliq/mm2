use super::*;
use axum::{
    Json, Router,
    routing::{get, post},
};
use std::sync::{Arc, Mutex};

pub(crate) fn evidence(target: Target, winner_up: bool, at_ms: u64) -> Evidence {
    Evidence {
        target,
        chain_id: 137,
        conditional_tokens: format!("{CTF:#x}"),
        token_ids: ["11".into(), "12".into()],
        block_number: 123,
        block_hash: format!("0x{}", "b".repeat(64)),
        block_timestamp_ms: at_ms,
        observed_at_ms: at_ms,
        payout_numerators: if winner_up { [1, 0] } else { [0, 1] },
        payout_denominator: 1,
        metadata_sha256: "c".repeat(64),
    }
}
fn target() -> Target {
    Target {
        condition_id: format!("0x{}", "a".repeat(64)),
        start_ms: 0,
        end_ms: 300_000,
    }
}
fn metadata() -> Value {
    json!({"slug":"btc-updown-5m-0","conditionId":target().condition_id,"active":false,"closed":true,
        "acceptingOrders":false,"negRisk":false,"eventStartTime":"1970-01-01T00:00:00Z","endDate":"1970-01-01T00:05:00Z",
        "outcomes":["Up","Down"],"clobTokenIds":["11","12"],"outcomePrices":["0","1"],
        "umaResolutionStatus":"resolved","orderPriceMinTickSize":0.01,
        "cryptoMarketConfig":{"asset":"btc","duration":"5m","twapEnabled":true,"twapLookbackSeconds":60}})
}
#[derive(Clone)]
struct Mock {
    denominator: u64,
    split: bool,
    reorg: bool,
    bad_token: bool,
    chain: u64,
    calls: Arc<Mutex<Vec<Value>>>,
}
impl Default for Mock {
    fn default() -> Self {
        Self {
            denominator: 1,
            split: false,
            reorg: false,
            bad_token: false,
            chain: 137,
            calls: Default::default(),
        }
    }
}
async fn mock(settings: Mock) -> (Client, tokio::task::JoinHandle<()>) {
    let router = Router::new().route("/markets/slug/btc-updown-5m-0",get(||async {Json(metadata())}))
        .route("/",post(move |Json(body):Json<Value>| {
            let settings = settings.clone();
            async move {
                settings.calls.lock().unwrap().push(body.clone());
                let result = match body["method"].as_str().unwrap() {
                    "eth_chainId"=>json!(format!("0x{:x}",settings.chain)),
                    "eth_getBlockByNumber"=>json!({"number":"0x7b","hash":format!("0x{}",if settings.reorg && body["params"][0]!="finalized" {"c"}else{"b"}.repeat(64)),"timestamp":"0x12c"}),
                    "eth_call"=> {
                        assert_eq!(body["params"][1]["blockHash"],format!("0x{}","b".repeat(64)));
                        assert_eq!(body["params"][1]["requireCanonical"],true);
                        assert_eq!(body["params"][0]["to"],format!("{CTF:#x}"));
                        let encoded = hex::decode(body["params"][0]["data"].as_str().unwrap().trim_start_matches("0x")).unwrap();
                        let selector = &encoded[..4];
                        let value = if selector==ConditionalTokens::getOutcomeSlotCountCall::SELECTOR {2}
                            else if selector==ConditionalTokens::payoutDenominatorCall::SELECTOR {settings.denominator}
                            else if selector==ConditionalTokens::getCollectionIdCall::SELECTOR {encoded[99] as u64}
                            else if selector==ConditionalTokens::getPositionIdCall::SELECTOR {encoded[67] as u64+10+u64::from(settings.bad_token)}
                            else if selector==ConditionalTokens::payoutNumeratorsCall::SELECTOR {u64::from(encoded[67]==0 || settings.split)}
                            else {panic!("unexpected RPC selector")};
                        json!(format!("0x{value:064x}"))
                    },
                    _=>panic!("write or unexpected RPC method"),
                };
                Json(json!({"jsonrpc":"2.0","id":1,"result":result}))
            }
        }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (
        Client {
            http: reqwest::Client::builder()
                .no_proxy()
                .timeout(Duration::from_secs(2))
                .build()
                .unwrap(),
            rpc_url: format!("{origin}/"),
            gamma: origin,
        },
        server,
    )
}
#[tokio::test]
async fn final_ctf_outcome_overrides_end_prices_with_pinned_block_and_exact_tokens() {
    let state = Mock::default();
    let calls = state.calls.clone();
    let (client, server) = mock(state).await;
    let proof = client.observe(&target()).await.unwrap().unwrap();
    assert_eq!(proof.winner().unwrap(), mm2_core::Outcome::Up);
    assert_eq!(proof.token_ids, ["11", "12"]);
    assert_eq!(
        calls
            .lock()
            .unwrap()
            .iter()
            .filter(|v| v["method"] == "eth_call")
            .count(),
        8
    );
    server.abort();
}
#[tokio::test]
async fn no_resolution_or_non_binary_result_never_becomes_a_winning_receipt() {
    for (d, split) in [(0, false), (2, true)] {
        let (client, server) = mock(Mock {
            denominator: d,
            split,
            ..Default::default()
        })
        .await;
        let result = client.observe(&target()).await;
        if d == 0 {
            assert_eq!(result.unwrap(), None);
        } else {
            assert!(result.unwrap_err().contains("non-binary"));
        }
        server.abort();
    }
}
#[tokio::test]
async fn chain_reorg_and_wrong_outcome_mapping_reject_without_credit() {
    for state in [
        Mock {
            chain: 1,
            ..Default::default()
        },
        Mock {
            reorg: true,
            ..Default::default()
        },
        Mock {
            bad_token: true,
            ..Default::default()
        },
    ] {
        let (client, server) = mock(state).await;
        assert!(client.observe(&target()).await.is_err());
        server.abort();
    }
}
#[test]
fn proposed_status_and_prices_cannot_replace_exact_market_identity() {
    let raw = metadata();
    assert!(tokens(&raw, &target()).is_ok());
    for (key, value) in [
        ("conditionId", json!(format!("0x{}", "d".repeat(64)))),
        ("outcomes", json!(["Down", "Up"])),
        ("endDate", json!("1970-01-01T00:10:00Z")),
        ("negRisk", json!(true)),
    ] {
        let mut changed = raw.clone();
        changed[key] = value;
        assert!(tokens(&changed, &target()).is_err());
    }
}
#[test]
fn replay_evidence_requires_finality_identity_and_supported_exact_payout() {
    let proof = evidence(target(), true, 300_000);
    proof.winner().unwrap();
    let mut invalid = Vec::new();
    let mut p = proof.clone();
    p.block_timestamp_ms = 299_999;
    invalid.push(p);
    let mut p = proof.clone();
    p.chain_id = 1;
    invalid.push(p);
    let mut p = proof.clone();
    p.payout_denominator = 0;
    invalid.push(p);
    let mut p = proof.clone();
    p.payout_numerators = [1, 1];
    invalid.push(p);
    let mut p = proof.clone();
    p.block_hash = format!("0x{}", "0".repeat(64));
    invalid.push(p);
    let mut p = proof.clone();
    p.token_ids[0] = p.token_ids[1].clone();
    invalid.push(p);
    for p in invalid {
        assert!(p.winner().is_err());
    }
}
