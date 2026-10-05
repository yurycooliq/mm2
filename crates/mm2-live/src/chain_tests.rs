use super::*;
use std::sync::{Arc, Mutex};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

fn fixture_live(wallet: u8) -> (LiveSettings, PrivateKeySigner) {
    let signer = PrivateKeySigner::from_str(
        "0000000000000000000000000000000000000000000000000000000000000001",
    )
    .unwrap();
    let from = signer.address();
    let funder = match wallet {
        0 => from,
        1 => polymarket_client_sdk_v2::derive_proxy_wallet(from, 137).unwrap(),
        2 => polymarket_client_sdk_v2::derive_safe_wallet(from, 137).unwrap(),
        _ => unreachable!(),
    };
    let c = polymarket_client_sdk_v2::contract_config(137, false).unwrap();
    (
        LiveSettings {
            owner_authorized: true,
            signature_type: wallet,
            owner_address: format!("{funder:#x}"),
            funder_address: format!("{funder:#x}"),
            signer_address: format!("{from:#x}"),
            exchange_address: format!("{:#x}", c.exchange_v2.unwrap()),
            collateral_address: format!("{:#x}", c.collateral),
            allowed_conditions: vec![format!("0x{}", "11".repeat(32))],
            ..Default::default()
        },
        signer,
    )
}
fn plan(operation: ChainOperation) -> ChainPlan {
    ChainPlan {
        id: "fixture-closeout-1".into(),
        condition_id: format!("0x{}", "11".repeat(32)),
        token_ids: ["11".into(), "22".into()],
        operation,
    }
}
fn word(n: u64) -> String {
    format!("0x{n:064x}")
}
fn address_word(a: Address) -> String {
    hex_data(&U256::from_be_slice(a.as_slice()).to_be_bytes::<32>())
}
#[derive(Default)]
struct State {
    sent: Vec<String>,
    methods: Vec<String>,
    receipt: Option<Value>,
    after: bool,
    wrong_identity: bool,
    approved: bool,
    wrong_owner: bool,
    wrong_canonical: bool,
    low_finality: bool,
    lost_ack: bool,
    unresolved: bool,
    other_market_debit: bool,
    reorg_after_head: bool,
    head_reads: u64,
}
struct MockRpc {
    url: String,
    state: Arc<Mutex<State>>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for MockRpc {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl MockRpc {
    async fn start(live: LiveSettings) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let state = Arc::new(Mutex::new(State {
            approved: true,
            ..Default::default()
        }));
        let shared = state.clone();
        let task = tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let live = live.clone();
                let shared = shared.clone();
                tokio::spawn(async move {
                    let mut raw = Vec::new();
                    let mut buf = [0u8; 4096];
                    let (head_end, length);
                    loop {
                        let n = stream.read(&mut buf).await.unwrap();
                        if n == 0 {
                            return;
                        }
                        raw.extend_from_slice(&buf[..n]);
                        if let Some(index) = raw.windows(4).position(|p| p == b"\r\n\r\n") {
                            let header = String::from_utf8_lossy(&raw[..index]);
                            length = header
                                .lines()
                                .find_map(|l| {
                                    let (k, v) = l.split_once(':')?;
                                    k.eq_ignore_ascii_case("content-length")
                                        .then(|| v.trim().parse::<usize>().unwrap())
                                })
                                .unwrap();
                            head_end = index + 4;
                            break;
                        }
                        assert!(raw.len() < 65536);
                    }
                    while raw.len() < head_end + length {
                        let n = stream.read(&mut buf).await.unwrap();
                        assert!(n > 0);
                        raw.extend_from_slice(&buf[..n]);
                    }
                    let request: Value =
                        serde_json::from_slice(&raw[head_end..head_end + length]).unwrap();
                    let (result, lost) = {
                        let mut s = shared.lock().unwrap();
                        let method = request["method"].as_str().unwrap();
                        s.methods.push(method.into());
                        let result = reply(&live, &mut s, method, &request["params"]);
                        (result, method == "eth_sendRawTransaction" && s.lost_ack)
                    };
                    if lost {
                        return;
                    }
                    let body = serde_json::to_vec(&json!({"jsonrpc":"2.0","id":1,"result":result}))
                        .unwrap();
                    let header = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: {}\r\n\r\n",
                        body.len()
                    );
                    stream.write_all(header.as_bytes()).await.unwrap();
                    stream.write_all(&body).await.unwrap();
                });
            }
        });
        Self { url, state, task }
    }
}
fn reply(live: &LiveSettings, s: &mut State, method: &str, params: &Value) -> Value {
    let c = polymarket_client_sdk_v2::contract_config(137, false).unwrap();
    let underlying = literal_address!("0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174");
    match method {
        "eth_chainId" => json!("0x89"),
        "eth_blockNumber" => json!("0x64"),
        "eth_getTransactionCount" => json!("0x7"),
        "eth_estimateGas" => json!("0x493e0"),
        "eth_getBalance" => json!("0xde0b6b3a7640000"),
        "eth_getCode" => json!(
            "0x363d3d373d3d3d363d7344e999d5c2f66ef0861317f9a4805ac2e90aeb4f5af43d82803e903d91602b57fd5bf3"
        ),
        "eth_getStorageAt" => json!(address_word(
            polymarket_client_sdk_v2::wallet_contract_config(137)
                .unwrap()
                .proxy_factory
                .unwrap()
        )),
        "eth_sendRawTransaction" => {
            let raw = params[0].as_str().unwrap().to_owned();
            let hash = hex_data(keccak256(hex_bytes(&raw, MAX_RAW_BYTES).unwrap()).as_slice());
            s.sent.push(raw);
            json!(hash)
        }
        "eth_getTransactionReceipt" => s.receipt.clone().unwrap_or(Value::Null),
        "eth_getBlockByNumber" => {
            s.head_reads += 1;
            let number = if params[0] == "finalized" {
                if s.low_finality { "0x6e" } else { "0xc8" }
            } else {
                "0x64"
            };
            let hash = if (s.wrong_canonical && params[0] != "finalized")
                || (s.reorg_after_head && s.head_reads > 1)
            {
                format!("0x{}", "04".repeat(32))
            } else {
                format!("0x{}", "03".repeat(32))
            };
            json!({"number":number,"hash":hash})
        }
        "eth_call" => {
            let data = hex_bytes(params[0]["data"].as_str().unwrap(), MAX_RAW_BYTES).unwrap();
            let selector = &data[..4];
            let result = if selector == Exchange::getCollateralCall::SELECTOR
                || selector == Adapter::COLLATERAL_TOKENCall::SELECTOR
            {
                address_word(c.collateral)
            } else if selector == Exchange::getCtfCall::SELECTOR
                || selector == Adapter::CONDITIONAL_TOKENSCall::SELECTOR
            {
                address_word(if s.wrong_identity {
                    Address::ZERO
                } else {
                    c.conditional_tokens
                })
            } else if selector == Exchange::getOutcomeTokenFactoryCall::SELECTOR {
                address_word(EXCHANGE_OUTCOME_FACTORY)
            } else if selector == Exchange::getCtfCollateralCall::SELECTOR
                || selector == Adapter::USDCECall::SELECTOR
            {
                address_word(underlying)
            } else if selector == Ctf::getOutcomeSlotCountCall::SELECTOR {
                word(2)
            } else if selector == Ctf::getCollectionIdCall::SELECTOR {
                hex_data(&data[data.len() - 32..])
            } else if selector == Ctf::getPositionIdCall::SELECTOR {
                if U256::from_be_slice(&data[data.len() - 32..]) == U256::from(1) {
                    word(11)
                } else {
                    word(22)
                }
            } else if selector == Ctf::isApprovedForAllCall::SELECTOR {
                let approval = Ctf::isApprovedForAllCall::abi_decode(&data).unwrap();
                word(u64::from(
                    approval.operator == STANDARD_ADAPTER && s.approved,
                ))
            } else if selector == Ctf::payoutDenominatorCall::SELECTOR {
                word(if s.unresolved { 0 } else { 1 })
            } else if selector == Ctf::payoutNumeratorsCall::SELECTOR {
                word(u64::from(
                    U256::from_be_slice(&data[data.len() - 32..]) == U256::ZERO,
                ))
            } else if selector == Ctf::balanceOfCall::SELECTOR {
                word(if s.after { 5_000_000 } else { 10_000_000 })
            } else if selector == Token::balanceOfCall::SELECTOR {
                word(if s.after {
                    if s.other_market_debit {
                        90_000_000
                    } else {
                        105_000_000
                    }
                } else {
                    100_000_000
                })
            } else if selector == Safe::getThresholdCall::SELECTOR {
                word(1)
            } else if selector == Safe::getOwnersCall::SELECTOR {
                let owner = if s.wrong_owner {
                    Address::ZERO
                } else {
                    address(&live.signer_address).unwrap()
                };
                format!("0x{:064x}{:064x}{}", 32, 1, &address_word(owner)[2..])
            } else if selector == Safe::nonceCall::SELECTOR {
                word(5)
            } else if selector == Safe::getTransactionHashCall::SELECTOR {
                format!("0x{}", "77".repeat(32))
            } else if selector == ProxyFactory::getImplementationCall::SELECTOR {
                address_word(literal_address!(
                    "0x44e999d5c2F66Ef0861317f9A4805AC2e90aEB4f"
                ))
            } else {
                panic!("unexpected mock-only ABI {selector:?}")
            };
            json!(result)
        }
        _ => panic!("unexpected mock-only method {method}"),
    }
}
async fn client(wallet: u8) -> (DirectChain, MockRpc) {
    let (live, signer) = fixture_live(wallet);
    let rpc = MockRpc::start(live.clone()).await;
    let chain = DirectChain::new(
        live,
        ChainSettings::default(),
        rpc.url.clone(),
        signer,
        true,
    )
    .unwrap();
    (chain, rpc)
}
fn topic(a: Address) -> String {
    address_word(a)
}
fn logs(
    chain: &DirectChain,
    p: &PreparedChainTransaction,
    safe_result: Option<bool>,
) -> Vec<Value> {
    let c = polymarket_client_sdk_v2::contract_config(137, false).unwrap();
    let funder = address(&chain.live.funder_address).unwrap();
    let amount = match p.plan.operation {
        ChainOperation::Merge { amount_micros } => amount_micros,
        ChainOperation::Redeem => 10_000_000,
    };
    let mut logs = vec![
        json!({"address":format!("{:#x}",c.collateral),"topics":[hex_data(keccak256(b"Transfer(address,address,uint256)").as_slice()),topic(Address::ZERO),topic(funder)],"data":word(p.expected_payout_micros)}),
    ];
    for id in [11u64, 22] {
        logs.push(json!({"address":format!("{:#x}",c.conditional_tokens),"topics":[hex_data(keccak256(b"TransferSingle(address,address,address,uint256,uint256)").as_slice()),topic(STANDARD_ADAPTER),topic(funder),topic(STANDARD_ADAPTER)],"data":format!("0x{id:064x}{amount:064x}")}));
    }
    if let Some(success) = safe_result {
        let sig = if success {
            b"ExecutionSuccess(bytes32,uint256)".as_slice()
        } else {
            b"ExecutionFailure(bytes32,uint256)".as_slice()
        };
        logs.push(json!({"address":format!("{funder:#x}"),"topics":[hex_data(keccak256(sig).as_slice())],"data":format!("{}{:064x}",p.safe_transaction_hash.as_ref().unwrap(),0)}));
    }
    logs
}
fn receipt(p: &PreparedChainTransaction, logs: Vec<Value>, status: u64) -> Value {
    json!({
        "transactionHash":p.hash,"blockNumber":"0x64","blockHash":format!("0x{}","03".repeat(32)),
        "status":format!("0x{status:x}"),"gasUsed":"0x493e0","effectiveGasPrice":"0x6fc23ac00","logs":logs
    })
}

#[tokio::test]
async fn prepare_eoa_proxy_safe_signs_exact_intent_without_broadcast() {
    for wallet in [0, 1, 2] {
        let (chain, rpc) = client(wallet).await;
        let p = chain
            .prepare(
                &plan(ChainOperation::Merge {
                    amount_micros: 5_000_000,
                }),
                1000,
            )
            .await
            .unwrap();
        validate_prepared(&chain.live, &chain.settings, &p).unwrap();
        assert!(rpc.state.lock().unwrap().sent.is_empty());
        assert_eq!(p.nonce, 7);
        assert_eq!(p.before_tokens_micros, [10_000_000, 10_000_000]);
        assert_eq!(p.safe_nonce, if wallet == 2 { Some(5) } else { None });
        assert_eq!(p.expected_payout_micros, 5_000_000);
    }
}
#[tokio::test]
async fn canonicalizes_missing_token_ids_and_rejects_supplied_wrong_token() {
    let (chain, rpc) = client(0).await;
    let mut q = plan(ChainOperation::Merge {
        amount_micros: 5_000_000,
    });
    q.token_ids = ["".into(), "22".into()];
    let p = chain.prepare(&q, 1000).await.unwrap();
    assert_eq!(p.plan.token_ids, ["11", "22"]);
    q.token_ids = ["22".into(), "11".into()];
    assert_eq!(
        chain.prepare(&q, 1000).await.unwrap().plan.token_ids,
        ["11", "22"]
    );
    q.token_ids[0] = "33".into();
    assert!(
        chain
            .prepare(&q, 1000)
            .await
            .unwrap_err()
            .contains("identity mismatch")
    );
    assert!(rpc.state.lock().unwrap().sent.is_empty());
}
#[tokio::test]
async fn identity_approval_safe_owner_and_gas_are_fail_closed() {
    let (chain, rpc) = client(2).await;
    let q = plan(ChainOperation::Merge {
        amount_micros: 5_000_000,
    });
    rpc.state.lock().unwrap().wrong_identity = true;
    assert!(chain.prepare(&q, 1000).await.is_err());
    {
        let mut s = rpc.state.lock().unwrap();
        s.wrong_identity = false;
        s.approved = false;
    }
    assert!(
        chain
            .prepare(&q, 1000)
            .await
            .unwrap_err()
            .contains("approval missing")
    );
    {
        let mut s = rpc.state.lock().unwrap();
        s.approved = true;
        s.wrong_owner = true;
    }
    assert!(
        chain
            .prepare(&q, 1000)
            .await
            .unwrap_err()
            .contains("one-of-one")
    );
    rpc.state.lock().unwrap().wrong_owner = false;
    let mut small = chain;
    small.settings.max_transaction_cost_wei = "1".into();
    assert!(
        small
            .prepare(&q, 1000)
            .await
            .unwrap_err()
            .contains("finite cap")
    );
    assert!(rpc.state.lock().unwrap().sent.is_empty());
}
#[tokio::test]
async fn unknown_post_keeps_exact_hash_and_missing_receipt_never_reissues() {
    let (chain, rpc) = client(0).await;
    let p = chain
        .prepare(
            &plan(ChainOperation::Merge {
                amount_micros: 5_000_000,
            }),
            1000,
        )
        .await
        .unwrap();
    rpc.state.lock().unwrap().lost_ack = true;
    assert!(chain.submit(&p).await.is_err());
    assert!(chain.lookup(&p).await.unwrap().is_none());
    assert!(chain.lookup(&p).await.unwrap().is_none());
    let s = rpc.state.lock().unwrap();
    assert_eq!(s.sent, vec![p.raw_transaction.clone()]);
    assert_eq!(
        s.methods
            .iter()
            .filter(|m| m.as_str() == "eth_sendRawTransaction")
            .count(),
        1
    );
}
#[tokio::test]
async fn finalized_receipt_credits_exact_logs_despite_other_market_cash_debit() {
    let (chain, rpc) = client(0).await;
    let p = chain
        .prepare(
            &plan(ChainOperation::Merge {
                amount_micros: 5_000_000,
            }),
            1000,
        )
        .await
        .unwrap();
    {
        let mut s = rpc.state.lock().unwrap();
        s.after = true;
        s.other_market_debit = true;
        s.receipt = Some(receipt(&p, logs(&chain, &p, None), 1));
        s.low_finality = true;
    }
    assert!(chain.lookup(&p).await.unwrap().is_none());
    rpc.state.lock().unwrap().low_finality = false;
    let r = chain.lookup(&p).await.unwrap().unwrap();
    assert!(r.success);
    assert_eq!(r.collateral_delta_micros, 5_000_000);
    assert_eq!(r.after_collateral_micros, 90_000_000);
    assert_eq!(r.consumed_tokens_micros, [5_000_000, 5_000_000]);
    rpc.state.lock().unwrap().wrong_canonical = true;
    assert!(
        chain
            .lookup(&p)
            .await
            .unwrap_err()
            .contains("not canonical")
    );
}
#[tokio::test]
async fn safe_success_event_and_exact_transfer_logs_are_mandatory() {
    let (chain, rpc) = client(2).await;
    let p = chain
        .prepare(
            &plan(ChainOperation::Merge {
                amount_micros: 5_000_000,
            }),
            1000,
        )
        .await
        .unwrap();
    rpc.state.lock().unwrap().receipt = Some(receipt(&p, logs(&chain, &p, None), 1));
    assert!(
        chain
            .lookup(&p)
            .await
            .unwrap_err()
            .contains("event missing")
    );
    rpc.state.lock().unwrap().receipt = Some(receipt(&p, logs(&chain, &p, Some(true)), 1));
    assert!(chain.lookup(&p).await.unwrap().unwrap().success);
    let mut bad = logs(&chain, &p, Some(true));
    bad[0]["data"] = json!(word(4_000_000));
    rpc.state.lock().unwrap().receipt = Some(receipt(&p, bad, 1));
    assert!(
        chain
            .lookup(&p)
            .await
            .unwrap_err()
            .contains("economic intent")
    );
    rpc.state.lock().unwrap().receipt = Some(receipt(&p, vec![], 0));
    let r = chain.lookup(&p).await.unwrap().unwrap();
    assert!(!r.success);
    assert_eq!(r.consumed_tokens_micros, [0, 0]);
}
#[tokio::test]
async fn redemption_requires_authoritative_resolution_and_persists_payout_fraction() {
    let (chain, rpc) = client(0).await;
    let q = plan(ChainOperation::Redeem);
    rpc.state.lock().unwrap().unresolved = true;
    assert!(
        chain
            .prepare(&q, 1000)
            .await
            .unwrap_err()
            .contains("resolution")
    );
    rpc.state.lock().unwrap().unresolved = false;
    let p = chain.prepare(&q, 1000).await.unwrap();
    assert_eq!(p.payout_numerators, [1, 0]);
    assert_eq!(p.payout_denominator, 1);
    assert_eq!(p.expected_payout_micros, 10_000_000);
    let mut bad = p.clone();
    bad.payout_numerators = [1, 1];
    assert!(validate_prepared(&chain.live, &chain.settings, &bad).is_err());
}
#[tokio::test]
async fn durable_envelope_rejects_nonce_wallet_calldata_hash_and_gas_tamper() {
    let (chain, _rpc) = client(0).await;
    let p = chain
        .prepare(
            &plan(ChainOperation::Merge {
                amount_micros: 5_000_000,
            }),
            1000,
        )
        .await
        .unwrap();
    let mut bad = p.clone();
    bad.nonce += 1;
    assert!(validate_prepared(&chain.live, &chain.settings, &bad).is_err());
    let mut bad = p.clone();
    bad.plan.operation = ChainOperation::Merge {
        amount_micros: 4_000_000,
    };
    assert!(validate_prepared(&chain.live, &chain.settings, &bad).is_err());
    let mut bad = p.clone();
    bad.hash = format!("0x{}", "ff".repeat(32));
    assert!(validate_prepared(&chain.live, &chain.settings, &bad).is_err());
    let mut bad = p.clone();
    bad.maximum_gas_cost_wei = "1".into();
    assert!(validate_prepared(&chain.live, &chain.settings, &bad).is_err());
    let restored: PreparedChainTransaction =
        serde_json::from_str(&serde_json::to_string(&p).unwrap()).unwrap();
    validate_prepared(&chain.live, &chain.settings, &restored).unwrap();
    assert_eq!(restored.raw_transaction, p.raw_transaction);
}
#[tokio::test]
async fn finalized_redemption_accepts_verified_adapter_dust_but_rejects_underpayment() {
    let (chain, rpc) = client(0).await;
    let prepared = chain
        .prepare(&plan(ChainOperation::Redeem), 1000)
        .await
        .unwrap();
    let mut transfers = logs(&chain, &prepared, None);
    // The adapter sweeps existing USDCE dust alongside the resolved caller payout.
    transfers[0]["data"] = json!(word(prepared.expected_payout_micros + 1));
    rpc.state.lock().unwrap().receipt = Some(receipt(&prepared, transfers.clone(), 1));
    let observed = chain.lookup(&prepared).await.unwrap().unwrap();
    assert!(observed.success);
    assert_eq!(
        observed.collateral_delta_micros,
        prepared.expected_payout_micros + 1
    );
    assert_eq!(
        observed.consumed_tokens_micros,
        prepared.before_tokens_micros
    );
    transfers[0]["data"] = json!(word(prepared.expected_payout_micros - 1));
    rpc.state.lock().unwrap().receipt = Some(receipt(&prepared, transfers, 1));
    assert!(
        chain
            .lookup(&prepared)
            .await
            .unwrap_err()
            .contains("economic intent")
    );
}
#[tokio::test]
async fn chain_balances_are_bounded_ordered_and_use_no_data_api() {
    let (chain, rpc) = client(0).await;
    assert_eq!(
        chain
            .token_balances(&["22".into(), "11".into()])
            .await
            .unwrap(),
        vec![10_000_000, 10_000_000]
    );
    assert!(chain.token_balances(&vec!["11".into(); 129]).await.is_err());
    assert!(
        !rpc.state
            .lock()
            .unwrap()
            .methods
            .iter()
            .any(|m| m == "eth_sendRawTransaction")
    );
}
#[test]
fn settings_and_urls_reject_unbounded_or_insecure_inputs() {
    let s = ChainSettings {
        minimum_confirmations: 0,
        ..Default::default()
    };
    assert!(s.validate().is_err());
    let s = ChainSettings {
        max_session_gas_cost_wei: "1".into(),
        ..Default::default()
    };
    assert!(s.validate().is_err());
    assert!(validate_rpc_url("http://example.com", false).is_err());
    assert!(validate_rpc_url("https://user:password@example.com", false).is_err());
    assert!(validate_rpc_url("http://127.0.0.1:1", false).is_err());
    assert!(validate_rpc_url("https://rpc.example/owner-token", false).is_ok());
    assert!(token_id("0001").is_err());
    assert!(quantity_u64(&json!("0x01")).is_err());
}

#[tokio::test]
async fn readiness_checks_approval_and_wallet_before_any_position_or_broadcast() {
    for wallet in [0, 1, 2] {
        let (chain, rpc) = client(wallet).await;
        chain.ready().await.unwrap();
        rpc.state.lock().unwrap().approved = false;
        assert!(chain.ready().await.is_err());
        let state = rpc.state.lock().unwrap();
        assert!(state.sent.is_empty());
        assert!(
            !state
                .methods
                .iter()
                .any(|method| method == "eth_estimateGas")
        );
    }
    let (chain, rpc) = client(2).await;
    rpc.state.lock().unwrap().wrong_owner = true;
    assert!(chain.ready().await.is_err());
    assert!(rpc.state.lock().unwrap().sent.is_empty());
}

#[tokio::test]
async fn prepare_and_readiness_reject_a_head_reorg_without_signing_or_broadcasting() {
    let (chain, rpc) = client(0).await;
    rpc.state.lock().unwrap().reorg_after_head = true;
    assert!(
        chain
            .prepare(
                &plan(ChainOperation::Merge {
                    amount_micros: 5_000_000
                }),
                1000
            )
            .await
            .is_err()
    );
    assert!(rpc.state.lock().unwrap().sent.is_empty());
    let (chain, rpc) = client(0).await;
    rpc.state.lock().unwrap().reorg_after_head = true;
    assert!(chain.ready().await.is_err());
    assert!(rpc.state.lock().unwrap().sent.is_empty());
}

#[tokio::test]
async fn coordinator_fixture_is_signed_without_rpc_or_environment() {
    let (live, p) = fixture_prepared(
        plan(ChainOperation::Merge {
            amount_micros: 5_000_000,
        }),
        [10_000_000, 10_000_000],
    )
    .await;
    validate_prepared(&live, &ChainSettings::default(), &p).unwrap();
    assert_eq!(p.before_tokens_micros, [10_000_000, 10_000_000]);
}

#[tokio::test]
async fn batch_debits_and_safe_failure_logs_have_exact_transaction_semantics() {
    let (chain, rpc) = client(2).await;
    let p = chain
        .prepare(
            &plan(ChainOperation::Merge {
                amount_micros: 5_000_000,
            }),
            1000,
        )
        .await
        .unwrap();
    let ctf = polymarket_client_sdk_v2::contract_config(137, false)
        .unwrap()
        .conditional_tokens;
    let funder = address(&chain.live.funder_address).unwrap();
    let batch = json!({"address":format!("{ctf:#x}"),"topics":[
        hex_data(keccak256(b"TransferBatch(address,address,address,uint256[],uint256[])").as_slice()),
        topic(STANDARD_ADAPTER),topic(funder),topic(STANDARD_ADAPTER)],
        "data":format!("0x{:064x}{:064x}{:064x}{:064x}{:064x}{:064x}{:064x}{:064x}",64,160,2,11,22,2,5_000_000,5_000_000)});
    let standard = logs(&chain, &p, Some(true));
    let r = receipt(&p, vec![standard[0].clone(), batch, standard[3].clone()], 1);
    assert_eq!(
        receipt_deltas(&r, &chain.live, &p.plan).unwrap(),
        ([5_000_000, 5_000_000], 5_000_000)
    );
    rpc.state.lock().unwrap().receipt = Some(r);
    assert!(chain.lookup(&p).await.unwrap().unwrap().success);
    let failure = logs(&chain, &p, Some(false)).pop().unwrap();
    rpc.state.lock().unwrap().receipt = Some(receipt(&p, vec![failure], 1));
    let r = chain.lookup(&p).await.unwrap().unwrap();
    assert!(!r.success);
    assert_eq!(r.collateral_delta_micros, 0);
    assert_eq!(r.consumed_tokens_micros, [0, 0]);
    assert!(decode_batch(&[0u8; 128]).is_err());
}
