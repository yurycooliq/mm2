use super::*;
use std::sync::{Arc, Mutex};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

fn hash(n: u64) -> String {
    format!("0x{n:064x}")
}
fn addr(a: Address) -> String {
    hex_data(&U256::from_be_slice(a.as_slice()).to_be_bytes::<32>())
}
async fn fixture() -> (LiveSettings, PreparedChainTransaction, PrivateKeySigner) {
    let plan = ChainPlan {
        id: "relay-offline-1".into(),
        condition_id: format!("0x{}", "11".repeat(32)),
        token_ids: ["11".into(), "22".into()],
        operation: ChainOperation::Merge {
            amount_micros: 1_000_000,
        },
    };
    let (mut live, mut p) = super::super::fixture_prepared(plan, [2_000_000, 2_000_000]).await;
    let signer = PrivateKeySigner::from_str(
        "0000000000000000000000000000000000000000000000000000000000000001",
    )
    .unwrap();
    live.signature_type = 3;
    live.funder_address = format!("{:#x}", Address::repeat_byte(0x55));
    live.owner_address = live.funder_address.clone();
    live.chain = Some(ChainSettings {
        relayer: Some(RelaySettings::default()),
        ..Default::default()
    });
    p.funder_address = live.funder_address.clone();
    p.signer_address = live.signer_address.clone();
    p.hash = hash(0);
    p.raw_transaction.clear();
    p.nonce = 7;
    p.safe_nonce = None;
    p.safe_transaction_hash = None;
    p.gas_limit = 0;
    p.max_fee_per_gas_wei = 0;
    p.max_priority_fee_per_gas_wei = 0;
    p.maximum_gas_cost_wei = "0".into();
    p.prepared_at_ms = 1_000_000;
    p.before_tokens_micros = [2_000_000, 2_000_000];
    p.before_collateral_micros = 4_000_000;
    p.expected_payout_micros = 1_000_000;
    (live, p, signer)
}

struct State {
    nonce: u64,
    api_nonce: u64,
    owner_wrong: bool,
    pending_owner: bool,
    paused: bool,
    wrong_factory: bool,
    wrong_prediction: bool,
    wrong_effective_impl: bool,
    default_impl_only: bool,
    wrong_beacon: bool,
    wrong_factory_impl: bool,
    missing_code: bool,
    wrong_interface: bool,
    wrong_domain: bool,
    wrong_canonical: bool,
    finalized_height: u64,
    finalized_timestamp: u64,
    tokens: [u64; 2],
    cash: u64,
    post_bodies: Vec<String>,
    paths: Vec<String>,
    rpc_methods: Vec<String>,
    beacon_from: Vec<String>,
    ranges: Vec<(u64, u64)>,
    lose_ack: bool,
    reject_submit: bool,
    malformed_ack: bool,
    hint: Option<String>,
    transaction: Option<Value>,
    receipt: Option<Value>,
    logs: Vec<Value>,
}
impl Default for State {
    fn default() -> Self {
        Self {
            nonce: 7,
            api_nonce: 7,
            owner_wrong: false,
            pending_owner: false,
            paused: false,
            wrong_factory: false,
            wrong_prediction: false,
            wrong_effective_impl: false,
            default_impl_only: false,
            wrong_beacon: false,
            wrong_factory_impl: false,
            missing_code: false,
            wrong_interface: false,
            wrong_domain: false,
            wrong_canonical: false,
            finalized_height: 200,
            finalized_timestamp: 2000,
            tokens: [2_000_000, 2_000_000],
            cash: 4_000_000,
            post_bodies: Vec::new(),
            paths: Vec::new(),
            rpc_methods: Vec::new(),
            beacon_from: Vec::new(),
            ranges: Vec::new(),
            lose_ack: false,
            reject_submit: false,
            malformed_ack: false,
            hint: None,
            transaction: None,
            receipt: None,
            logs: Vec::new(),
        }
    }
}
struct Mock {
    url: String,
    state: Arc<Mutex<State>>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Mock {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Mock {
    async fn start(live: LiveSettings) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let state = Arc::new(Mutex::new(State::default()));
        let shared = state.clone();
        let task = tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let shared = shared.clone();
                let live = live.clone();
                tokio::spawn(async move {
                    let mut raw = Vec::new();
                    let mut buf = [0; 4096];
                    let (head_end, length);
                    loop {
                        let n = stream.read(&mut buf).await.unwrap();
                        if n == 0 {
                            return;
                        }
                        raw.extend_from_slice(&buf[..n]);
                        if let Some(i) = raw.windows(4).position(|b| b == b"\r\n\r\n") {
                            let head = String::from_utf8_lossy(&raw[..i]);
                            length = head
                                .lines()
                                .find_map(|line| {
                                    let (k, v) = line.split_once(':')?;
                                    k.eq_ignore_ascii_case("content-length")
                                        .then(|| v.trim().parse::<usize>().unwrap())
                                })
                                .unwrap_or(0);
                            head_end = i + 4;
                            break;
                        }
                        assert!(raw.len() < 65536);
                    }
                    while raw.len() < head_end + length {
                        let n = stream.read(&mut buf).await.unwrap();
                        if n == 0 {
                            return;
                        }
                        raw.extend_from_slice(&buf[..n]);
                    }
                    let head = String::from_utf8_lossy(&raw[..head_end]);
                    let mut first = head.lines().next().unwrap().split_whitespace();
                    let method = first.next().unwrap();
                    let path = first.next().unwrap();
                    let body =
                        String::from_utf8(raw[head_end..head_end + length].to_vec()).unwrap();
                    let answer = {
                        let mut s = shared.lock().unwrap();
                        s.paths.push(path.into());
                        if path.starts_with("/v1/account/transactions/params") {
                            assert!(
                                head.to_ascii_lowercase()
                                    .contains("relayer_api_key: offline-fixture-key")
                            );
                            Some((
                                200,
                                json!({"address":live.signer_address,"nonce":s.api_nonce.to_string()}),
                            ))
                        } else if path.starts_with("/v1/account/transactions/") {
                            let id = path.rsplit('/').next().unwrap();
                            Some((
                                200,
                                json!({"transaction_id":id,"transaction_hash":s.hint,"state":"STATE_CONFIRMED","error_msg":null}),
                            ))
                        } else if method == "POST" && path == "/submit" {
                            s.post_bodies.push(body);
                            if s.lose_ack {
                                None
                            } else if s.reject_submit {
                                Some((
                                    429,
                                    json!({"error":"offline-fixture-key MUST NOT APPEAR IN ERROR"}),
                                ))
                            } else if s.malformed_ack {
                                Some((200, json!({"state":"STATE_NEW"})))
                            } else {
                                Some((
                                    200,
                                    json!({"transactionID":"fixture-id","state":"STATE_NEW"}),
                                ))
                            }
                        } else {
                            let request: Value = serde_json::from_str(&body).unwrap();
                            let rpc = request["method"].as_str().unwrap();
                            let params = &request["params"];
                            s.rpc_methods.push(rpc.into());
                            let result = rpc_result(rpc, params, &live, &mut s);
                            Some((200, json!({"jsonrpc":"2.0","id":1,"result":result})))
                        }
                    };
                    if let Some((status, data)) = answer {
                        let body = serde_json::to_vec(&data).unwrap();
                        let header = format!(
                            "HTTP/1.1 {status} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                            if status == 200 { "OK" } else { "Rejected" },
                            body.len()
                        );
                        stream.write_all(header.as_bytes()).await.unwrap();
                        stream.write_all(&body).await.unwrap();
                    }
                });
            }
        });
        Self { url, state, task }
    }
    fn transport(&self, owner: Address) -> RelayTransport {
        RelayTransport::test_transport(
            RelaySettings {
                url: self.url.clone(),
                ..Default::default()
            },
            owner,
        )
        .unwrap()
    }
    fn chain(&self, live: LiveSettings, signer: PrivateKeySigner) -> DirectChain {
        DirectChain::new(
            live,
            ChainSettings::default(),
            self.url.clone(),
            signer,
            true,
        )
        .unwrap()
    }
}
fn rpc_result(method: &str, p: &Value, live: &LiveSettings, s: &mut State) -> Value {
    match method {
        "eth_getBlockByNumber" => {
            let tag = p[0].as_str().unwrap();
            let n = if tag == "finalized" {
                s.finalized_height
            } else if tag == "latest" {
                100
            } else {
                u64::from_str_radix(tag.trim_start_matches("0x"), 16).unwrap()
            };
            json!({"number":format!("0x{n:x}"),"hash":if s.wrong_canonical&&n==110{hash(999)}else{hash(n)},"timestamp":format!("0x{:x}",if tag=="finalized"{s.finalized_timestamp}else{1000})})
        }
        "eth_getStorageAt" => {
            if p[0]
                .as_str()
                .unwrap()
                .eq_ignore_ascii_case(&format!("{DEPOSIT_FACTORY:#x}"))
            {
                json!(addr(if s.wrong_factory_impl {
                    Address::repeat_byte(0x66)
                } else {
                    FACTORY_IMPLEMENTATION
                }))
            } else {
                json!(addr(if s.wrong_beacon {
                    Address::repeat_byte(0x66)
                } else {
                    DEPOSIT_BEACON
                }))
            }
        }
        "eth_getCode" => json!(if s.missing_code { "0x" } else { "0x6001" }),
        "eth_call" => {
            let to = address(p[0]["to"].as_str().unwrap()).unwrap();
            let data = hex_bytes(p[0]["data"].as_str().unwrap(), MAX_RPC_BYTES).unwrap();
            let sel = &data[..4];
            if to == DEPOSIT_BEACON {
                let from = p[0].get("from").and_then(Value::as_str).unwrap_or("");
                s.beacon_from.push(from.into());
                return json!(addr(
                    if s.wrong_effective_impl
                        || (s.default_impl_only && from.eq_ignore_ascii_case(&live.funder_address))
                    {
                        Address::repeat_byte(0x66)
                    } else {
                        DEPOSIT_IMPLEMENTATION
                    }
                ));
            }
            if sel == DepositWallet::ownerCall::SELECTOR {
                json!(addr(if s.owner_wrong {
                    Address::repeat_byte(0x66)
                } else {
                    address(&live.signer_address).unwrap()
                }))
            } else if sel == DepositWallet::pendingOwnerCall::SELECTOR {
                json!(addr(if s.pending_owner {
                    Address::repeat_byte(0x66)
                } else {
                    Address::ZERO
                }))
            } else if sel == DepositWallet::factoryCall::SELECTOR {
                json!(addr(if s.wrong_factory {
                    Address::repeat_byte(0x66)
                } else {
                    DEPOSIT_FACTORY
                }))
            } else if sel == DepositWallet::idCall::SELECTOR {
                json!(addr(address(&live.signer_address).unwrap()))
            } else if sel == DepositWallet::pausedCall::SELECTOR {
                json!(hash(u64::from(s.paused)))
            } else if sel == DepositWallet::nonceCall::SELECTOR {
                json!(hash(s.nonce))
            } else if sel == DepositWallet::walletInterfaceIdCall::SELECTOR {
                json!(if s.wrong_interface {
                    hash(0)
                } else {
                    format!("{:#x}", keccak256("Polymarket.DepositWallet"))
                })
            } else if sel == DepositWallet::eip712DomainCall::SELECTOR {
                json!(hex_data(
                    &(
                        alloy::primitives::FixedBytes::<1>::from([0x0f]),
                        if s.wrong_domain {
                            "EvilWallet"
                        } else {
                            "DepositWallet"
                        }
                        .to_string(),
                        "1".to_string(),
                        U256::from(137),
                        address(&live.funder_address).unwrap(),
                        B256::ZERO,
                        Vec::<U256>::new()
                    )
                        .abi_encode_params()
                ))
            } else if sel == DepositFactory::predictWalletAddressCall::SELECTOR {
                json!(addr(if s.wrong_prediction {
                    Address::repeat_byte(0x66)
                } else {
                    address(&live.funder_address).unwrap()
                }))
            } else if sel == Ctf::balanceOfCall::SELECTOR {
                let id = U256::from_be_slice(&data[data.len() - 32..]);
                json!(hash(s.tokens[usize::from(id == U256::from(22))]))
            } else if sel == Token::balanceOfCall::SELECTOR {
                json!(hash(s.cash))
            } else {
                panic!("unexpected offline ABI call {}", hex_data(sel));
            }
        }
        "eth_getLogs" => {
            let start = u64::from_str_radix(
                p[0]["fromBlock"].as_str().unwrap().trim_start_matches("0x"),
                16,
            )
            .unwrap();
            let end = u64::from_str_radix(
                p[0]["toBlock"].as_str().unwrap().trim_start_matches("0x"),
                16,
            )
            .unwrap();
            s.ranges.push((start, end));
            Value::Array(
                s.logs
                    .iter()
                    .filter(|l| {
                        let n = quantity_u64(&l["blockNumber"]).unwrap();
                        n >= start && n <= end
                    })
                    .cloned()
                    .collect(),
            )
        }
        "eth_getTransactionReceipt" => s.receipt.clone().unwrap_or(Value::Null),
        "eth_getTransactionByHash" => s.transaction.clone().unwrap_or(Value::Null),
        _ => panic!("unexpected offline RPC {method}"),
    }
}
async fn prepared(
    mock: &Mock,
    live: &LiveSettings,
    p: &mut PreparedChainTransaction,
    signer: PrivateKeySigner,
) -> (DirectChain, RelayTransport, PreparedRelay) {
    let chain = mock.chain(live.clone(), signer.clone());
    let transport = mock.transport(signer.address());
    let relay = chain
        .prepare_relay(&transport, p, 100, &hash(100), 1000)
        .await
        .unwrap();
    p.hash = relay.batch_hash.clone();
    (chain, transport, relay)
}
fn install_success(
    mock: &Mock,
    live: &LiveSettings,
    p: &PreparedChainTransaction,
    r: &PreparedRelay,
) {
    let tx_hash = hash(900);
    let block = 110;
    let sponsor = Address::repeat_byte(0x77);
    let batch = make_batch(live, p, r.deadline).unwrap();
    let raw = DepositFactory::proxyCall {
        batches: vec![batch],
        signatures: vec![hex_bytes(&r.signature, 65).unwrap().into()],
    }
    .abi_encode();
    let log = |topics: Vec<String>, data: String| json!({"address":live.funder_address,"topics":topics,"data":data,"blockNumber":format!("0x{block:x}"),"blockHash":hash(block),"transactionHash":tx_hash,"removed":false});
    let execution = log(
        vec![
            format!("{:#x}", DepositWallet::Execution::SIGNATURE_HASH),
            addr(STANDARD_ADAPTER),
        ],
        hex_data(
            &(
                U256::ZERO,
                Bytes::from(
                    inner_call(&p.plan, address(&live.collateral_address).unwrap()).unwrap(),
                ),
                Bytes::new(),
            )
                .abi_encode_params(),
        ),
    );
    let completed = log(
        vec![
            format!("{:#x}", DepositWallet::BatchExecuted::SIGNATURE_HASH),
            hash(p.nonce),
        ],
        "0x".into(),
    );
    let mut s = mock.state.lock().unwrap();
    s.logs = vec![completed.clone()];
    s.transaction = Some(
        json!({"hash":tx_hash,"from":format!("{sponsor:#x}"),"to":format!("{DEPOSIT_FACTORY:#x}"),"value":"0x0","input":hex_data(&raw),"blockNumber":format!("0x{block:x}"),"blockHash":hash(block)}),
    );
    s.receipt = Some(
        json!({"transactionHash":tx_hash,"from":format!("{sponsor:#x}"),"to":format!("{DEPOSIT_FACTORY:#x}"),"status":"0x1","blockNumber":format!("0x{block:x}"),"blockHash":hash(block),"logs":[execution,completed]}),
    );
}

#[test]
fn production_relayer_url_and_bounds_are_pinned() {
    RelaySettings::default().validate().unwrap();
    for url in [
        "https://relayer-v2.polymarket.com.evil.test",
        "http://relayer-v2.polymarket.com",
        "https://relayer-v2.polymarket.com/redirect",
        "https://user@relayer-v2.polymarket.com",
    ] {
        assert!(
            RelaySettings {
                url: url.into(),
                ..Default::default()
            }
            .validate()
            .is_err()
        );
    }
    for seconds in [0, 59, 3601, u64::MAX] {
        assert!(
            RelaySettings {
                deadline_seconds: seconds,
                ..Default::default()
            }
            .validate()
            .is_err()
        );
    }
    assert!(
        RelaySettings {
            key_env: "PRIVATE\nKEY".into(),
            ..Default::default()
        }
        .validate()
        .is_err()
    );
    assert!(serde_json::from_value::<RelaySettings>(json!({"unknown":true})).is_err());
}
#[test]
fn factory_abi_matches_observed_public_transaction_selector() {
    assert_eq!(
        DepositFactory::proxyCall::SELECTOR,
        [0x0a, 0x3c, 0x44, 0x05]
    );
}
#[test]
fn actual_public_deposit_wallet_domain_return_decodes_without_tuple_wrapper() {
    // Public read-only RPC result at Polygon block 95002118; contains no signed payload/key.
    let raw = hex_bytes("0x0f0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e00000000000000000000000000000000000000000000000000000000000000120000000000000000000000000000000000000000000000000000000000000008900000000000000000000000004ad8cb08f04b58fbcea273603d6cd151ba8adb400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000160000000000000000000000000000000000000000000000000000000000000000d4465706f73697457616c6c657400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000131000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",MAX_RPC_BYTES).unwrap();
    let domain = DepositWallet::eip712DomainCall::abi_decode_returns(&raw).unwrap();
    assert_eq!(domain.fields.as_slice(), [0x0f]);
    assert_eq!(domain.name, "DepositWallet");
    assert_eq!(domain.version, "1");
    assert_eq!(domain.chainId, U256::from(137));
    assert_eq!(
        domain.verifyingContract,
        address("0x04Ad8Cb08F04b58fbcEa273603d6cd151Ba8aDB4").unwrap()
    );
    assert_eq!(domain.salt, B256::ZERO);
    assert!(domain.extensions.is_empty());
}
#[tokio::test]
async fn preparation_signs_exact_owner_batch_without_post_or_chain_mutation() {
    let (live, mut p, signer) = fixture().await;
    let mock = Mock::start(live.clone()).await;
    let (_, _, r) = prepared(&mock, &live, &mut p, signer).await;
    verify_prepared_relay(&live, &p, &r).unwrap();
    let body: Value = serde_json::from_str(&r.body).unwrap();
    assert_eq!(body["type"], "WALLET");
    assert_eq!(body["nonce"], "7");
    assert_eq!(
        body["depositWalletParams"]["calls"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(body["depositWalletParams"]["calls"][0]["value"], "0");
    assert!(!r.dispatched);
    assert_eq!(r.deadline, 1600);
    let s = mock.state.lock().unwrap();
    assert!(s.post_bodies.is_empty());
    assert!(!s.rpc_methods.iter().any(|m| m == "eth_sendRawTransaction"));
    assert_eq!(s.beacon_from, vec![live.funder_address]);
}
#[tokio::test]
async fn owner_nonce_pending_pause_factory_derivation_and_beacon_checks_fail_before_signing() {
    for flag in 0..11 {
        let (live, p, signer) = fixture().await;
        let mock = Mock::start(live.clone()).await;
        {
            let mut s = mock.state.lock().unwrap();
            match flag {
                0 => s.owner_wrong = true,
                1 => s.pending_owner = true,
                2 => s.paused = true,
                3 => s.wrong_factory = true,
                4 => s.wrong_prediction = true,
                5 => s.wrong_beacon = true,
                6 => s.wrong_factory_impl = true,
                7 => s.default_impl_only = true,
                8 => s.missing_code = true,
                9 => s.wrong_interface = true,
                _ => s.wrong_domain = true,
            };
        }
        let chain = mock.chain(live, signer.clone());
        let transport = mock.transport(signer.address());
        assert!(
            chain
                .prepare_relay(&transport, &p, 100, &hash(100), 1000)
                .await
                .is_err()
        );
        assert!(mock.state.lock().unwrap().post_bodies.is_empty());
    }
}
#[tokio::test]
async fn authenticated_nonce_mismatch_and_key_owner_never_trigger_refresh_or_post() {
    let (live, p, signer) = fixture().await;
    let mock = Mock::start(live.clone()).await;
    mock.state.lock().unwrap().api_nonce = 8;
    let chain = mock.chain(live, signer.clone());
    let transport = mock.transport(signer.address());
    assert!(
        chain
            .prepare_relay(&transport, &p, 100, &hash(100), 1000)
            .await
            .is_err()
    );
    assert!(
        transport
            .authenticated_nonce(Address::repeat_byte(0x66))
            .await
            .is_err()
    );
    assert!(mock.state.lock().unwrap().post_bodies.is_empty());
}
#[tokio::test]
async fn durable_body_signature_and_economics_tampering_are_rejected_locally() {
    let (live, mut p, signer) = fixture().await;
    let mock = Mock::start(live.clone()).await;
    let (_, _, r) = prepared(&mock, &live, &mut p, signer).await;
    let mut changed = r.clone();
    changed.body = changed.body.replace("\"value\":\"0\"", "\"value\":\"1\"");
    assert!(verify_prepared_relay(&live, &p, &changed).is_err());
    let mut changed = r.clone();
    changed.signature = hex_data(&[0; 65]);
    assert!(verify_prepared_relay(&live, &p, &changed).is_err());
    let mut changed = r.clone();
    changed.deadline += 1;
    assert!(verify_prepared_relay(&live, &p, &changed).is_err());
    let mut changed = p.clone();
    changed.expected_payout_micros += 1;
    assert!(verify_prepared_relay(&live, &changed, &r).is_err());
    let mut changed = p.clone();
    changed.raw_transaction = "0x00".into();
    assert!(verify_prepared_relay(&live, &changed, &r).is_err());
    let mut changed = r.clone();
    changed.request_id = Some("../../secret".into());
    assert!(verify_prepared_relay(&live, &p, &changed).is_err());
}
#[tokio::test]
async fn submit_requires_durable_dispatch_sends_exact_body_once_and_returns_id() {
    let (live, mut p, signer) = fixture().await;
    let mock = Mock::start(live.clone()).await;
    let (_, transport, mut r) = prepared(&mock, &live, &mut p, signer).await;
    assert!(transport.submit_relay(&r).await.is_err());
    assert!(mock.state.lock().unwrap().post_bodies.is_empty());
    r.dispatched = true;
    assert_eq!(transport.submit_relay(&r).await.unwrap(), "fixture-id");
    assert_eq!(mock.state.lock().unwrap().post_bodies, vec![r.body.clone()]);
    r.request_id = Some("fixture-id".into());
    assert!(transport.submit_relay(&r).await.is_err());
    assert_eq!(mock.state.lock().unwrap().post_bodies.len(), 1);
}
#[tokio::test]
async fn dropped_ack_quota_and_malformed_ack_never_retry_or_expose_credentials() {
    for flag in 0..3 {
        let (live, mut p, signer) = fixture().await;
        let mock = Mock::start(live.clone()).await;
        let (_, transport, mut r) = prepared(&mock, &live, &mut p, signer).await;
        r.dispatched = true;
        {
            let mut s = mock.state.lock().unwrap();
            match flag {
                0 => s.lose_ack = true,
                1 => s.reject_submit = true,
                _ => s.malformed_ack = true,
            };
        }
        let error = transport.submit_relay(&r).await.unwrap_err();
        assert!(!error.contains("offline-fixture-key"));
        assert_eq!(mock.state.lock().unwrap().post_bodies, vec![r.body]);
    }
}
#[tokio::test]
async fn lost_ack_is_recovered_only_from_complete_finalized_factory_batch_and_wallet_events() {
    let (live, mut p, signer) = fixture().await;
    let mock = Mock::start(live.clone()).await;
    let (chain, transport, mut r) = prepared(&mock, &live, &mut p, signer).await;
    r.dispatched = true;
    install_success(&mock, &live, &p, &r);
    let candidate = chain
        .lookup_relay(&transport, &p, &r)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(candidate.transaction_hash, hash(900));
    assert_eq!(candidate.receipt["status"], "0x1");
    assert_ne!(candidate.transaction_hash, r.batch_hash);
    assert!(mock.state.lock().unwrap().post_bodies.is_empty());
}
#[tokio::test]
async fn relayer_hint_is_bound_to_full_chain_evidence() {
    let (live, mut p, signer) = fixture().await;
    let mock = Mock::start(live.clone()).await;
    let (chain, transport, mut r) = prepared(&mock, &live, &mut p, signer).await;
    r.dispatched = true;
    r.request_id = Some("fixture-id".into());
    install_success(&mock, &live, &p, &r);
    mock.state.lock().unwrap().hint = Some(hash(900));
    assert!(
        chain
            .lookup_relay(&transport, &p, &r)
            .await
            .unwrap()
            .is_some()
    );
    assert!(mock.state.lock().unwrap().ranges.is_empty());
}
#[tokio::test]
async fn unknown_hash_and_no_logs_remain_pending_with_bounded_progress_and_no_post() {
    let (live, mut p, signer) = fixture().await;
    let mock = Mock::start(live.clone()).await;
    let (chain, transport, mut r) = prepared(&mock, &live, &mut p, signer).await;
    r.dispatched = true;
    mock.state.lock().unwrap().finalized_height = 1000;
    assert!(
        chain
            .lookup_relay(&transport, &p, &r)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(mock.state.lock().unwrap().ranges.len(), 8);
    assert_eq!(mock.state.lock().unwrap().ranges[0], (100, 149));
    assert!(
        chain
            .lookup_relay(&transport, &p, &r)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(mock.state.lock().unwrap().ranges[8], (500, 549));
    assert!(mock.state.lock().unwrap().post_bodies.is_empty());
}
#[tokio::test]
async fn low_finality_retains_scan_candidate_for_later_confirmation() {
    let (live, mut p, signer) = fixture().await;
    let mock = Mock::start(live.clone()).await;
    let (chain, transport, mut r) = prepared(&mock, &live, &mut p, signer).await;
    r.dispatched = true;
    install_success(&mock, &live, &p, &r);
    mock.state.lock().unwrap().finalized_height = 110;
    assert!(
        chain
            .lookup_relay(&transport, &p, &r)
            .await
            .unwrap()
            .is_none()
    );
    mock.state.lock().unwrap().finalized_height = 200;
    assert!(
        chain
            .lookup_relay(&transport, &p, &r)
            .await
            .unwrap()
            .is_some()
    );
}
#[tokio::test]
async fn wrong_factory_call_signature_extra_wallet_batch_and_canonical_receipt_fail_closed() {
    for flag in 0..6 {
        let (live, mut p, signer) = fixture().await;
        let mock = Mock::start(live.clone()).await;
        let (chain, transport, mut r) = prepared(&mock, &live, &mut p, signer).await;
        r.dispatched = true;
        install_success(&mock, &live, &p, &r);
        {
            let mut s = mock.state.lock().unwrap();
            match flag {
                0 => {
                    s.transaction.as_mut().unwrap()["to"] =
                        json!(Address::repeat_byte(0x66).to_string())
                }
                1 => {
                    let mut b = make_batch(&live, &p, r.deadline).unwrap();
                    b.calls[0].value = U256::from(1);
                    s.transaction.as_mut().unwrap()["input"] = json!(hex_data(
                        &DepositFactory::proxyCall {
                            batches: vec![b],
                            signatures: vec![hex_bytes(&r.signature, 65).unwrap().into()]
                        }
                        .abi_encode()
                    ));
                }
                2 => {
                    s.transaction.as_mut().unwrap()["input"] = json!(hex_data(
                        &DepositFactory::proxyCall {
                            batches: vec![make_batch(&live, &p, r.deadline).unwrap()],
                            signatures: vec![Bytes::from(vec![0; 65])]
                        }
                        .abi_encode()
                    ));
                }
                3 => {
                    let a = make_batch(&live, &p, r.deadline).unwrap();
                    let b = make_batch(&live, &p, r.deadline).unwrap();
                    let sig = Bytes::from(hex_bytes(&r.signature, 65).unwrap());
                    s.transaction.as_mut().unwrap()["input"] = json!(hex_data(
                        &DepositFactory::proxyCall {
                            batches: vec![a, b],
                            signatures: vec![sig.clone(), sig]
                        }
                        .abi_encode()
                    ));
                }
                4 => s.wrong_canonical = true,
                _ => {
                    s.receipt.as_mut().unwrap()["logs"][0]["data"] = json!(hex_data(
                        &(U256::from(1), Bytes::new(), Bytes::new()).abi_encode_params()
                    ))
                }
            }
        }
        assert!(
            chain.lookup_relay(&transport, &p, &r).await.is_err(),
            "case {flag}"
        );
        assert!(mock.state.lock().unwrap().post_bodies.is_empty());
    }
}
#[tokio::test]
async fn missing_completion_removed_event_duplicate_execution_and_wrong_nonce_are_not_success() {
    for flag in 0..4 {
        let (live, mut p, signer) = fixture().await;
        let mock = Mock::start(live.clone()).await;
        let (chain, transport, mut r) = prepared(&mock, &live, &mut p, signer).await;
        r.dispatched = true;
        install_success(&mock, &live, &p, &r);
        {
            let mut s = mock.state.lock().unwrap();
            match flag {
                0 => {
                    s.receipt.as_mut().unwrap()["logs"]
                        .as_array_mut()
                        .unwrap()
                        .pop();
                }
                1 => s.receipt.as_mut().unwrap()["logs"][1]["removed"] = json!(true),
                2 => {
                    let log = s.receipt.as_ref().unwrap()["logs"][0].clone();
                    s.receipt.as_mut().unwrap()["logs"]
                        .as_array_mut()
                        .unwrap()
                        .insert(1, log);
                }
                _ => s.logs[0]["topics"][1] = json!(hash(8)),
            }
        }
        assert!(
            chain.lookup_relay(&transport, &p, &r).await.is_err(),
            "case {flag}"
        );
    }
}
#[tokio::test]
async fn failed_outer_receipt_does_not_release_or_fabricate_batch_success() {
    let (live, mut p, signer) = fixture().await;
    let mock = Mock::start(live.clone()).await;
    let (chain, transport, mut r) = prepared(&mock, &live, &mut p, signer).await;
    r.dispatched = true;
    r.request_id = Some("fixture-id".into());
    install_success(&mock, &live, &p, &r);
    {
        let mut s = mock.state.lock().unwrap();
        s.hint = Some(hash(900));
        s.logs.clear();
        s.receipt.as_mut().unwrap()["status"] = json!("0x0");
    }
    assert!(
        chain
            .lookup_relay(&transport, &p, &r)
            .await
            .unwrap()
            .is_none()
    );
    assert!(mock.state.lock().unwrap().post_bodies.is_empty());
}
#[tokio::test]
async fn expiry_requires_strict_final_deadline_exact_nonce_and_unchanged_condition_tokens() {
    let (live, mut p, signer) = fixture().await;
    let mock = Mock::start(live.clone()).await;
    let (chain, _, mut r) = prepared(&mock, &live, &mut p, signer).await;
    r.dispatched = true;
    mock.state.lock().unwrap().finalized_timestamp = 1600;
    assert!(chain.expired_relay(&p, &r).await.unwrap().is_none());
    mock.state.lock().unwrap().finalized_timestamp = 1601;
    let expiry = chain.expired_relay(&p, &r).await.unwrap().unwrap();
    assert_eq!(expiry.intent_hash, r.batch_hash);
    assert_eq!(expiry.retained_tokens_micros, p.before_tokens_micros);
    assert_eq!(expiry.wallet_nonce, p.nonce);
    mock.state.lock().unwrap().nonce = 8;
    assert!(chain.expired_relay(&p, &r).await.unwrap().is_none());
    {
        let mut s = mock.state.lock().unwrap();
        s.nonce = 7;
        s.tokens[0] -= 1;
    }
    assert!(chain.expired_relay(&p, &r).await.unwrap().is_none());
    assert!(mock.state.lock().unwrap().post_bodies.is_empty());
}
#[tokio::test]
async fn unrelated_cash_change_does_not_erase_valid_expiry_and_undispatched_intent_never_expires() {
    let (live, mut p, signer) = fixture().await;
    let mock = Mock::start(live.clone()).await;
    let (chain, _, mut r) = prepared(&mock, &live, &mut p, signer).await;
    assert!(chain.expired_relay(&p, &r).await.unwrap().is_none());
    r.dispatched = true;
    mock.state.lock().unwrap().cash = 123;
    assert_eq!(
        chain
            .expired_relay(&p, &r)
            .await
            .unwrap()
            .unwrap()
            .collateral_micros,
        123
    );
}
