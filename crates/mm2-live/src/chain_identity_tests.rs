use super::*;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

const BLOCK: &str = "0x5a99e06";

#[derive(Default)]
struct State {
    overrides: HashMap<(Address, [u8; 4]), String>,
    approvals: HashMap<Address, String>,
    error_operator: Option<Address>,
    seen: Vec<Value>,
}

struct Rpc {
    state: Arc<Mutex<State>>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Rpc {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn word(n: u64) -> String {
    format!("0x{n:064x}")
}
fn address_word(value: Address) -> String {
    hex_data(&U256::from_be_slice(value.as_slice()).to_be_bytes::<32>())
}

fn reply(state: &mut State, request: Value) -> Value {
    assert_eq!(request["method"], "eth_call");
    assert_eq!(request["params"][1], BLOCK);
    let target = address(request["params"][0]["to"].as_str().unwrap()).unwrap();
    let data = hex_bytes(request["params"][0]["data"].as_str().unwrap(), 4096).unwrap();
    let selector: [u8; 4] = data[..4].try_into().unwrap();
    state.seen.push(request);
    if selector == Ctf::isApprovedForAllCall::SELECTOR {
        let approval = Ctf::isApprovedForAllCall::abi_decode(&data).unwrap();
        if state.error_operator == Some(approval.operator) {
            return json!({"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"unavailable"}});
        }
        let approved = state
            .approvals
            .get(&approval.operator)
            .cloned()
            .unwrap_or_else(|| word(u64::from(approval.operator == STANDARD_ADAPTER)));
        return json!({"jsonrpc":"2.0","id":1,"result":approved});
    }
    let contracts = polymarket_client_sdk_v2::contract_config(137, false).unwrap();
    let result = state
        .overrides
        .get(&(target, selector))
        .cloned()
        .unwrap_or_else(|| {
            if selector == Exchange::getCollateralCall::SELECTOR
                || selector == Adapter::COLLATERAL_TOKENCall::SELECTOR
            {
                address_word(contracts.collateral)
            } else if selector == Exchange::getCtfCall::SELECTOR
                || selector == Adapter::CONDITIONAL_TOKENSCall::SELECTOR
            {
                address_word(contracts.conditional_tokens)
            } else if selector == Exchange::getOutcomeTokenFactoryCall::SELECTOR {
                address_word(EXCHANGE_OUTCOME_FACTORY)
            } else if selector == Exchange::getCtfCollateralCall::SELECTOR
                || selector == Adapter::USDCECall::SELECTOR
            {
                address_word(CTF_UNDERLYING_USDCE)
            } else {
                panic!("unexpected identity-only ABI selector {selector:?}");
            }
        });
    json!({"jsonrpc":"2.0","id":1,"result":result})
}

async fn client() -> (DirectChain, Rpc) {
    let signer = PrivateKeySigner::from_str(
        "0000000000000000000000000000000000000000000000000000000000000001",
    )
    .unwrap();
    let owner = format!("{:#x}", signer.address());
    let contracts = polymarket_client_sdk_v2::contract_config(137, false).unwrap();
    let live = LiveSettings {
        owner_authorized: true,
        owner_address: owner.clone(),
        funder_address: owner.clone(),
        signer_address: owner,
        exchange_address: format!("{:#x}", contracts.exchange_v2.unwrap()),
        collateral_address: format!("{:#x}", contracts.collateral),
        allowed_conditions: vec![format!("0x{}", "11".repeat(32))],
        ..Default::default()
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let state = Arc::new(Mutex::new(State::default()));
    let shared = state.clone();
    let task = tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let shared = shared.clone();
            tokio::spawn(async move {
                let mut raw = Vec::new();
                let mut buffer = [0u8; 4096];
                let (header_end, length) = loop {
                    let n = stream.read(&mut buffer).await.unwrap();
                    if n == 0 {
                        return;
                    }
                    raw.extend_from_slice(&buffer[..n]);
                    assert!(raw.len() < 16384);
                    if let Some(index) = raw.windows(4).position(|p| p == b"\r\n\r\n") {
                        let header = String::from_utf8_lossy(&raw[..index]);
                        let length = header
                            .lines()
                            .find_map(|line| {
                                let (key, value) = line.split_once(':')?;
                                key.eq_ignore_ascii_case("content-length")
                                    .then(|| value.trim().parse::<usize>().unwrap())
                            })
                            .unwrap();
                        break (index + 4, length);
                    }
                };
                assert!(length < 8192);
                while raw.len() < header_end + length {
                    let n = stream.read(&mut buffer).await.unwrap();
                    assert!(n > 0);
                    raw.extend_from_slice(&buffer[..n]);
                }
                let request =
                    serde_json::from_slice(&raw[header_end..header_end + length]).unwrap();
                let response = reply(&mut shared.lock().unwrap(), request);
                let body = serde_json::to_vec(&response).unwrap();
                let header = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: {}\r\n\r\n",
                    body.len()
                );
                stream.write_all(header.as_bytes()).await.unwrap();
                stream.write_all(&body).await.unwrap();
            });
        }
    });
    let chain = DirectChain::new(live, ChainSettings::default(), url, signer, true).unwrap();
    (chain, Rpc { state, task })
}

#[tokio::test]
async fn actual_exchange_factory_and_separate_approved_closeout_adapter_are_accepted() {
    let (chain, rpc) = client().await;
    let (ctf, underlying) = chain.deployment_identity(BLOCK).await.unwrap();
    assert_eq!(
        ctf,
        polymarket_client_sdk_v2::contract_config(137, false)
            .unwrap()
            .conditional_tokens
    );
    assert_eq!(underlying, CTF_UNDERLYING_USDCE);
    let state = rpc.state.lock().unwrap();
    for adapter in [STANDARD_ADAPTER, EXCHANGE_OUTCOME_FACTORY] {
        for selector in [
            Adapter::COLLATERAL_TOKENCall::SELECTOR,
            Adapter::CONDITIONAL_TOKENSCall::SELECTOR,
            Adapter::USDCECall::SELECTOR,
        ] {
            assert!(state.seen.iter().any(|r| {
                r["params"][0]["to"] == format!("{adapter:#x}")
                    && hex_bytes(r["params"][0]["data"].as_str().unwrap(), 4096).unwrap()[..4]
                        == selector
            }));
        }
    }
    assert!(state.seen.iter().all(|r| r["method"] == "eth_call"));
}

#[tokio::test]
async fn zero_old_or_unknown_exchange_factory_cannot_pass_identity() {
    let (chain, rpc) = client().await;
    let exchange = address(&chain.live.exchange_address).unwrap();
    for wrong in [Address::ZERO, STANDARD_ADAPTER, Address::repeat_byte(0x55)] {
        rpc.state.lock().unwrap().overrides.insert(
            (exchange, Exchange::getOutcomeTokenFactoryCall::SELECTOR),
            address_word(wrong),
        );
        assert!(
            chain
                .deployment_identity(BLOCK)
                .await
                .unwrap_err()
                .contains("deployment identity mismatch")
        );
    }
}

#[tokio::test]
async fn both_adapters_and_exchange_underlying_must_match_the_pinned_tuple() {
    let (chain, rpc) = client().await;
    let mut checks = Vec::new();
    for adapter in [STANDARD_ADAPTER, EXCHANGE_OUTCOME_FACTORY] {
        for selector in [
            Adapter::COLLATERAL_TOKENCall::SELECTOR,
            Adapter::CONDITIONAL_TOKENSCall::SELECTOR,
            Adapter::USDCECall::SELECTOR,
        ] {
            checks.push((adapter, selector));
        }
    }
    checks.push((
        address(&chain.live.exchange_address).unwrap(),
        Exchange::getCtfCollateralCall::SELECTOR,
    ));
    for key in checks {
        rpc.state
            .lock()
            .unwrap()
            .overrides
            .insert(key, address_word(Address::repeat_byte(0x55)));
        assert!(chain.deployment_identity(BLOCK).await.is_err());
        rpc.state.lock().unwrap().overrides.remove(&key);
    }
}

#[tokio::test]
async fn selected_closeout_approval_is_required_even_with_valid_exchange_factory() {
    let (chain, rpc) = client().await;
    rpc.state
        .lock()
        .unwrap()
        .approvals
        .insert(STANDARD_ADAPTER, word(0));
    assert!(
        chain
            .deployment_identity(BLOCK)
            .await
            .unwrap_err()
            .contains("approval missing")
    );
}

#[tokio::test]
async fn external_guard_reads_both_exact_operators_for_the_actual_funder() {
    let (chain, rpc) = client().await;
    chain.verify_no_external_redeemer(BLOCK).await.unwrap();
    let state = rpc.state.lock().unwrap();
    assert_eq!(state.seen.len(), 2);
    for operator in EXTERNAL_AUTO_REDEEMERS {
        assert!(state.seen.iter().any(|request| {
            let data = hex_bytes(request["params"][0]["data"].as_str().unwrap(), 4096).unwrap();
            let call = Ctf::isApprovedForAllCall::abi_decode(&data).unwrap();
            call.owner == address(&chain.live.funder_address).unwrap() && call.operator == operator
        }));
    }
}

#[tokio::test]
async fn either_external_full_balance_redeemer_blocks_the_sole_writer() {
    let (chain, rpc) = client().await;
    for operator in EXTERNAL_AUTO_REDEEMERS {
        rpc.state
            .lock()
            .unwrap()
            .approvals
            .insert(operator, word(1));
        let error = chain.verify_no_external_redeemer(BLOCK).await.unwrap_err();
        assert!(error.contains("external AutoRedeemer"));
        assert!(error.contains(&format!("{operator:#x}")));
        rpc.state.lock().unwrap().approvals.remove(&operator);
    }
}

#[tokio::test]
async fn external_guard_fails_closed_on_bad_boolean_bad_abi_or_rpc_error() {
    let (chain, rpc) = client().await;
    for malformed in [word(2), "0x00".into()] {
        rpc.state
            .lock()
            .unwrap()
            .approvals
            .insert(EXTERNAL_AUTO_REDEEMERS[1], malformed);
        assert!(chain.verify_no_external_redeemer(BLOCK).await.is_err());
    }
    {
        let mut state = rpc.state.lock().unwrap();
        state.approvals.clear();
        state.error_operator = Some(EXTERNAL_AUTO_REDEEMERS[0]);
    }
    assert!(chain.verify_no_external_redeemer(BLOCK).await.is_err());
}
