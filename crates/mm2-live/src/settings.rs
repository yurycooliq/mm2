use polymarket_client_sdk_v2::{
    contract_config, derive_proxy_wallet, derive_safe_wallet, types::Address,
};
use serde::{Deserialize, Serialize};
use std::str::FromStr;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LiveRiskLimits {
    pub max_budget_micros: u64,
    pub max_gross_micros: u64,
    pub max_imbalance_micros: u64,
    pub max_order_micros: u64,
    pub max_orders: usize,
    pub max_markets: usize,
    pub daily_loss_limit_micros: u64,
}
impl Default for LiveRiskLimits {
    fn default() -> Self {
        Self {
            max_budget_micros: 10_000_000,
            max_gross_micros: 10_000_000,
            max_imbalance_micros: 5_000_000,
            max_order_micros: 5_000_000,
            max_orders: 8,
            max_markets: 8,
            daily_loss_limit_micros: 2_000_000,
        }
    }
}
impl LiveRiskLimits {
    pub fn validate(&self) -> Result<(), String> {
        if self.max_budget_micros == 0
            || self.max_budget_micros > 1_000_000_000_000
            || self.max_gross_micros == 0
            || self.max_gross_micros > self.max_budget_micros
            || self.max_imbalance_micros == 0
            || self.max_order_micros == 0
            || self.max_imbalance_micros > 1_000_000_000_000_000
            || self.max_order_micros > 1_000_000_000_000_000
            || !(1..=64).contains(&self.max_orders)
            || !(1..=64).contains(&self.max_markets)
            || self.daily_loss_limit_micros == 0
            || self.daily_loss_limit_micros > self.max_budget_micros
        {
            return Err("invalid finite live risk limits".into());
        }
        Ok(())
    }
}

/// Public configuration only. Values name environment variables, never contain key material.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LiveSettings {
    pub clob_url: String,
    pub owner_address: String,
    pub funder_address: String,
    pub signer_address: String,
    pub signature_type: u8,
    pub chain_id: u64,
    pub protocol: u8,
    pub exchange_address: String,
    pub collateral_address: String,
    pub private_key_env: String,
    pub api_key_env: String,
    pub api_secret_env: String,
    pub api_passphrase_env: String,
    pub allowed_conditions: Vec<String>,
    pub allowed_market_prefixes: Vec<String>,
    pub owner_authorized: bool,
    pub risk: LiveRiskLimits,
    pub request_timeout_ms: u64,
    pub quote_ttl_ms: u64,
    pub heartbeat_interval_ms: u64,
    pub journal_max_bytes: u64,
}
impl Default for LiveSettings {
    fn default() -> Self {
        Self {
            clob_url: "https://clob-v2.polymarket.com".into(),
            owner_address: String::new(),
            funder_address: String::new(),
            signer_address: String::new(),
            signature_type: 0,
            chain_id: 137,
            protocol: 2,
            exchange_address: String::new(),
            collateral_address: String::new(),
            private_key_env: "MM2_POLYMARKET_PRIVATE_KEY".into(),
            api_key_env: "MM2_POLYMARKET_API_KEY".into(),
            api_secret_env: "MM2_POLYMARKET_API_SECRET".into(),
            api_passphrase_env: "MM2_POLYMARKET_API_PASSPHRASE".into(),
            allowed_conditions: vec![],
            allowed_market_prefixes: vec![],
            owner_authorized: false,
            risk: LiveRiskLimits::default(),
            request_timeout_ms: 2000,
            quote_ttl_ms: 1000,
            heartbeat_interval_ms: 2000,
            journal_max_bytes: 64 * 1024 * 1024,
        }
    }
}
pub(crate) fn address(s: &str) -> Result<Address, String> {
    let value = Address::from_str(s).map_err(|_| "invalid configured address")?;
    if value.is_zero() {
        return Err("zero configured address".into());
    }
    Ok(value)
}
pub(crate) fn condition(s: &str) -> bool {
    s.len() == 66 && s.starts_with("0x") && s[2..].bytes().all(|b| b.is_ascii_hexdigit())
}
impl LiveSettings {
    pub fn validate(&self) -> Result<(), String> {
        self.risk.validate()?;
        if !self.owner_authorized {
            return Err("explicit live owner authorization is missing".into());
        }
        if self.chain_id != 137
            || self.protocol != 2
            || self.clob_url != "https://clob-v2.polymarket.com"
        {
            return Err("only explicitly pinned Polygon CLOB V2 token orders are supported".into());
        }
        let signer = address(&self.signer_address)?;
        let funder = address(&self.funder_address)?;
        if address(&self.owner_address)? != funder {
            return Err("owner account and funder must match".into());
        }
        let expected = match self.signature_type {
            0 => Some(signer),
            1 => derive_proxy_wallet(signer, self.chain_id),
            2 => derive_safe_wallet(signer, self.chain_id),
            _ => {
                return Err(
                    "supported signature types are EOA, canonical proxy and canonical safe".into(),
                );
            }
        };
        if expected != Some(funder) {
            return Err("signer/signature type/funder derivation mismatch".into());
        }
        let contract =
            contract_config(self.chain_id, false).ok_or("SDK contract configuration missing")?;
        if Some(address(&self.exchange_address)?) != contract.exchange_v2
            || address(&self.collateral_address)? != contract.collateral
        {
            return Err(
                "configured exchange/collateral differs from pinned SDK V2 deployment".into(),
            );
        }
        if (self.allowed_conditions.is_empty() && self.allowed_market_prefixes.is_empty())
            || self.allowed_conditions.len() > 64
            || self.allowed_conditions.iter().any(|s| !condition(s))
        {
            return Err("explicit bounded allowed condition IDs are required".into());
        }
        if self.allowed_market_prefixes.len() > 1
            || self
                .allowed_market_prefixes
                .iter()
                .any(|s| s != "btc-updown-5m-")
        {
            return Err(
                "only the explicitly approved BTC five-minute family prefix is supported".into(),
            );
        }
        for name in [
            &self.private_key_env,
            &self.api_key_env,
            &self.api_secret_env,
            &self.api_passphrase_env,
        ] {
            if !(1..=96).contains(&name.len())
                || !name
                    .bytes()
                    .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
            {
                return Err("secret references must be bounded environment variable names".into());
            }
        }
        if !(100..=10_000).contains(&self.request_timeout_ms)
            || !(100..=5000).contains(&self.quote_ttl_ms)
            || !(500..=5000).contains(&self.heartbeat_interval_ms)
            || !(1_048_576..=1_073_741_824).contains(&self.journal_max_bytes)
        {
            return Err("live scheduling/storage bounds exceeded".into());
        }
        Ok(())
    }
}
