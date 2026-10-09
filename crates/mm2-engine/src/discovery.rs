use crate::{
    domain::{Core, CoreConfig, Market, parse_micro},
    feeds::validate_token,
};
use serde_json::Value;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

pub fn client() -> Result<reqwest::Client, String> {
    reqwest::Client::builder()
        .user_agent("mm2-public-metadata/1.0")
        .timeout(std::time::Duration::from_secs(10))
        .connect_timeout(std::time::Duration::from_secs(5))
        .redirect(reqwest::redirect::Policy::none())
        .https_only(true)
        .build()
        .map_err(|_| "cannot build public metadata client".into())
}

pub(crate) async fn get(client: &reqwest::Client, url: &str) -> Result<Value, String> {
    let mut response = client
        .get(url)
        .send()
        .await
        .map_err(|_| "public metadata transport failed")?;
    if !response.status().is_success() {
        return Err(format!(
            "public metadata HTTP {}",
            response.status().as_u16()
        ));
    }
    if response.content_length().is_some_and(|n| n > 262_144) {
        return Err("metadata exceeds 256 KiB".into());
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| "metadata body failed")? {
        if bytes.len() + chunk.len() > 262_144 {
            return Err("metadata exceeds 256 KiB".into());
        }
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes).map_err(|_| "metadata schema is not JSON".into())
}

/// Read-only market metadata is independent of order-placement eligibility.
/// The funded venue performs its own fail-closed host/account checks.
pub async fn current_btc(client: &reqwest::Client, now_wall_ms: u64) -> Result<Market, String> {
    current_btc_from(client, now_wall_ms, "https://gamma-api.polymarket.com").await
}

async fn current_btc_from(
    client: &reqwest::Client,
    now_wall_ms: u64,
    gamma_origin: &str,
) -> Result<Market, String> {
    let start = now_wall_ms / 300_000 * 300_000;
    let slug = format!("btc-updown-5m-{}", start / 1000);
    let raw = get(client, &format!("{gamma_origin}/markets/slug/{slug}")).await?;
    parse_market(&raw, start)
}

fn array(value: &Value, key: &str) -> Result<Vec<String>, String> {
    let value = value.get(key).ok_or_else(|| format!("missing {key}"))?;
    let decoded = if let Some(text) = value.as_str() {
        serde_json::from_str(text).map_err(|_| format!("invalid {key}"))?
    } else {
        value.clone()
    };
    let list = decoded.as_array().ok_or_else(|| format!("invalid {key}"))?;
    if list.len() != 2 {
        return Err(format!("unsupported {key} cardinality"));
    }
    list.iter()
        .map(|v| {
            v.as_str()
                .map(str::to_owned)
                .ok_or_else(|| format!("invalid {key} member"))
        })
        .collect()
}

fn timestamp(value: &Value, key: &str) -> Result<u64, String> {
    let stamp = OffsetDateTime::parse(
        value
            .get(key)
            .and_then(Value::as_str)
            .ok_or_else(|| format!("missing {key}"))?,
        &Rfc3339,
    )
    .map_err(|_| format!("invalid {key}"))?
    .unix_timestamp_nanos()
        / 1_000_000;
    u64::try_from(stamp).map_err(|_| format!("negative {key}"))
}

/// Current metadata validates identity and window. It does not supply historical tick or TWAP availability.
pub fn parse_market(raw: &Value, window_start_ms: u64) -> Result<Market, String> {
    let window_end_ms = window_start_ms
        .checked_add(300_000)
        .ok_or("market window overflow")?;
    let expected = format!("btc-updown-5m-{}", window_start_ms / 1000);
    if raw.get("slug").and_then(Value::as_str) != Some(&expected)
        || raw.get("active").and_then(Value::as_bool) != Some(true)
        || raw.get("closed").and_then(Value::as_bool) != Some(false)
        || raw.get("acceptingOrders").and_then(Value::as_bool) != Some(true)
        || timestamp(raw, "eventStartTime")? != window_start_ms
        || timestamp(raw, "endDate")? != window_end_ms
    {
        return Err("market identity, status or five-minute boundaries are unsupported".into());
    }
    let crypto = raw
        .get("cryptoMarketConfig")
        .ok_or("missing market reference contract")?;
    if crypto.get("asset").and_then(Value::as_str) != Some("btc")
        || crypto.get("duration").and_then(Value::as_str) != Some("5m")
        || crypto.get("twapEnabled").and_then(Value::as_bool) != Some(true)
        || crypto.get("twapLookbackSeconds").and_then(Value::as_u64) != Some(60)
    {
        return Err("unsupported market reference contract".into());
    }
    let tokens = array(raw, "clobTokenIds")?;
    let outcomes = array(raw, "outcomes")?;
    if outcomes != ["Up", "Down"] {
        return Err("unsupported outcome ordering".into());
    }
    for token in &tokens {
        validate_token(token)?;
    }
    let tick = raw
        .get("orderPriceMinTickSize")
        .ok_or("missing current tick")?;
    let tick_micro = parse_micro(
        tick.as_str()
            .map(str::to_owned)
            .unwrap_or_else(|| tick.to_string())
            .as_str(),
    )?;
    let market = Market {
        slug: expected,
        condition_id: raw
            .get("conditionId")
            .and_then(Value::as_str)
            .ok_or("missing condition ID")?
            .to_ascii_lowercase(),
        up_token: tokens[0].clone(),
        down_token: tokens[1].clone(),
        window_start_ms,
        window_end_ms,
        tick_micro,
    };
    let mut validation = Core::new(CoreConfig {
        initial_collateral_micro: 1_000_000_000,
        max_imbalance_micro: 25_000_000,
        lot_micro: 5_000_000,
        half_width_micro: 45_000,
        max_feed_age_ms: 1000,
    })?;
    validation.set_market(market.clone())?;
    Ok(market)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Json, Router, http::StatusCode, routing::get};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    fn supported_market() -> Value {
        serde_json::json!({"slug":"btc-updown-5m-1790712600", "active":true,"closed":false,"acceptingOrders":true,
            "eventStartTime":"2026-09-29T20:10:00Z","endDate":"2026-09-29T20:15:00Z","conditionId":format!("0x{}", "a".repeat(64)),
            "clobTokenIds":"[\"1\",\"2\"]","outcomes":"[\"Up\",\"Down\"]","orderPriceMinTickSize":0.01,
            "cryptoMarketConfig":{"asset":"btc","duration":"5m","twapEnabled":true,"twapLookbackSeconds":60}})
    }

    #[tokio::test]
    async fn blocked_or_unavailable_order_eligibility_does_not_gate_read_only_discovery() {
        for geo_status in [StatusCode::OK, StatusCode::SERVICE_UNAVAILABLE] {
            let geo_calls = Arc::new(AtomicUsize::new(0));
            let market_calls = Arc::new(AtomicUsize::new(0));
            let geo_count = geo_calls.clone();
            let market_count = market_calls.clone();
            let routes = Router::new()
                .route(
                    "/api/geoblock",
                    get(move || {
                        geo_count.fetch_add(1, Ordering::Relaxed);
                        async move {
                            (
                                geo_status,
                                Json(serde_json::json!({"blocked":true,"country":"IE"})),
                            )
                        }
                    }),
                )
                .route(
                    "/markets/slug/btc-updown-5m-1790712600",
                    get(move || {
                        market_count.fetch_add(1, Ordering::Relaxed);
                        async { Json(supported_market()) }
                    }),
                );
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let origin = format!("http://{}", listener.local_addr().unwrap());
            let server = tokio::spawn(async move { axum::serve(listener, routes).await.unwrap() });
            // Only this loopback fixture allows HTTP; the production client remains HTTPS-only.
            let fixture_client = reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(2))
                .build()
                .unwrap();
            let geo_response = fixture_client
                .get(format!("{origin}/api/geoblock"))
                .send()
                .await
                .unwrap();
            assert_eq!(geo_response.status().as_u16(), geo_status.as_u16());
            assert_eq!(geo_calls.load(Ordering::Relaxed), 1);
            let market = current_btc_from(&fixture_client, 1_790_712_601_234, &origin)
                .await
                .unwrap();
            assert_eq!(market.slug, "btc-updown-5m-1790712600");
            assert_eq!(market_calls.load(Ordering::Relaxed), 1);
            assert_eq!(geo_calls.load(Ordering::Relaxed), 1);
            server.abort();
            let _ = server.await;
        }
    }

    #[test]
    fn discovery_pins_rules_tokens_outcome_order_and_window() {
        let mut raw = supported_market();
        assert!(parse_market(&raw, 1790712600000).is_ok());
        raw["outcomes"] = serde_json::json!(["Down", "Up"]);
        assert!(parse_market(&raw, 1790712600000).is_err());
        raw["outcomes"] = serde_json::json!(["Up", "Down"]);
        raw["closed"] = true.into();
        assert!(parse_market(&raw, 1790712600000).is_err());
        raw["closed"] = false.into();
        raw["cryptoMarketConfig"]["twapLookbackSeconds"] = 30.into();
        assert!(parse_market(&raw, 1790712600000).is_err());
    }
}
