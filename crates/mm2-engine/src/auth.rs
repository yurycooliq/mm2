use axum::http::HeaderMap;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    sync::Mutex,
    time::{Duration, Instant},
};
use subtle::ConstantTimeEq;

type HmacSha256 = Hmac<Sha256>;
const AUTH_TTL_SECONDS: u64 = 300;

/// Secrets stay in this infrastructure owner and never enter snapshots or durable events.
pub struct Auth {
    operator_token: String,
    telegram: Option<(String, u64)>,
    sessions: Mutex<BTreeMap<[u8; 32], Instant>>,
}

impl Auth {
    pub fn new(operator_token: String, telegram: Option<(String, u64)>) -> Result<Self, String> {
        if !(32..=256).contains(&operator_token.len())
            || !operator_token
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        {
            return Err(
                "MM2_ENGINE_API_TOKEN must contain 32-256 random ASCII token characters".into(),
            );
        }
        if let Some((token, owner)) = &telegram
            && (token.is_empty() || token.len() > 256 || *owner == 0)
        {
            return Err("invalid Telegram owner configuration".into());
        }
        Ok(Self {
            operator_token,
            telegram,
            sessions: Mutex::new(BTreeMap::new()),
        })
    }

    pub fn from_env() -> Result<Self, String> {
        let token = std::env::var("MM2_ENGINE_API_TOKEN")
            .map_err(|_| "MM2_ENGINE_API_TOKEN is required")?;
        let bot = std::env::var("MM2_ENGINE_TELEGRAM_BOT_TOKEN")
            .ok()
            .filter(|v| !v.is_empty());
        let owner = std::env::var("MM2_ENGINE_TELEGRAM_OWNER_ID")
            .ok()
            .filter(|v| !v.is_empty());
        let telegram = match (bot, owner) {
            (None, None) => None,
            (Some(bot), Some(owner)) => Some((
                bot,
                owner
                    .parse()
                    .map_err(|_| "invalid MM2_ENGINE_TELEGRAM_OWNER_ID")?,
            )),
            _ => return Err("both Telegram environment variables must be set together".into()),
        };
        Self::new(token, telegram)
    }

    pub fn authorized(&self, headers: &HeaderMap) -> bool {
        if headers.get_all("authorization").iter().count() != 1 {
            return false;
        }
        let Some(token) = headers
            .get("authorization")
            .and_then(|h| h.to_str().ok())
            .and_then(|h| h.strip_prefix("Bearer "))
        else {
            return false;
        };
        if token.len() > 256 {
            return false;
        }
        if token.len() == self.operator_token.len()
            && bool::from(token.as_bytes().ct_eq(self.operator_token.as_bytes()))
        {
            return true;
        }
        let digest: [u8; 32] = Sha256::digest(token.as_bytes()).into();
        let Ok(mut sessions) = self.sessions.lock() else {
            return false;
        };
        let now = Instant::now();
        sessions.retain(|_, expires| *expires > now);
        sessions.contains_key(&digest)
    }

    pub fn telegram_session(&self, init_data: &str, now_seconds: u64) -> Result<String, String> {
        let Some((bot, owner)) = &self.telegram else {
            return Err("Telegram authentication is not configured".into());
        };
        validate_telegram(init_data, bot, *owner, now_seconds)?;
        let mut sessions = self
            .sessions
            .lock()
            .map_err(|_| "session store unavailable")?;
        let now = Instant::now();
        sessions.retain(|_, expires| *expires > now);
        if sessions.len() >= 128 {
            return Err("session capacity reached".into());
        }
        let token = format!(
            "tg_{}{}",
            uuid::Uuid::new_v4().simple(),
            uuid::Uuid::new_v4().simple()
        );
        sessions.insert(
            Sha256::digest(token.as_bytes()).into(),
            now + Duration::from_secs(AUTH_TTL_SECONDS),
        );
        Ok(token)
    }
}

/// Telegram HMAC includes every decoded field except hash. Signed user.id is the only authority.
pub fn validate_telegram(
    init_data: &str,
    bot_token: &str,
    owner: u64,
    now: u64,
) -> Result<(), String> {
    if init_data.is_empty() || init_data.len() > 8192 {
        return Err("invalid initData".into());
    }
    let bytes = init_data.as_bytes();
    for (index, byte) in bytes.iter().enumerate() {
        if *byte == b'%'
            && (index + 2 >= bytes.len()
                || !bytes[index + 1].is_ascii_hexdigit()
                || !bytes[index + 2].is_ascii_hexdigit())
        {
            return Err("invalid initData encoding".into());
        }
    }
    let mut fields = BTreeMap::new();
    for (key, value) in url::form_urlencoded::parse(bytes) {
        if key.is_empty()
            || key.len() > 64
            || !key.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
            || value.contains('\u{fffd}')
            || fields
                .insert(key.into_owned(), value.into_owned())
                .is_some()
        {
            return Err("invalid or duplicate initData field".into());
        }
    }
    if fields.len() > 32 {
        return Err("too many initData fields".into());
    }
    let hash = fields.remove("hash").ok_or("missing initData hash")?;
    let signature = hex::decode(hash).map_err(|_| "invalid initData hash")?;
    if signature.len() != 32 {
        return Err("invalid initData hash".into());
    }
    let check = fields
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("\n");
    let mut secret = HmacSha256::new_from_slice(b"WebAppData").map_err(|_| "HMAC unavailable")?;
    secret.update(bot_token.as_bytes());
    let key = secret.finalize().into_bytes();
    let mut mac = HmacSha256::new_from_slice(&key).map_err(|_| "HMAC unavailable")?;
    mac.update(check.as_bytes());
    mac.verify_slice(&signature)
        .map_err(|_| "invalid initData signature")?;
    let auth_date: u64 = fields
        .get("auth_date")
        .ok_or("missing auth_date")?
        .parse()
        .map_err(|_| "invalid auth_date")?;
    if auth_date > now.saturating_add(30) || now.saturating_sub(auth_date) > AUTH_TTL_SECONDS {
        return Err("expired initData".into());
    }
    let user: serde_json::Value =
        serde_json::from_str(fields.get("user").ok_or("missing signed user")?)
            .map_err(|_| "invalid signed user")?;
    if user.get("id").and_then(|id| id.as_u64()) != Some(owner) {
        return Err("owner authorization required".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn signed(user: u64, date: u64, bot: &str) -> String {
        let value = format!("{{\"id\":{user}}}");
        let check = format!("auth_date={date}\nquery_id=fixture\nuser={value}");
        let mut secret = HmacSha256::new_from_slice(b"WebAppData").unwrap();
        secret.update(bot.as_bytes());
        let mut mac = HmacSha256::new_from_slice(&secret.finalize().into_bytes()).unwrap();
        mac.update(check.as_bytes());
        url::form_urlencoded::Serializer::new(String::new())
            .append_pair("user", &value)
            .append_pair("auth_date", &date.to_string())
            .append_pair("query_id", "fixture")
            .append_pair("hash", &hex::encode(mac.finalize().into_bytes()))
            .finish()
    }
    #[test]
    fn signed_owner_and_clock_are_required() {
        let data = signed(42, 1000, "fixture-bot");
        assert!(validate_telegram(&data, "fixture-bot", 42, 1100).is_ok());
        for (bot, owner, now) in [
            ("wrong", 42, 1100),
            ("fixture-bot", 43, 1100),
            ("fixture-bot", 42, 1301),
            ("fixture-bot", 42, 969),
        ] {
            assert!(validate_telegram(&data, bot, owner, now).is_err());
        }
        assert!(
            validate_telegram(&(data.clone() + "&auth_date=1000"), "fixture-bot", 42, 1100)
                .is_err()
        );
        assert!(validate_telegram(&(data + "&bad=%xx"), "fixture-bot", 42, 1100).is_err());
    }
    #[test]
    fn only_bearer_tokens_or_verified_ephemeral_sessions_authorize() {
        let auth = Auth::new("a".repeat(64), Some(("fixture-bot".into(), 42))).unwrap();
        let mut headers = HeaderMap::new();
        assert!(!auth.authorized(&headers));
        headers.insert(
            "authorization",
            format!("Bearer {}", "a".repeat(64)).parse().unwrap(),
        );
        assert!(auth.authorized(&headers));
        let session = auth
            .telegram_session(&signed(42, 1000, "fixture-bot"), 1100)
            .unwrap();
        headers.insert(
            "authorization",
            format!("Bearer {session}").parse().unwrap(),
        );
        assert!(auth.authorized(&headers));
        assert!(
            !Auth::new("a".repeat(64), None)
                .unwrap()
                .authorized(&headers)
        );
        assert!(
            auth.telegram_session(&signed(43, 1000, "fixture-bot"), 1100)
                .is_err()
        );
    }
}
