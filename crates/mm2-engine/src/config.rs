use crate::domain::{CoreConfig, parse_micro};
use serde::Deserialize;
use std::{io::Read, net::SocketAddr, path::PathBuf};

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub mode: String,
    pub feed_source: FeedSource,
    pub listen: SocketAddr,
    pub allow_remote_http: bool,
    pub state_dir: PathBuf,
    pub journal_max_bytes: u64,
    pub command_capacity: usize,
    pub feed_capacity: usize,
    pub limits: Limits,
}

#[derive(Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum FeedSource {
    Demo,
    Public,
}

#[derive(Clone, Debug, Deserialize, serde::Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Limits {
    pub initial_collateral_usd: String,
    pub max_imbalance_shares: String,
    pub lot_shares: String,
    pub half_width: String,
    pub max_feed_age_ms: u64,
}

impl Limits {
    pub fn canonical(&self) -> Result<Self, String> {
        let c = self.core()?;
        Ok(Self {
            initial_collateral_usd: decimal(c.initial_collateral_micro),
            max_imbalance_shares: decimal(c.max_imbalance_micro),
            lot_shares: decimal(c.lot_micro),
            half_width: decimal(c.half_width_micro),
            max_feed_age_ms: c.max_feed_age_ms,
        })
    }
    pub fn core(&self) -> Result<CoreConfig, String> {
        let config = CoreConfig {
            initial_collateral_micro: parse_micro(&self.initial_collateral_usd)?,
            max_imbalance_micro: parse_micro(&self.max_imbalance_shares)?,
            lot_micro: parse_micro(&self.lot_shares)?,
            half_width_micro: parse_micro(&self.half_width)?,
            max_feed_age_ms: self.max_feed_age_ms,
        };
        // The domain constructor is the canonical validator, including cross-field rules.
        crate::domain::Core::new(config.clone())?;
        Ok(config)
    }
}

impl Config {
    pub fn load(path: &std::path::Path) -> Result<Self, String> {
        let mut bytes = Vec::new();
        std::fs::File::open(path)
            .map_err(|_| "cannot read configuration")?
            .take(65_537)
            .read_to_end(&mut bytes)
            .map_err(|_| "cannot read configuration")?;
        if bytes.len() > 65_536 {
            return Err("configuration exceeds 64 KiB".into());
        }
        let text = std::str::from_utf8(&bytes).map_err(|_| "configuration is not UTF-8")?;
        let config: Self = toml::from_str(text).map_err(|_| "invalid configuration schema")?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), String> {
        if !matches!(
            (self.mode.as_str(), self.feed_source),
            ("emulator", FeedSource::Demo)
                | ("shadow", FeedSource::Public)
                | ("shadow", FeedSource::Demo)
        ) {
            return Err("supported pairs are emulator/demo and shadow/public; live gateway is deliberately absent".into());
        }
        if !self.listen.ip().is_loopback() && !self.allow_remote_http {
            return Err(
                "non-loopback listen requires explicit allow_remote_http and a TLS boundary".into(),
            );
        }
        if self.listen.port() == 0
            || self.state_dir.as_os_str().is_empty()
            || !(1_048_576..=67_108_864).contains(&self.journal_max_bytes)
            || !(1..=128).contains(&self.command_capacity)
            || !(16..=512).contains(&self.feed_capacity)
        {
            return Err("configuration exceeds supported resource bounds".into());
        }
        self.limits.core()?;
        Ok(())
    }
}

pub fn decimal(value: u64) -> String {
    if value.is_multiple_of(1_000_000) {
        return (value / 1_000_000).to_string();
    }
    format!("{}.{:06}", value / 1_000_000, value % 1_000_000)
        .trim_end_matches('0')
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packaged_demo_and_public_configs_load_through_the_service_boundary() {
        for (file, source) in [
            ("demo.toml", FeedSource::Demo),
            ("public.toml", FeedSource::Public),
        ] {
            let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../config")
                .join(file);
            let config = Config::load(&path).unwrap();
            assert!(config.feed_source == source);
            assert_eq!(
                config.mode,
                if source == FeedSource::Demo {
                    "emulator"
                } else {
                    "shadow"
                }
            );
            assert_eq!(config.listen, "0.0.0.0:8088".parse::<SocketAddr>().unwrap());
            assert!(config.allow_remote_http);
            assert_eq!(config.state_dir, PathBuf::from("/data"));
            assert_eq!(config.journal_max_bytes, 8 * 1024 * 1024);
            let core = config.limits.core().unwrap();
            assert_eq!(core.initial_collateral_micro, 100_000_000);
            assert_eq!(core.max_imbalance_micro, 10_000_000);
            assert_eq!(core.lot_micro, 5_000_000);
            assert_eq!(core.half_width_micro, 10_000);
        }
    }
}
