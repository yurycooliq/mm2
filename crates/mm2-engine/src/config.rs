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
    #[serde(default)]
    pub live_state_dir: Option<PathBuf>,
    pub journal_max_bytes: u64,
    #[serde(default)]
    pub journal: Option<crate::journal::JournalOptions>,
    #[serde(default)]
    pub strategy: mm2_strategy::Rules,
    /// Explicit isolated public-paper experiment; never a funded policy switch.
    #[serde(default)]
    pub paper_policy: Option<mm2_strategy::PaperPolicy>,
    #[serde(default)]
    pub live: Option<mm2_live::LiveSettings>,
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
                | ("simulation", FeedSource::Demo | FeedSource::Public)
                | ("shadow", FeedSource::Public)
                | ("shadow", FeedSource::Demo)
                | ("trading" | "both", FeedSource::Public)
        ) {
            return Err("supported modes: emulator/demo, simulation/demo|public, shadow/demo|public, trading/public, both/public".into());
        }
        if self.trades() && self.live.is_none() {
            return Err("trading and both require an explicit [live] configuration".into());
        }
        if self.trades()
            && self
                .live_state_dir
                .as_ref()
                .is_none_or(|path| path.as_os_str().is_empty() || path == &self.state_dir)
        {
            return Err("trading requires a separate persistent live_state_dir shared by the owner account across startup profiles".into());
        }
        if !self.trades() && self.live.is_some() {
            return Err("live settings are forbidden in simulation/shadow profiles".into());
        }
        if !self.trades() && self.live_state_dir.is_some() {
            return Err("live_state_dir is forbidden in simulation/shadow profiles".into());
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
        self.strategy.validate()?;
        if let Some(policy) = &self.paper_policy {
            policy.validate()?;
            if self.mode != "simulation" || self.feed_source != FeedSource::Public {
                return Err("experimental paper policy requires public simulation".into());
            }
        }
        if let Some(options) = &self.journal {
            options.validate()?;
        }
        Ok(())
    }

    pub fn simulates(&self) -> bool {
        matches!(self.mode.as_str(), "emulator" | "simulation" | "both")
    }

    pub fn trades(&self) -> bool {
        matches!(self.mode.as_str(), "trading" | "both")
    }

    /// A persisted profile prevents switching a paper ledger into a real account.
    /// Called only while the engine's directory fence is held.
    pub fn bind_profile(&self) -> Result<(), String> {
        use std::io::Write;
        let path = self.state_dir.join("execution-profile.json");
        let mut identity = serde_json::json!({"schema":1,"mode":self.mode,"feed_source":match self.feed_source { FeedSource::Demo=>"demo",FeedSource::Public=>"public" }});
        if let Some(policy) = &self.paper_policy {
            identity["paper_policy"] = serde_json::to_value(policy)
                .map_err(|_| "cannot encode experimental paper policy")?;
            identity["experimental_strategy"] = serde_json::to_value(&self.strategy)
                .map_err(|_| "cannot encode experimental strategy")?;
        }
        if let Some(path) = &self.live_state_dir {
            identity["live_state_dir"] =
                serde_json::to_value(path).map_err(|_| "cannot encode live state location")?;
        }
        if path.exists() {
            let mut bytes = Vec::new();
            std::fs::File::open(&path)
                .map_err(|_| "cannot read execution profile")?
                .take(4097)
                .read_to_end(&mut bytes)
                .map_err(|_| "cannot read execution profile")?;
            if bytes.len() > 4096
                || serde_json::from_slice::<serde_json::Value>(&bytes)
                    .ok()
                    .as_ref()
                    != Some(&identity)
            {
                return Err("state directory belongs to another execution profile; choose its original profile or a fresh directory".into());
            }
        } else {
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
                .map_err(|_| "cannot create execution profile")?;
            file.write_all(
                &serde_json::to_vec(&identity).map_err(|_| "cannot encode execution profile")?,
            )
            .and_then(|_| file.sync_all())
            .map_err(|_| "cannot persist execution profile")?;
            #[cfg(unix)]
            std::fs::File::open(&self.state_dir)
                .and_then(|f| f.sync_all())
                .map_err(|_| "cannot sync execution profile directory")?;
        }
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
    #[test]
    fn startup_modes_are_explicit_and_state_profiles_cannot_be_crossed() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config");
        for (name, mode) in [
            ("simulation", "simulation"),
            ("trading", "trading"),
            ("both", "both"),
        ] {
            let mut config = Config::load(&root.join(format!("{name}.toml"))).unwrap();
            assert_eq!(config.mode, mode);
            assert_eq!(config.live.is_some(), mode != "simulation");
            let directory = tempfile::tempdir().unwrap();
            config.state_dir = directory.path().into();
            config.bind_profile().unwrap();
            config.bind_profile().unwrap();
            config.mode = "shadow".into();
            assert!(config.bind_profile().is_err());
        }
        let mut config = Config::load(&root.join("demo.toml")).unwrap();
        config.mode = "trading".into();
        assert!(config.validate().is_err());
    }

    #[test]
    fn experimental_policy_is_public_paper_only_and_immutable_for_its_ledger() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config");
        let mut config = Config::load(&root.join("simulation.toml")).unwrap();
        assert!(config.paper_policy.is_none());
        let directory = tempfile::tempdir().unwrap();
        config.state_dir = directory.path().into();
        config.paper_policy = Some(mm2_strategy::PaperPolicy::default());
        config.validate().unwrap();
        config.bind_profile().unwrap();
        config.bind_profile().unwrap();
        config.strategy.max_pair_edge_micros = 30_000;
        assert!(config.bind_profile().is_err());
        config.strategy = mm2_strategy::Rules::default();
        config.paper_policy = None;
        assert!(config.bind_profile().is_err());
        config.paper_policy = Some(mm2_strategy::PaperPolicy::default());
        config.mode = "shadow".into();
        assert!(config.validate().is_err());
        config.mode = "simulation".into();
        config.feed_source = FeedSource::Demo;
        assert!(config.validate().is_err());
        let mut trading = Config::load(&root.join("trading.toml")).unwrap();
        trading.paper_policy = Some(mm2_strategy::PaperPolicy::default());
        assert!(trading.validate().is_err());
    }
}
