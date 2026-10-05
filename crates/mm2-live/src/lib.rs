//! One-account live executor. SDK I/O is isolated from the pure risk and durable ledger.
pub mod chain;
pub use chain::relay;
mod journal;
mod model;
mod runtime;
mod sdk;
mod settings;
mod settlement;

pub use chain::{
    ChainObservation, ChainOperation, ChainPlan, ChainReceipt, ChainSettings,
    PreparedChainTransaction,
};

pub use model::{
    AccountObservation, ControlAction, LiveControlReceipt, LiveOrder, LivePhase, LivePosition,
    LiveQuote, LiveSnapshot, OrderObservation, OrderPhase,
};
pub use runtime::{LiveHandle, LiveRuntime};
pub use sdk::{PostResult, PreparedOrder, Venue, VenueNotice};
pub use settings::{LiveRiskLimits, LiveSettings};

#[cfg(test)]
mod tests;
