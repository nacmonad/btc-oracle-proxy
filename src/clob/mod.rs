//! CLOB ingestion + persistence scaffolding.
//!
//! This module is the Rust-side replacement path for Python `pm_clob_consumer`.
//! It currently provides:
//! - shared models for L1/L2 order-book updates
//! - metric derivation helpers (depth/imbalance/slippage)
//! - an async writer queue interface for DB persistence

pub mod client;
pub mod metrics;
pub mod writer;
pub mod ws;

pub use client::{BookLevel, BookUpdate, Side};
pub use metrics::{DerivedL2Metrics, derive_l2_metrics};
pub use writer::{ClobWriter, DepthLevel, SnapshotRow};
