//! cue-core — shared types for the cue-shell ecosystem.
//!
//! This crate defines the core domain types used by both the daemon (cued)
//! and clients (cue-tui, cue-cli). It contains no runtime logic.

pub mod command;
pub mod command_spec;
pub mod cron;
pub mod id;
pub mod ipc;
pub mod job;
pub mod mode;
pub mod pipeline;
pub mod scope;

// Re-export commonly used types at crate root.
pub use id::{ChainId, CronId, EntityRef, JobId, ScopeHash, ScriptId};
pub use mode::Mode;
