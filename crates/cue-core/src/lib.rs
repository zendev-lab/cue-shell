//! cue-core — shared types for the cue-shell ecosystem.
//!
//! This crate defines the core domain types and pure scheduling primitives used
//! by both the daemon (cued) and clients (cue-tui, cue-cli). It contains no
//! daemon runtime or I/O logic.

pub mod command;
pub mod command_spec;
pub mod cron;
pub mod event_channel;
pub mod id;
pub mod ipc;
pub mod job;
pub mod mode;
pub mod pipeline;
pub mod process_status;
pub mod scope;

// Re-export commonly used types at crate root.
pub use event_channel::EventChannel;
pub use id::{ChainId, CronId, EntityRef, JobId, ScopeHash, ScriptId};
pub use mode::Mode;
