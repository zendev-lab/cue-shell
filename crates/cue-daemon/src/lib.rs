//! cue-daemon — background daemon for cue-shell.
//!
//! This crate contains the parser, actor system, and process manager.

pub mod actor;
pub mod cli;
pub(crate) mod command_util;
pub mod config;
pub mod dirs;
pub mod gateway_stdio;
pub mod parser;
pub mod pty;
pub mod ring_buffer;
pub mod runtime_env;
pub mod service;
pub mod storage;
pub mod upgrade;
pub mod word_expansion;

pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[cfg(test)]
mod tests {
    #[test]
    fn version_is_set() {
        assert!(!crate::version().is_empty());
    }
}
