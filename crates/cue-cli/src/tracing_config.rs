use std::ffi::OsString;

use anyhow::{Context, Result, bail};
use tracing_subscriber::EnvFilter;

const RUST_LOG_ENV: &str = "RUST_LOG";

pub(crate) fn init_stderr_tracing(default_directive: &str) -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(env_filter_from_env(
            default_directive,
            std::env::var_os(RUST_LOG_ENV),
        )?)
        .with_writer(std::io::stderr)
        .try_init()
        .map_err(|error| anyhow::anyhow!("initialize tracing subscriber: {error}"))
}

fn env_filter_from_env(default_directive: &str, rust_log: Option<OsString>) -> Result<EnvFilter> {
    let Some(rust_log) = rust_log else {
        return default_env_filter(default_directive);
    };
    if rust_log.is_empty() {
        bail!("{RUST_LOG_ENV} must not be empty");
    }
    let Some(rust_log) = rust_log.to_str() else {
        bail!("{RUST_LOG_ENV} must be valid UTF-8");
    };
    EnvFilter::try_new(rust_log).with_context(|| format!("parse {RUST_LOG_ENV} `{rust_log}`"))
}

fn default_env_filter(default_directive: &str) -> Result<EnvFilter> {
    EnvFilter::try_new(default_directive)
        .with_context(|| format!("parse default tracing directive `{default_directive}`"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_filter_uses_default_when_rust_log_is_absent() {
        env_filter_from_env("warn", None).expect("default tracing directive should parse");
    }

    #[test]
    fn env_filter_rejects_empty_rust_log() {
        let error = env_filter_from_env("warn", Some(OsString::new()))
            .expect_err("explicit empty RUST_LOG should fail");

        assert_eq!(format!("{error:#}"), "RUST_LOG must not be empty");
    }

    #[test]
    fn env_filter_rejects_invalid_rust_log_instead_of_falling_back() {
        let error = env_filter_from_env("warn", Some(OsString::from("cue_cli=debug,[")))
            .expect_err("invalid RUST_LOG should fail");

        assert!(
            format!("{error:#}").contains("parse RUST_LOG `cue_cli=debug,[`"),
            "unexpected error: {error:#}"
        );
    }
}
