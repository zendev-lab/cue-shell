//! cue-daemon — background daemon for cue-shell.
//!
//! Public entry points are intentionally narrow: the daemon binary launcher,
//! the gateway-stdio bridge used by integration tests, and version reporting.

mod actor;
mod cli;
pub(crate) mod command_util;
mod config;
mod dirs;
mod gateway_stdio;
mod lifecycle;
mod parser;
mod pty;
mod resource;
mod ring_buffer;
mod runtime_env;
mod sandbox;
mod service;
mod storage;
mod upgrade;
mod word_expansion;

pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

static DAEMON_GENERATION_ID: std::sync::OnceLock<String> = std::sync::OnceLock::new();

pub(crate) fn daemon_instance_id() -> &'static str {
    static INSTANCE_ID: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    INSTANCE_ID
        .get_or_init(|| uuid::Uuid::new_v4().to_string())
        .as_str()
}

pub(crate) fn initialize_daemon_generation(
    restart: Option<&crate::lifecycle::RestartRecord>,
) -> anyhow::Result<()> {
    let generation = restart
        .map(|record| record.target_generation.clone())
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    DAEMON_GENERATION_ID
        .set(generation)
        .map_err(|_| anyhow::anyhow!("daemon generation was initialized more than once"))
}

pub(crate) fn daemon_generation_id() -> &'static str {
    DAEMON_GENERATION_ID
        .get_or_init(|| uuid::Uuid::new_v4().to_string())
        .as_str()
}

pub fn run_cli() -> i32 {
    cli::run()
}

pub async fn relay_gateway_stdio<R, W, S>(stdin: R, stdout: W, socket: S) -> anyhow::Result<()>
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
    W: tokio::io::AsyncWrite + Unpin + Send + 'static,
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    gateway_stdio::relay(stdin, stdout, socket).await
}

#[cfg(test)]
mod tests {
    #[test]
    fn version_is_set() {
        assert!(!crate::version().is_empty());
    }
}
