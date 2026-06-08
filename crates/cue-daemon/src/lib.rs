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
mod parser;
mod pty;
mod ring_buffer;
mod runtime_env;
mod service;
mod storage;
mod upgrade;
mod word_expansion;

pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
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
