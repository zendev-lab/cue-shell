//! `cue` — TUI entry point for cue-shell.
//!
//! 1. Load client-side transport config from `client.toml`.
//! 2. For local Unix transport, try to connect to `cued`, auto-starting it if needed.
//! 3. For remote SSH transport, speak the same IPC over `ssh ... "cued gateway --stdio"`.

use std::path::PathBuf;

use anyhow::{Context, Result};
use cue_client::{ResolvedTransport, transport_connector};

use crate::config::Config;
use crate::daemon_lifecycle::restart_handle_for_transport;
use crate::frontend_connection::connect_frontend_transport;

pub fn run() -> Result<()> {
    crate::tracing_config::init_stderr_tracing("warn")?;

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("build tokio runtime")?;

    rt.block_on(async_main())
}

async fn async_main() -> Result<()> {
    let client_config = Config::load()?;
    let transport =
        client_config.resolve_transport(std::env::var_os("CUE_SOCKET").map(PathBuf::from))?;
    let restart_handle = Some(restart_handle_for_transport(&transport));

    let connector = transport_connector(&transport);
    let session_profile_name = Some(match &transport {
        ResolvedTransport::Unix { profile_name, .. }
        | ResolvedTransport::Ssh { profile_name, .. } => profile_name.clone(),
    });
    let client = connect_frontend_transport(&transport).await?;

    let options = cue_tui::RunOptions::new(connector)
        .with_optional_client(client)
        .with_session_profile_name(session_profile_name)
        .with_restart_handle(restart_handle);
    cue_tui::run(options).await
}
