//! `cue` — TUI entry point for cue-shell.
//!
//! 1. Load client-side transport config from `client.toml` (or legacy `config.toml`).
//! 2. For local Unix transport, try to connect to `cued`, auto-starting it if needed.
//! 3. For remote SSH transport, speak the same IPC over `ssh ... "cued gateway --stdio"`.

use std::path::PathBuf;

use anyhow::{Context, Result};
use cue_client::{ResolvedTransport, connect_ssh_transport, transport_connector};

use crate::config::Config;
use crate::daemon_lifecycle::{
    check_local_daemon_version, ensure_daemon_running, restart_handle_for_transport,
    version_from_ping, warn_on_remote_version_mismatch,
};

pub fn run() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_writer(std::io::stderr)
        .init();

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
    validate_transport(&transport)?;
    let restart_handle = Some(restart_handle_for_transport(&transport));

    let connector = transport_connector(&transport);
    let session_profile_name = Some(match &transport {
        ResolvedTransport::Unix { profile_name, .. }
        | ResolvedTransport::Ssh { profile_name, .. } => profile_name.clone(),
    });

    match transport {
        ResolvedTransport::Unix { socket_path, .. } => {
            // Connect (auto-starting daemon if needed). The client is reused by TUI.
            let mut client = ensure_daemon_running(&socket_path).await;
            client = check_local_daemon_version(client, &socket_path).await;
            cue_tui::run(connector, client, session_profile_name, restart_handle).await
        }
        ssh_transport @ ResolvedTransport::Ssh { .. } => {
            let (client, daemon_version) = connect_ssh_transport(&ssh_transport).await?;
            warn_on_remote_version_mismatch(version_from_ping(daemon_version));
            cue_tui::run(
                connector,
                Some(client),
                session_profile_name,
                restart_handle,
            )
            .await
        }
    }
}

fn validate_transport(transport: &ResolvedTransport) -> Result<()> {
    validate_transport_with_lookup(transport, command_in_path)
}

fn validate_transport_with_lookup<F>(
    transport: &ResolvedTransport,
    command_in_path: F,
) -> Result<()>
where
    F: Fn(&str) -> bool,
{
    if let ResolvedTransport::Ssh {
        profile_name,
        destination,
        gateway_command,
        start_command,
    } = transport
    {
        if !command_in_path("ssh") {
            anyhow::bail!(ssh_install_hint(profile_name));
        }
        if destination.trim().is_empty() {
            anyhow::bail!("client profile `{profile_name}` has an empty SSH destination");
        }
        if gateway_command.trim().is_empty() {
            anyhow::bail!("client profile `{profile_name}` has an empty `gateway_command`");
        }
        if start_command.trim().is_empty() {
            anyhow::bail!("client profile `{profile_name}` has an empty `start_command`");
        }
    }
    Ok(())
}

fn ssh_install_hint(profile_name: &str) -> String {
    format!(
        "client profile `{profile_name}` uses `transport = \"ssh\"`, but OpenSSH `ssh` was not found in PATH. cue-shell phase 1 uses the system OpenSSH client. Install it (macOS: `brew install openssh`; Debian/Ubuntu: `sudo apt install openssh-client`; Fedora: `sudo dnf install openssh-clients`) or switch back to a unix transport profile."
    )
}

fn command_in_path(program: &str) -> bool {
    crate::path_lookup::command_in_path(program)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ssh_transport_without_ssh_shows_install_hint() {
        let error = validate_transport_with_lookup(
            &ResolvedTransport::Ssh {
                profile_name: "remote".into(),
                destination: "devbox".into(),
                gateway_command: "cued gateway --stdio".into(),
                start_command: "cued start".into(),
            },
            |_| false,
        )
        .expect_err("missing ssh should fail");

        let message = format!("{error:#}");
        assert!(message.contains("OpenSSH `ssh` was not found in PATH"));
        assert!(message.contains("brew install openssh"));
        assert!(message.contains("sudo apt install openssh-client"));
    }

    #[test]
    fn ssh_transport_rejects_empty_gateway_command() {
        let error = validate_transport_with_lookup(
            &ResolvedTransport::Ssh {
                profile_name: "remote".into(),
                destination: "devbox".into(),
                gateway_command: String::new(),
                start_command: "cued start".into(),
            },
            |_| true,
        )
        .expect_err("empty gateway command should fail");

        assert!(format!("{error:#}").contains("empty `gateway_command`"));
    }
}
