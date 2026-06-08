use anyhow::{Result, bail};
use cue_client::{CuedClient, ResolvedTransport, connect_ssh_transport};

#[cfg(feature = "tui")]
use crate::daemon_lifecycle::{check_local_daemon_version, ensure_daemon_running};
#[cfg(feature = "script")]
use crate::daemon_lifecycle::{check_required_local_daemon_version, require_daemon_running};
use crate::daemon_lifecycle::{version_from_ping, warn_on_remote_version_mismatch};

/// Connect a CLI-owned frontend to its initial transport.
///
/// This keeps startup policy in `cue-cli`: local profiles may auto-start a
/// sibling `cued`, while SSH profiles use the remote gateway and get a
/// one-shot version warning. Ongoing reconnects remain a `cue-client`
/// responsibility through `ClientConnector`.
#[cfg(feature = "tui")]
pub(crate) async fn connect_frontend_transport(
    transport: &ResolvedTransport,
) -> Result<Option<CuedClient>> {
    validate_frontend_transport(transport)?;

    match transport {
        ResolvedTransport::Unix { socket_path, .. } => {
            let client = ensure_daemon_running(socket_path).await;
            Ok(check_local_daemon_version(client, socket_path).await)
        }
        ssh_transport @ ResolvedTransport::Ssh { .. } => optional_ssh_frontend_connection(
            ssh_transport,
            connect_ssh_transport(ssh_transport).await,
        ),
    }
}

#[cfg(feature = "tui")]
fn optional_ssh_frontend_connection(
    transport: &ResolvedTransport,
    connection: Result<(CuedClient, String)>,
) -> Result<Option<CuedClient>> {
    let ResolvedTransport::Ssh { profile_name, .. } = transport else {
        bail!("expected an ssh transport profile");
    };

    match connection {
        Ok((client, daemon_version)) => {
            warn_on_remote_version_mismatch(version_from_ping(daemon_version));
            Ok(Some(client))
        }
        Err(error) => {
            tracing::warn!(
                %error,
                profile_name,
                "remote cued unavailable, entering offline mode"
            );
            eprintln!("cue: remote cued for profile `{profile_name}` is unavailable:\n{error:#}");
            Ok(None)
        }
    }
}

/// Connect a CLI frontend that cannot run in offline mode.
#[cfg(feature = "script")]
pub(crate) async fn connect_required_frontend_transport(
    transport: &ResolvedTransport,
) -> Result<CuedClient> {
    validate_frontend_transport(transport)?;

    match transport {
        ResolvedTransport::Unix { socket_path, .. } => {
            let client = require_daemon_running(socket_path).await?;
            check_required_local_daemon_version(client, socket_path).await
        }
        ssh_transport @ ResolvedTransport::Ssh { .. } => {
            let (client, daemon_version) = connect_ssh_transport(ssh_transport).await?;
            warn_on_remote_version_mismatch(version_from_ping(daemon_version));
            Ok(client)
        }
    }
}

fn validate_frontend_transport(transport: &ResolvedTransport) -> Result<()> {
    validate_frontend_transport_with_lookup(transport, crate::path_lookup::command_in_path)
}

fn validate_frontend_transport_with_lookup<F>(
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
            bail!(ssh_install_hint(profile_name));
        }
        if destination.trim().is_empty() {
            bail!("client profile `{profile_name}` has an empty SSH destination");
        }
        if gateway_command.trim().is_empty() {
            bail!("client profile `{profile_name}` has an empty `gateway_command`");
        }
        if start_command.trim().is_empty() {
            bail!("client profile `{profile_name}` has an empty `start_command`");
        }
    }
    Ok(())
}

fn ssh_install_hint(profile_name: &str) -> String {
    format!(
        "client profile `{profile_name}` uses `transport = \"ssh\"`, but OpenSSH `ssh` was not found in PATH. cue-shell phase 1 uses the system OpenSSH client. Install it (macOS: `brew install openssh`; Debian/Ubuntu: `sudo apt install openssh-client`; Fedora: `sudo dnf install openssh-clients`) or switch back to a unix transport profile."
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ssh_transport_without_ssh_shows_install_hint() {
        let error = validate_frontend_transport_with_lookup(
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
        let error = validate_frontend_transport_with_lookup(
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

    #[cfg(feature = "tui")]
    #[test]
    fn optional_ssh_frontend_connection_enters_offline_when_remote_is_unavailable() {
        let transport = ResolvedTransport::Ssh {
            profile_name: "remote".into(),
            destination: "devbox".into(),
            gateway_command: "cued gateway --stdio".into(),
            start_command: "cued start".into(),
        };

        let client = optional_ssh_frontend_connection(
            &transport,
            Err(anyhow::anyhow!("remote gateway refused connection")),
        )
        .expect("connection unavailability should not abort TUI startup");

        assert!(client.is_none());
    }
}
