//! `cue-tui` — interactive TUI entry point for cue-shell.

use std::ffi::OsString;
use std::path::PathBuf;

use crate::RunOptions;
use crate::tui_debug::{DebugCliCommand, run_debug_command};
use anyhow::{Context, Result, bail};
use cue_client::daemon_lifecycle::{
    check_local_daemon_version, ensure_daemon_running, restart_handle_for_transport,
    version_from_ping, warn_on_remote_version_mismatch,
};
use cue_client::{
    ResolvedTransport, connect_ssh_transport, load_transport_config, transport_connector,
};

#[derive(Debug, Clone, PartialEq, Eq)]
enum TuiCommand {
    Help,
    Version,
    Run {
        debug_socket: Option<PathBuf>,
        session: Option<String>,
        session_refresh: bool,
    },
    Debug {
        socket: PathBuf,
        command: DebugCliCommand,
    },
}

pub fn run() -> anyhow::Result<()> {
    match parse_command(std::env::args_os().skip(1))? {
        TuiCommand::Help => {
            print_help();
            Ok(())
        }
        TuiCommand::Version => {
            println!("cue-tui {}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
        TuiCommand::Run {
            debug_socket,
            session,
            session_refresh,
        } => run_interactive(debug_socket, session, session_refresh),
        TuiCommand::Debug { socket, command } => run_debug_command(socket, command),
    }
}

fn parse_command(args: impl IntoIterator<Item = OsString>) -> Result<TuiCommand> {
    parse_command_with_env(
        args,
        std::env::var_os("CUE_TUI_DEBUG_SOCKET"),
        std::env::var_os("CUE_SESSION"),
        std::env::var_os("CUE_SESSION_REFRESH"),
    )
}

fn parse_command_with_env(
    args: impl IntoIterator<Item = OsString>,
    debug_socket_env: Option<OsString>,
    session_env: Option<OsString>,
    session_refresh_env: Option<OsString>,
) -> Result<TuiCommand> {
    let mut args = args.into_iter().collect::<Vec<_>>();
    let mut debug_socket = debug_socket_env.map(PathBuf::from);
    let mut session_flag = None;
    let mut session_flag_seen = false;
    let mut session_refresh_flag = false;
    let mut session_refresh_flag_seen = false;

    let mut index = 0;
    while index < args.len() {
        let arg = args[index].to_string_lossy().into_owned();
        match arg.as_str() {
            "--debug-socket" => {
                let value = args
                    .get(index + 1)
                    .ok_or_else(|| anyhow::anyhow!("`--debug-socket` requires a path"))?;
                debug_socket = Some(PathBuf::from(value));
                args.remove(index + 1);
                args.remove(index);
                continue;
            }
            "--session" => {
                if session_flag_seen {
                    bail!("`--session` may only be specified once");
                }
                let value = args
                    .get(index + 1)
                    .ok_or_else(|| anyhow::anyhow!("`--session` requires a name or id"))?;
                session_flag = Some(value.clone());
                session_flag_seen = true;
                args.remove(index + 1);
                args.remove(index);
                continue;
            }
            "--session-refresh" => {
                if session_refresh_flag_seen {
                    bail!("`--session-refresh` may only be specified once");
                }
                session_refresh_flag = true;
                session_refresh_flag_seen = true;
                args.remove(index);
                continue;
            }
            "-h" | "--help" | "help" => {
                if index + 1 < args.len() {
                    bail!("`cue-tui help` does not accept extra arguments");
                }
                return Ok(TuiCommand::Help);
            }
            "-V" | "--version" | "version" => {
                if index + 1 < args.len() {
                    bail!("`cue-tui version` does not accept extra arguments");
                }
                return Ok(TuiCommand::Version);
            }
            "debug" => {
                if session_flag_seen || session_refresh_flag_seen {
                    bail!("session flags are only valid for the interactive TUI");
                }
                args.remove(index);
                return parse_debug_command(args, debug_socket);
            }
            _ => index += 1,
        }
    }

    let session = match session_flag {
        Some(value) => parse_session_selector(Some(value), "--session")?,
        None => parse_session_selector(session_env, "CUE_SESSION")?,
    };
    let session_refresh = if session_refresh_flag {
        true
    } else {
        parse_session_refresh(session_refresh_env)?
    };
    if session_refresh && session.is_none() {
        bail!("session refresh requires `--session <name-or-id>` or CUE_SESSION");
    }
    Ok(TuiCommand::Run {
        debug_socket,
        session,
        session_refresh,
    })
}

fn parse_session_selector(value: Option<OsString>, source: &str) -> Result<Option<String>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let value = value
        .into_string()
        .map_err(|_| anyhow::anyhow!("{source} must be valid UTF-8"))?;
    if value.is_empty() {
        bail!("{source} must not be empty");
    }
    if value.trim() != value {
        bail!("{source} must not have leading or trailing whitespace");
    }
    Ok(Some(value))
}

fn parse_session_refresh(value: Option<OsString>) -> Result<bool> {
    let Some(value) = value else {
        return Ok(false);
    };
    let value = value
        .into_string()
        .map_err(|_| anyhow::anyhow!("CUE_SESSION_REFRESH must be valid UTF-8"))?;
    match value.as_str() {
        "1" | "true" => Ok(true),
        "0" | "false" => Ok(false),
        _ => bail!("CUE_SESSION_REFRESH must be one of: 1, true, 0, false"),
    }
}

fn parse_debug_command(
    mut args: Vec<OsString>,
    default_socket: Option<PathBuf>,
) -> Result<TuiCommand> {
    let mut socket = default_socket;
    let mut index = 0;
    while index < args.len() {
        let arg = args[index].to_string_lossy().into_owned();
        if arg == "--socket" {
            let value = args
                .get(index + 1)
                .ok_or_else(|| anyhow::anyhow!("`--socket` requires a path"))?;
            socket = Some(PathBuf::from(value));
            args.remove(index + 1);
            args.remove(index);
            continue;
        }
        index += 1;
    }

    let socket = socket.ok_or_else(|| {
        anyhow::anyhow!("debug commands require `--socket <path>` or `CUE_TUI_DEBUG_SOCKET`")
    })?;

    let command_name = args
        .first()
        .ok_or_else(|| {
            anyhow::anyhow!(
                "missing debug subcommand; supported: capture, send-keys, write-chars, state, subscribe"
            )
        })?
        .to_string_lossy()
        .into_owned();
    args.remove(0);

    let command = match command_name.as_str() {
        "capture" => {
            let styled = args.iter().any(|arg| arg == "--styled");
            if args.iter().any(|arg| arg != "--styled") {
                bail!("`cue-tui debug capture` only accepts `--styled`");
            }
            DebugCliCommand::Capture { styled }
        }
        "send-keys" => {
            if args.is_empty() {
                bail!("`cue-tui debug send-keys` requires at least one key token");
            }
            DebugCliCommand::SendKeys {
                keys: args
                    .into_iter()
                    .map(|arg| arg.to_string_lossy().into_owned())
                    .collect(),
            }
        }
        "write-chars" => {
            let text = args
                .into_iter()
                .map(|arg| arg.to_string_lossy().into_owned())
                .collect::<Vec<_>>()
                .join(" ");
            if text.is_empty() {
                bail!("`cue-tui debug write-chars` requires text");
            }
            DebugCliCommand::WriteChars { text }
        }
        "state" => {
            if !args.is_empty() {
                bail!("`cue-tui debug state` does not accept extra arguments");
            }
            DebugCliCommand::State
        }
        "subscribe" => {
            let styled = args.iter().any(|arg| arg == "--styled");
            if args.iter().any(|arg| arg != "--styled") {
                bail!("`cue-tui debug subscribe` only accepts `--styled`");
            }
            DebugCliCommand::Subscribe { styled }
        }
        other => bail!(
            "unknown debug subcommand `{other}`; supported: capture, send-keys, write-chars, state, subscribe"
        ),
    };

    Ok(TuiCommand::Debug { socket, command })
}

fn run_interactive(
    debug_socket: Option<PathBuf>,
    session: Option<String>,
    session_refresh: bool,
) -> anyhow::Result<()> {
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

    rt.block_on(async_main(debug_socket, session, session_refresh))
}

fn print_help() {
    println!(
        "cue-tui {}\n\nUsage:\n  cue-tui [--session <name-or-id>] [--session-refresh] [--debug-socket <path>]\n  cue-tui debug <command> [--socket <path>]\n  cue-tui --help\n  cue-tui --version\n\nOptions:\n  --session <name-or-id>  Attach to a durable named session (or set CUE_SESSION)\n  --session-refresh       If that session needs recovery after daemon restart, explicitly replace its scope from this process environment (or set CUE_SESSION_REFRESH=1)\n  --debug-socket <path>   Enable the external debug control socket (or set CUE_TUI_DEBUG_SOCKET)\n  -h, --help              Print help\n  -V, --version           Print version information\n\nDebug commands:\n  capture [--styled]                 Print the current rendered frame\n  send-keys <key>...                 Inject named key events\n  write-chars <text>                 Inject character key events\n  state                              Print JSON app-state summary\n  subscribe [--styled]               Stream frame snapshots until disconnect",
        env!("CARGO_PKG_VERSION")
    );
}

async fn async_main(
    debug_socket: Option<PathBuf>,
    session: Option<String>,
    session_refresh: bool,
) -> Result<()> {
    let transport = load_transport_config()?
        .resolve_transport(std::env::var_os("CUE_SOCKET").map(PathBuf::from))?;
    validate_transport(&transport)?;
    let restart_handle = Some(restart_handle_for_transport(&transport));

    let connector = transport_connector(&transport);
    let session_profile_name = Some(match &transport {
        ResolvedTransport::Unix { profile_name, .. }
        | ResolvedTransport::Ssh { profile_name, .. } => profile_name.clone(),
    });

    match transport {
        ResolvedTransport::Unix { socket_path, .. } => {
            let mut client = ensure_daemon_running(&socket_path).await;
            client = check_local_daemon_version(client, &socket_path).await;
            crate::run(
                RunOptions::new(connector)
                    .with_optional_client(client)
                    .with_session_profile_name(session_profile_name)
                    .with_named_session_selector(session)
                    .with_named_session_refresh(session_refresh)
                    .with_restart_handle(restart_handle)
                    .with_debug_socket(debug_socket),
            )
            .await
        }
        ssh_transport @ ResolvedTransport::Ssh { .. } => {
            let (client, daemon_version) = connect_ssh_transport(&ssh_transport).await?;
            warn_on_remote_version_mismatch(version_from_ping(daemon_version));
            crate::run(
                RunOptions::new(connector)
                    .with_client(client)
                    .with_session_profile_name(session_profile_name)
                    .with_named_session_selector(session)
                    .with_named_session_refresh(session_refresh)
                    .with_restart_handle(restart_handle)
                    .with_debug_socket(debug_socket),
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
    let Some(path) = std::env::var_os("PATH") else {
        return false;
    };

    std::env::split_paths(&path).any(|dir| is_executable_file(&dir.join(program)))
}

#[cfg(unix)]
fn is_executable_file(path: &std::path::Path) -> bool {
    use std::os::unix::fs::PermissionsExt;

    std::fs::metadata(path)
        .map(|metadata| metadata.is_file() && metadata.permissions().mode() & 0o111 != 0)
        .unwrap_or(false)
}

#[cfg(not(unix))]
fn is_executable_file(path: &std::path::Path) -> bool {
    path.is_file()
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

    #[test]
    fn parse_debug_socket_flag_for_run_command() {
        let command = parse_command_with_env(
            [
                OsString::from("--debug-socket"),
                OsString::from("/tmp/cue-tui.sock"),
            ],
            None,
            None,
            None,
        )
        .expect("parse run flags");

        assert_eq!(
            command,
            TuiCommand::Run {
                debug_socket: Some(PathBuf::from("/tmp/cue-tui.sock")),
                session: None,
                session_refresh: false,
            }
        );
    }

    #[test]
    fn parse_session_flag_for_run_command() {
        let command = parse_command_with_env(
            [OsString::from("--session"), OsString::from("shared")],
            None,
            None,
            None,
        )
        .expect("parse named session flag");

        assert_eq!(
            command,
            TuiCommand::Run {
                debug_socket: None,
                session: Some("shared".into()),
                session_refresh: false,
            }
        );
    }

    #[test]
    fn parse_explicit_session_refresh_flag() {
        let command = parse_command_with_env(
            [
                OsString::from("--session"),
                OsString::from("shared"),
                OsString::from("--session-refresh"),
            ],
            None,
            None,
            None,
        )
        .expect("parse explicit session recovery");

        assert_eq!(
            command,
            TuiCommand::Run {
                debug_socket: None,
                session: Some("shared".into()),
                session_refresh: true,
            }
        );
    }

    #[test]
    fn cue_session_refresh_env_requires_session_and_explicit_boolean() {
        let command = parse_command_with_env(
            Vec::<OsString>::new(),
            None,
            Some(OsString::from("shared")),
            Some(OsString::from("1")),
        )
        .expect("parse environment recovery opt-in");
        assert_eq!(
            command,
            TuiCommand::Run {
                debug_socket: None,
                session: Some("shared".into()),
                session_refresh: true,
            }
        );

        let no_session = parse_command_with_env(
            Vec::<OsString>::new(),
            None,
            None,
            Some(OsString::from("1")),
        )
        .expect_err("refresh without a selected session must fail");
        assert!(format!("{no_session:#}").contains("session refresh requires"));

        let invalid = parse_command_with_env(
            Vec::<OsString>::new(),
            None,
            Some(OsString::from("shared")),
            Some(OsString::new()),
        )
        .expect_err("empty recovery opt-in must fail closed");
        assert!(format!("{invalid:#}").contains("CUE_SESSION_REFRESH must be one of"));
    }

    #[test]
    fn cue_session_is_used_when_flag_is_absent() {
        let command = parse_command_with_env(
            Vec::<OsString>::new(),
            None,
            Some(OsString::from("from-env")),
            None,
        )
        .expect("parse CUE_SESSION");

        assert_eq!(
            command,
            TuiCommand::Run {
                debug_socket: None,
                session: Some("from-env".into()),
                session_refresh: false,
            }
        );
    }

    #[test]
    fn session_flag_overrides_cue_session() {
        let command = parse_command_with_env(
            [OsString::from("--session"), OsString::from("from-flag")],
            None,
            Some(OsString::new()),
            None,
        )
        .expect("flag should override even an invalid CUE_SESSION");

        assert_eq!(
            command,
            TuiCommand::Run {
                debug_socket: None,
                session: Some("from-flag".into()),
                session_refresh: false,
            }
        );
    }

    #[test]
    fn empty_session_selector_is_rejected() {
        let error =
            parse_command_with_env(Vec::<OsString>::new(), None, Some(OsString::new()), None)
                .expect_err("empty CUE_SESSION must fail");

        assert!(format!("{error:#}").contains("CUE_SESSION must not be empty"));
    }

    #[test]
    fn parse_debug_send_keys_subcommand() {
        let command = parse_command_with_env(
            [
                OsString::from("debug"),
                OsString::from("--socket"),
                OsString::from("/tmp/cue-tui.sock"),
                OsString::from("send-keys"),
                OsString::from("enter"),
                OsString::from("ctrl+c"),
            ],
            None,
            None,
            None,
        )
        .expect("parse debug send-keys");

        assert_eq!(
            command,
            TuiCommand::Debug {
                socket: PathBuf::from("/tmp/cue-tui.sock"),
                command: DebugCliCommand::SendKeys {
                    keys: vec!["enter".into(), "ctrl+c".into()],
                },
            }
        );
    }
}
