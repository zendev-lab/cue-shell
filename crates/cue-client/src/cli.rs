use std::ffi::OsString;
use std::io::Write;
use std::path::PathBuf;

use anyhow::{Context, bail};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use cue_core::ipc::{
    EventPayload, ForegroundAttachmentInfo, ForegroundRole, Message, SessionInfo, SessionScopeState,
};
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq)]
enum ClientCommand {
    Help,
    Version,
    Run {
        path: PathBuf,
        session_refresh: bool,
    },
    Foreground(ForegroundCommand),
    Session(SessionCommand),
    Target(TargetCommand),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ForegroundCommand {
    Help,
    Watch {
        id: String,
        session: Option<String>,
        session_refresh: bool,
        jsonl: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SessionCommand {
    Help,
    Create {
        name: String,
        json: bool,
    },
    List {
        view: SessionListView,
        json: bool,
    },
    Archive {
        selector: String,
        json: bool,
    },
    Restore {
        selector: String,
        json: bool,
    },
    Attach {
        selector: String,
        json: bool,
        refresh: bool,
    },
    Info {
        selector: String,
        json: bool,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionListView {
    Active,
    Archived,
    All,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TargetCommand {
    Help,
    Resolve { profile: Option<String>, json: bool },
    List { json: bool },
}

pub fn run() -> anyhow::Result<()> {
    match parse_command(std::env::args_os())? {
        ClientCommand::Help => {
            print_help();
            Ok(())
        }
        ClientCommand::Version => {
            println!("cue-client {}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
        ClientCommand::Run {
            path,
            session_refresh,
        } => {
            let code = crate::script_runner::run(path, session_refresh)?;
            std::process::exit(code);
        }
        ClientCommand::Foreground(command) => run_foreground(command),
        ClientCommand::Session(command) => run_session(command),
        ClientCommand::Target(command) => run_target(command),
    }
}

fn parse_command(args: impl IntoIterator<Item = OsString>) -> anyhow::Result<ClientCommand> {
    let mut args = args.into_iter();
    let _program = args.next();

    match args.next().as_deref().and_then(|arg| arg.to_str()) {
        None | Some("-h" | "--help" | "help") => {
            if args.next().is_some() {
                bail!("`cue-client help` does not accept extra arguments");
            }
            Ok(ClientCommand::Help)
        }
        Some("-V" | "--version" | "version") => {
            if args.next().is_some() {
                bail!("`cue-client version` does not accept extra arguments");
            }
            Ok(ClientCommand::Version)
        }
        Some("run") => parse_run_command(args),
        Some("fg") => Ok(ClientCommand::Foreground(parse_foreground_command(
            args.collect(),
        )?)),
        Some("session") => Ok(ClientCommand::Session(parse_session_command(
            args.collect(),
        )?)),
        Some("target") => Ok(ClientCommand::Target(parse_target_command(args.collect())?)),
        Some(other) => {
            bail!(
                "unknown cue-client subcommand `{other}`; supported: help, version, run, fg, session, target"
            )
        }
    }
}

fn parse_run_command(args: impl IntoIterator<Item = OsString>) -> anyhow::Result<ClientCommand> {
    let mut path = None;
    let mut session_refresh = false;
    let mut session_refresh_seen = false;
    for arg in args {
        match arg.to_str() {
            Some("--session-refresh") => {
                if session_refresh_seen {
                    bail!("`--session-refresh` may only be specified once");
                }
                session_refresh = true;
                session_refresh_seen = true;
            }
            Some(value) if value.starts_with('-') => {
                bail!("unknown `cue-client run` option `{value}`")
            }
            Some(_) => {
                if path.replace(PathBuf::from(arg)).is_some() {
                    bail!("`cue-client run` accepts exactly one .cue file path");
                }
            }
            None => bail!("`cue-client run` file path must be valid UTF-8"),
        }
    }
    let path = path.ok_or_else(|| anyhow::anyhow!("`cue-client run` expects a .cue file path"))?;
    if path.extension().and_then(|ext| ext.to_str()) != Some("cue") {
        bail!("`cue-client run` only accepts files with the .cue extension");
    }
    Ok(ClientCommand::Run {
        path,
        session_refresh,
    })
}

fn parse_foreground_command(args: Vec<OsString>) -> anyhow::Result<ForegroundCommand> {
    let mut args = args.into_iter();
    match args.next().as_deref().and_then(|arg| arg.to_str()) {
        None | Some("-h" | "--help" | "help") => {
            if args.next().is_some() {
                bail!("`cue-client fg help` does not accept extra arguments");
            }
            Ok(ForegroundCommand::Help)
        }
        Some("watch") => parse_foreground_watch_args(args),
        Some(other) => {
            bail!("unknown cue-client fg command `{other}`; supported: watch")
        }
    }
}

fn parse_foreground_watch_args(
    args: impl IntoIterator<Item = OsString>,
) -> anyhow::Result<ForegroundCommand> {
    let mut args = args.into_iter();
    let mut id = None;
    let mut session = None;
    let mut session_refresh = false;
    let mut session_refresh_seen = false;
    let mut jsonl = false;
    let mut jsonl_seen = false;

    while let Some(arg) = args.next() {
        match arg.to_str() {
            Some("--session") => {
                if session.is_some() {
                    bail!("`--session` may only be specified once");
                }
                let selector = args
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("`--session` expects a name or ID"))?;
                let selector = selector
                    .into_string()
                    .map_err(|_| anyhow::anyhow!("session selectors must be valid UTF-8"))?;
                if selector.is_empty() {
                    bail!("`--session` expects a non-empty name or ID");
                }
                session = Some(selector);
            }
            Some("--session-refresh") => {
                if session_refresh_seen {
                    bail!("`--session-refresh` may only be specified once");
                }
                session_refresh = true;
                session_refresh_seen = true;
            }
            Some("--jsonl") => {
                if jsonl_seen {
                    bail!("`--jsonl` may only be specified once");
                }
                jsonl = true;
                jsonl_seen = true;
            }
            Some(value) if value.starts_with('-') => {
                bail!("unknown `cue-client fg watch` option `{value}`")
            }
            Some(value) => {
                if id.replace(value.to_string()).is_some() {
                    bail!("`cue-client fg watch` accepts exactly one job ID");
                }
            }
            None => bail!("foreground job IDs must be valid UTF-8"),
        }
    }

    let id = id.ok_or_else(|| anyhow::anyhow!("`cue-client fg watch` expects one job ID"))?;
    Ok(ForegroundCommand::Watch {
        id,
        session,
        session_refresh,
        jsonl,
    })
}

fn parse_session_command(args: Vec<OsString>) -> anyhow::Result<SessionCommand> {
    let mut args = args.into_iter();
    match args.next().as_deref().and_then(|arg| arg.to_str()) {
        None | Some("-h" | "--help" | "help") => {
            if args.next().is_some() {
                bail!("`cue-client session help` does not accept extra arguments");
            }
            Ok(SessionCommand::Help)
        }
        Some("create") => {
            let (name, json) = parse_session_selector_args("create", args, true)?;
            Ok(SessionCommand::Create {
                name: name.expect("required session name"),
                json,
            })
        }
        Some("list") => {
            let (view, json) = parse_session_list_args(args)?;
            Ok(SessionCommand::List { view, json })
        }
        Some("archive") => {
            let (selector, json) = parse_session_selector_args("archive", args, true)?;
            Ok(SessionCommand::Archive {
                selector: selector.expect("required session selector"),
                json,
            })
        }
        Some("restore") => {
            let (selector, json) = parse_session_selector_args("restore", args, true)?;
            Ok(SessionCommand::Restore {
                selector: selector.expect("required session selector"),
                json,
            })
        }
        Some("attach") => {
            let (selector, json, refresh) = parse_session_attach_args(args)?;
            Ok(SessionCommand::Attach {
                selector,
                json,
                refresh,
            })
        }
        Some("info") => {
            let (selector, json) = parse_session_selector_args("info", args, true)?;
            Ok(SessionCommand::Info {
                selector: selector.expect("required session selector"),
                json,
            })
        }
        Some(other) => bail!(
            "unknown cue-client session command `{other}`; supported: create, list, archive, restore, attach, info"
        ),
    }
}

fn parse_session_list_args(
    args: impl IntoIterator<Item = OsString>,
) -> anyhow::Result<(SessionListView, bool)> {
    let mut view = SessionListView::Active;
    let mut view_option = None;
    let mut json = false;
    for arg in args {
        match arg.to_str() {
            Some("--json") => json = true,
            Some(option @ ("--archived" | "--all")) => {
                let (next_view, option_name) = if option == "--archived" {
                    (SessionListView::Archived, "--archived")
                } else {
                    (SessionListView::All, "--all")
                };
                if let Some(previous) = view_option {
                    bail!(
                        "`cue-client session list` options `{previous}` and `{option_name}` are mutually exclusive"
                    );
                }
                view = next_view;
                view_option = Some(option_name);
            }
            Some(value) if value.starts_with('-') => {
                bail!("unknown `cue-client session list` option `{value}`")
            }
            Some(_) => bail!("`cue-client session list` does not accept a session selector"),
            None => bail!("session list arguments must be valid UTF-8"),
        }
    }
    Ok((view, json))
}

fn parse_session_attach_args(
    args: impl IntoIterator<Item = OsString>,
) -> anyhow::Result<(String, bool, bool)> {
    let mut selector = None;
    let mut json = false;
    let mut refresh = false;
    for arg in args {
        match arg.to_str() {
            Some("--json") => json = true,
            Some("--refresh") => refresh = true,
            Some(value) if value.starts_with('-') => {
                bail!("unknown `cue-client session attach` option `{value}`")
            }
            Some(value) => {
                if selector.replace(value.to_string()).is_some() {
                    bail!("`cue-client session attach` accepts at most one session selector");
                }
            }
            None => bail!("session selectors must be valid UTF-8"),
        }
    }
    let selector = selector.ok_or_else(|| {
        anyhow::anyhow!("`cue-client session attach` expects one session selector")
    })?;
    Ok((selector, json, refresh))
}

fn parse_session_selector_args(
    command: &str,
    args: impl IntoIterator<Item = OsString>,
    required: bool,
) -> anyhow::Result<(Option<String>, bool)> {
    let mut selector = None;
    let mut json = false;
    for arg in args {
        match arg.to_str() {
            Some("--json") => json = true,
            Some(value) if value.starts_with('-') => {
                bail!("unknown `cue-client session {command}` option `{value}`")
            }
            Some(value) => {
                if selector.replace(value.to_string()).is_some() {
                    bail!("`cue-client session {command}` accepts at most one session selector");
                }
            }
            None => bail!("session selectors must be valid UTF-8"),
        }
    }
    if required && selector.is_none() {
        bail!("`cue-client session {command}` expects one session selector");
    }
    Ok((selector, json))
}

fn run_foreground(command: ForegroundCommand) -> anyhow::Result<()> {
    if command == ForegroundCommand::Help {
        print_foreground_help();
        return Ok(());
    }

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("build Tokio runtime for foreground command")?;
    runtime.block_on(async move {
        let ForegroundCommand::Watch {
            id,
            session,
            session_refresh,
            jsonl,
        } = command
        else {
            unreachable!("foreground help handled before connecting")
        };

        let selector = resolve_foreground_session(session, std::env::var_os("CUE_SESSION"))?;
        let refresh_if_needed = session_refresh
            || parse_foreground_session_refresh(std::env::var_os("CUE_SESSION_REFRESH"))?;
        if refresh_if_needed && selector.is_none() {
            bail!("session refresh requires --session or CUE_SESSION to select a named session");
        }

        let mut client = connect_for_session_command().await?;
        if let Some(selector) = selector {
            client
                .attach_session_with_refresh_if_needed(&selector, refresh_if_needed)
                .await
                .with_context(|| {
                    if refresh_if_needed {
                        format!(
                            "attach foreground watch to session `{selector}` with explicit restart recovery"
                        )
                    } else {
                        format!(
                            "attach foreground watch to session `{selector}`; if it reports needs_refresh after a daemon restart, rerun with --session-refresh"
                        )
                    }
                })?;
        }

        watch_foreground(&mut client, id, jsonl).await
    })
}

fn resolve_foreground_session(
    explicit: Option<String>,
    environment: Option<OsString>,
) -> anyhow::Result<Option<String>> {
    if explicit.is_some() {
        return Ok(explicit);
    }
    let Some(environment) = environment.filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    environment
        .into_string()
        .map(Some)
        .map_err(|_| anyhow::anyhow!("CUE_SESSION must be valid UTF-8"))
}

fn parse_foreground_session_refresh(value: Option<OsString>) -> anyhow::Result<bool> {
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

async fn watch_foreground(
    client: &mut crate::CuedClient,
    requested_id: String,
    jsonl: bool,
) -> anyhow::Result<()> {
    let attachment = client
        .fg_watch_roundtrip(&requested_id)
        .await
        .with_context(|| format!("watch foreground job `{requested_id}`"))?;
    let mut stdout = std::io::stdout().lock();
    let mut stderr = std::io::stderr().lock();
    emit_foreground_snapshot(&mut stdout, &mut stderr, &attachment, jsonl)?;

    loop {
        match client.recv().await? {
            Message::Event { payload } => {
                let Some(event) =
                    matching_foreground_event(payload, &attachment.id, attachment.attachment_id)
                else {
                    continue;
                };
                let exited = matches!(event, MatchingForegroundEvent::Exited { .. });
                emit_foreground_event(
                    &mut stdout,
                    &mut stderr,
                    &attachment.id,
                    attachment.attachment_id,
                    event,
                    jsonl,
                )?;
                if exited {
                    return Ok(());
                }
            }
            Message::Response { .. } => {}
            Message::Request { .. } => bail!("daemon sent an unexpected request message"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum MatchingForegroundEvent {
    Output { data: Vec<u8> },
    ControlChanged { control_available: bool },
    Exited { reason: String },
}

fn matching_foreground_event(
    payload: EventPayload,
    id: &str,
    attachment_id: u64,
) -> Option<MatchingForegroundEvent> {
    match payload {
        EventPayload::FgOutput {
            id: event_id,
            attachment_id: event_attachment_id,
            data,
        } if foreground_event_matches(id, attachment_id, &event_id, event_attachment_id) => {
            Some(MatchingForegroundEvent::Output { data })
        }
        EventPayload::FgControlChanged {
            id: event_id,
            attachment_id: event_attachment_id,
            control_available,
        } if foreground_event_matches(id, attachment_id, &event_id, event_attachment_id) => {
            Some(MatchingForegroundEvent::ControlChanged { control_available })
        }
        EventPayload::FgExited {
            id: event_id,
            attachment_id: event_attachment_id,
            reason,
        } if foreground_event_matches(id, attachment_id, &event_id, event_attachment_id) => {
            Some(MatchingForegroundEvent::Exited { reason })
        }
        _ => None,
    }
}

fn foreground_event_matches(
    active_job_id: &str,
    active_attachment_id: u64,
    event_job_id: &str,
    event_attachment_id: u64,
) -> bool {
    if active_attachment_id != event_attachment_id {
        return false;
    }
    event_job_id == active_job_id
        || (active_attachment_id == 0 && event_attachment_id == 0 && event_job_id.is_empty())
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ForegroundWatchRecord<'a> {
    Snapshot {
        schema_version: u32,
        job_id: &'a str,
        attachment_id: u64,
        role: ForegroundRole,
        control_available: bool,
        snapshot_truncated: bool,
        data_base64: String,
    },
    Output {
        schema_version: u32,
        job_id: &'a str,
        attachment_id: u64,
        data_base64: String,
    },
    ControlChanged {
        schema_version: u32,
        job_id: &'a str,
        attachment_id: u64,
        control_available: bool,
    },
    Exited {
        schema_version: u32,
        job_id: &'a str,
        attachment_id: u64,
        reason: &'a str,
    },
}

fn emit_foreground_snapshot(
    stdout: &mut impl Write,
    stderr: &mut impl Write,
    attachment: &ForegroundAttachmentInfo,
    jsonl: bool,
) -> anyhow::Result<()> {
    if jsonl {
        write_jsonl(
            stdout,
            &ForegroundWatchRecord::Snapshot {
                schema_version: 1,
                job_id: &attachment.id,
                attachment_id: attachment.attachment_id,
                role: attachment.role,
                control_available: attachment.control_available,
                snapshot_truncated: attachment.snapshot_truncated,
                data_base64: BASE64_STANDARD.encode(&attachment.snapshot),
            },
        )?;
    } else {
        stdout
            .write_all(&attachment.snapshot)
            .context("write foreground snapshot")?;
        stdout.flush().context("flush foreground snapshot")?;
        writeln!(
            stderr,
            "watching {} attachment={} role={} control_available={} snapshot_truncated={}",
            attachment.id,
            attachment.attachment_id,
            foreground_role_name(attachment.role),
            attachment.control_available,
            attachment.snapshot_truncated
        )
        .context("write foreground watch status")?;
    }
    Ok(())
}

fn emit_foreground_event(
    stdout: &mut impl Write,
    stderr: &mut impl Write,
    id: &str,
    attachment_id: u64,
    event: MatchingForegroundEvent,
    jsonl: bool,
) -> anyhow::Result<()> {
    match event {
        MatchingForegroundEvent::Output { data } if jsonl => write_jsonl(
            stdout,
            &ForegroundWatchRecord::Output {
                schema_version: 1,
                job_id: id,
                attachment_id,
                data_base64: BASE64_STANDARD.encode(data),
            },
        ),
        MatchingForegroundEvent::Output { data } => {
            stdout.write_all(&data).context("write foreground output")?;
            stdout.flush().context("flush foreground output")?;
            Ok(())
        }
        MatchingForegroundEvent::ControlChanged { control_available } if jsonl => write_jsonl(
            stdout,
            &ForegroundWatchRecord::ControlChanged {
                schema_version: 1,
                job_id: id,
                attachment_id,
                control_available,
            },
        ),
        MatchingForegroundEvent::ControlChanged { control_available } => {
            writeln!(
                stderr,
                "foreground control for {id} attachment={attachment_id} available={control_available}"
            )
            .context("write foreground control status")?;
            Ok(())
        }
        MatchingForegroundEvent::Exited { reason } if jsonl => write_jsonl(
            stdout,
            &ForegroundWatchRecord::Exited {
                schema_version: 1,
                job_id: id,
                attachment_id,
                reason: &reason,
            },
        ),
        MatchingForegroundEvent::Exited { reason } => {
            writeln!(
                stderr,
                "foreground job {id} attachment={attachment_id} exited: {reason}"
            )
            .context("write foreground exit status")?;
            Ok(())
        }
    }
}

fn write_jsonl(stdout: &mut impl Write, record: &ForegroundWatchRecord<'_>) -> anyhow::Result<()> {
    serde_json::to_writer(&mut *stdout, record).context("serialize foreground JSONL record")?;
    stdout
        .write_all(b"\n")
        .context("write foreground JSONL newline")?;
    stdout.flush().context("flush foreground JSONL record")?;
    Ok(())
}

fn foreground_role_name(role: ForegroundRole) -> &'static str {
    match role {
        ForegroundRole::Controller => "controller",
        ForegroundRole::Observer => "observer",
    }
}

fn parse_target_command(args: Vec<OsString>) -> anyhow::Result<TargetCommand> {
    let mut args = args.into_iter();
    match args.next().as_deref().and_then(|arg| arg.to_str()) {
        None | Some("-h" | "--help" | "help") => {
            if args.next().is_some() {
                bail!("`cue-client target help` does not accept extra arguments");
            }
            Ok(TargetCommand::Help)
        }
        Some("resolve") => {
            let mut json = false;
            let mut profile = None;
            for arg in args {
                match arg.to_str() {
                    Some("--json") => json = true,
                    Some(value) if value.starts_with('-') => {
                        bail!("unknown `cue-client target resolve` option `{value}`")
                    }
                    Some(value) => {
                        if profile.replace(value.to_string()).is_some() {
                            bail!("`cue-client target resolve` accepts at most one profile name");
                        }
                    }
                    None => bail!("target profile names must be valid UTF-8"),
                }
            }
            Ok(TargetCommand::Resolve { profile, json })
        }
        Some("list") => {
            let mut json = false;
            for arg in args {
                match arg.to_str() {
                    Some("--json") => json = true,
                    Some(value) => bail!("unknown `cue-client target list` argument `{value}`"),
                    None => bail!("target list arguments must be valid UTF-8"),
                }
            }
            Ok(TargetCommand::List { json })
        }
        Some(other) => {
            bail!("unknown cue-client target command `{other}`; supported: resolve, list")
        }
    }
}

fn run_session(command: SessionCommand) -> anyhow::Result<()> {
    if command == SessionCommand::Help {
        print_session_help();
        return Ok(());
    }

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("build Tokio runtime for session command")?;
    runtime.block_on(async move {
        let mut client = connect_for_session_command().await?;
        match command {
            SessionCommand::Help => unreachable!("handled before connecting"),
            SessionCommand::Create { name, json } => {
                let session = client.create_session_roundtrip(name).await?;
                print_session_result("created", &session, json)
            }
            SessionCommand::List { view, json } => {
                let sessions = match view {
                    SessionListView::Active => client.list_sessions_roundtrip().await?,
                    SessionListView::Archived => client.list_archived_sessions_roundtrip().await?,
                    SessionListView::All => client.list_all_sessions_roundtrip().await?,
                };
                if json {
                    print_json(&sessions)
                } else if sessions.is_empty() {
                    println!("{}", empty_session_list_message(view));
                    Ok(())
                } else {
                    for session in &sessions {
                        print_session_line(None, session);
                    }
                    Ok(())
                }
            }
            SessionCommand::Archive { selector, json } => {
                let session = client.archive_session_roundtrip(selector).await?;
                print_session_result("archived", &session, json)
            }
            SessionCommand::Restore { selector, json } => {
                let session = client.restore_session_roundtrip(selector).await?;
                print_session_result("restored", &session, json)
            }
            SessionCommand::Attach {
                selector,
                json,
                refresh,
            } => {
                let session = client
                    .attach_session_with_refresh_if_needed(selector, refresh)
                    .await?;
                if json {
                    print_json(&session)
                } else {
                    print_session_line(Some("attached/probed"), &session);
                    println!("control connection exits after this probe");
                    Ok(())
                }
            }
            SessionCommand::Info { selector, json } => {
                let session = client.session_info_roundtrip(Some(selector)).await?;
                print_session_result("session", &session, json)
            }
        }
    })
}

async fn connect_for_session_command() -> anyhow::Result<crate::CuedClient> {
    use crate::ResolvedTransport;
    use crate::daemon_lifecycle::{
        check_local_daemon_version, ensure_daemon_running, version_from_ping,
        warn_on_remote_version_mismatch,
    };

    let transport = crate::load_transport_config()?
        .resolve_transport(std::env::var_os("CUE_SOCKET").map(PathBuf::from))?;
    match transport {
        ResolvedTransport::Unix { socket_path, .. } => {
            let client = ensure_daemon_running(&socket_path).await.ok_or_else(|| {
                anyhow::anyhow!("cued is not available at {}", socket_path.display())
            })?;
            check_local_daemon_version(Some(client), &socket_path)
                .await
                .ok_or_else(|| {
                    anyhow::anyhow!("cued is not available at {}", socket_path.display())
                })
        }
        ssh_transport @ ResolvedTransport::Ssh { .. } => {
            let (client, daemon_version) = crate::connect_ssh_transport(&ssh_transport).await?;
            warn_on_remote_version_mismatch(version_from_ping(daemon_version));
            Ok(client)
        }
    }
}

fn print_session_result(action: &str, session: &SessionInfo, json: bool) -> anyhow::Result<()> {
    if json {
        print_json(session)
    } else {
        print_session_line(Some(action), session);
        Ok(())
    }
}

fn print_session_line(action: Option<&str>, session: &SessionInfo) {
    let marker = if session.current { "*" } else { " " };
    let action = action
        .map(|action| format!("{action} "))
        .unwrap_or_default();
    let lifecycle = session
        .archived_at_ms
        .map(|archived_at_ms| format!("archived archived_at_ms={archived_at_ms}"))
        .unwrap_or_else(|| "active".into());
    println!(
        "{marker} {action}{} {} {} lifecycle={} clients={} restart_safe={}",
        session.name,
        session.id,
        session_scope_state_name(session.scope_state),
        lifecycle,
        session.connected_clients,
        if session.restart_safe { "yes" } else { "no" }
    );
}

fn empty_session_list_message(view: SessionListView) -> &'static str {
    match view {
        SessionListView::Active => "no active named sessions",
        SessionListView::Archived => "no archived named sessions",
        SessionListView::All => "no named sessions",
    }
}

fn session_scope_state_name(state: SessionScopeState) -> &'static str {
    match state {
        SessionScopeState::ReadyDurable => "ready_durable",
        SessionScopeState::ReadyVolatile => "ready_volatile",
        SessionScopeState::NeedsRefresh => "needs_refresh",
    }
}

fn run_target(command: TargetCommand) -> anyhow::Result<()> {
    match command {
        TargetCommand::Help => {
            print_target_help();
            Ok(())
        }
        TargetCommand::Resolve { profile, json } => run_target_resolve(profile, json),
        TargetCommand::List { json } => run_target_list(json),
    }
}

fn run_target_resolve(profile: Option<String>, json: bool) -> anyhow::Result<()> {
    let config = crate::load_transport_config()?;
    let transport = if let Some(profile) = profile {
        config.resolve_profile(&profile)?
    } else {
        config.resolve_transport(std::env::var_os("CUE_SOCKET").map(PathBuf::from))?
    };
    let rendered = ResolvedTargetJson::from_transport(transport);
    if json {
        print_json(&rendered)
    } else {
        println!("{}", rendered.display_line());
        Ok(())
    }
}

fn run_target_list(json: bool) -> anyhow::Result<()> {
    let snapshot = crate::load_transport_settings_snapshot()?;
    let rendered = TargetListJson::from_snapshot(snapshot);
    if json {
        print_json(&rendered)
    } else {
        for profile in rendered.profiles {
            let marker = if profile.name == rendered.default_profile {
                "*"
            } else {
                " "
            };
            println!(
                "{marker} {:<24} {:<5} {} ({})",
                profile.name, profile.transport, profile.detail, profile.source
            );
        }
        Ok(())
    }
}

fn print_json<T: Serialize>(value: &T) -> anyhow::Result<()> {
    let stdout = std::io::stdout();
    let mut handle = stdout.lock();
    serde_json::to_writer_pretty(&mut handle, value).context("serialize JSON")?;
    use std::io::Write as _;
    writeln!(&mut handle).context("write target JSON newline")?;
    Ok(())
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct ResolvedTargetJson {
    schema_version: u32,
    profile_name: String,
    transport: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    socket_path: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    destination: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    gateway_command: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    start_command: Option<String>,
}

impl ResolvedTargetJson {
    fn from_transport(transport: crate::ResolvedTransport) -> Self {
        match transport {
            crate::ResolvedTransport::Unix {
                profile_name,
                socket_path,
            } => Self {
                schema_version: 1,
                profile_name,
                transport: "unix".into(),
                socket_path: Some(socket_path),
                destination: None,
                gateway_command: None,
                start_command: None,
            },
            crate::ResolvedTransport::Ssh {
                profile_name,
                destination,
                gateway_command,
                start_command,
            } => Self {
                schema_version: 1,
                profile_name,
                transport: "ssh".into(),
                socket_path: None,
                destination: Some(destination),
                gateway_command: Some(gateway_command),
                start_command: Some(start_command),
            },
        }
    }

    fn display_line(&self) -> String {
        match self.transport.as_str() {
            "unix" => format!(
                "{} unix {}",
                self.profile_name,
                self.socket_path
                    .as_ref()
                    .map(|path| path.display().to_string())
                    .unwrap_or_default()
            ),
            "ssh" => format!(
                "{} ssh {} via {}",
                self.profile_name,
                self.destination.as_deref().unwrap_or_default(),
                self.gateway_command.as_deref().unwrap_or_default()
            ),
            _ => format!("{} {}", self.profile_name, self.transport),
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct TargetListJson {
    schema_version: u32,
    source_path: PathBuf,
    auto_detect_ssh: bool,
    default_profile: String,
    profiles: Vec<TargetProfileJson>,
}

impl TargetListJson {
    fn from_snapshot(snapshot: crate::TransportSettingsSnapshot) -> Self {
        Self {
            schema_version: 1,
            source_path: snapshot.source_path,
            auto_detect_ssh: snapshot.auto_detect_ssh,
            default_profile: snapshot.default_profile,
            profiles: snapshot
                .profiles
                .into_iter()
                .map(TargetProfileJson::from_summary)
                .collect(),
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct TargetProfileJson {
    name: String,
    transport: String,
    detail: String,
    source: String,
    usable: bool,
}

impl TargetProfileJson {
    fn from_summary(summary: crate::TransportProfileSummary) -> Self {
        let source = match summary.source {
            crate::TransportProfileSource::Local => "local",
            crate::TransportProfileSource::Configured => "configured",
            crate::TransportProfileSource::AutoDetectedSsh => "auto_detected_ssh",
            crate::TransportProfileSource::Missing => "missing",
        }
        .to_string();
        let usable = summary.is_usable_target();
        let transport = summary.transport.as_str().to_string();
        Self {
            name: summary.name,
            transport,
            detail: summary.detail,
            source,
            usable,
        }
    }
}

fn print_help() {
    println!(
        "cue-client {}\n\nUsage:\n  cue-client run <file.cue> [--session-refresh]\n  cue-client fg <command> [args...]\n  cue-client session <command> [args...]\n  cue-client target <command> [args...]\n  cue-client --help\n  cue-client --version\n\nCommands:\n  run       Run a .cue script file (uses CUE_SESSION when set)\n  fg        Persistently watch a foreground PTY job\n  session   Named process-session commands\n  target    Client target/profile commands\n\nRun options:\n  --session-refresh  If CUE_SESSION needs recovery after daemon restart, explicitly replace its scope from this process environment\n\nEnvironment:\n  CUE_SESSION          Default session selector used by `run` and `fg watch`\n  CUE_SESSION_REFRESH  Set to 1 to opt into the same restart recovery as --session-refresh\n\nOptions:\n  -h, --help     Print help\n  -V, --version  Print version information",
        env!("CARGO_PKG_VERSION")
    );
}

fn print_foreground_help() {
    println!(
        "cue-client fg\n\nUsage:\n  cue-client fg watch <Jid> [--session <name-or-id>] [--session-refresh] [--jsonl]\n\nCommands:\n  watch    Attach as a persistent read-only observer; exits when the job exits\n\nOutput:\n  default  Write the exact PTY snapshot and live output bytes to stdout; lifecycle status goes to stderr\n  --jsonl  Emit snapshot, output, control_changed, and exited records; byte fields use base64\n\nOptions:\n  --session <name-or-id>  Select a named session, overriding CUE_SESSION\n  --session-refresh       Explicitly recover a selected needs_refresh session\n  --jsonl                 Emit newline-delimited JSON records\n\nEnvironment:\n  CUE_SOCKET           Override the local daemon socket\n  CUE_SESSION          Default named-session selector\n  CUE_SESSION_REFRESH  Set to 1 to opt into the same restart recovery as --session-refresh"
    );
}

fn print_session_help() {
    println!(
        "cue-client session\n\nUsage:\n  cue-client session create <name> [--json]\n  cue-client session list [--archived | --all] [--json]\n  cue-client session archive <name-or-id> [--json]\n  cue-client session restore <name-or-id> [--json]\n  cue-client session attach <name-or-id> [--refresh] [--json]\n  cue-client session info <name-or-id> [--json]\n\nCommands:\n  create    Create a durable named session from the current scope\n  list      List active sessions by default, archived sessions, or both\n  archive   Reversibly hide an idle session from the default list\n  restore   Make an archived session active and attachable again\n  attach    Attach this control connection, print a probe result, then exit\n  info      Inspect a selected named session\n\nOptions:\n  --archived  List only archived sessions\n  --all       List active and archived sessions\n  --refresh   Explicitly replace a scope that could not survive daemon restart\n  --json      Emit authoritative session metadata as JSON\n\nArchive safety:\n  Archiving never deletes state and refuses sessions with connected clients, non-terminal work, pending scripts/chains, or owned crons. Restore before attaching.\n\nEnvironment:\n  CUE_SOCKET           Override the local daemon socket\n  CUE_SESSION          Session selector used by `cue-client run` before submission\n  CUE_SESSION_REFRESH  Set to 1 to let `cue-client run` recover a needs_refresh session"
    );
}

fn print_target_help() {
    println!(
        "cue-client target\n\nUsage:\n  cue-client target resolve [profile] [--json]\n  cue-client target list [--json]\n\nCommands:\n  resolve   Resolve the active or named client transport profile\n  list      List client transport profiles\n\nExamples:\n  cue-client target resolve --json\n  cue-client target resolve remote-dev --json\n  cue-client target list --json"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bare_client_prints_help() {
        assert_eq!(
            parse_command([OsString::from("cue-client")]).expect("parse command"),
            ClientCommand::Help
        );
    }

    #[test]
    fn parses_run() {
        assert_eq!(
            parse_command([
                OsString::from("cue-client"),
                OsString::from("run"),
                OsString::from("script.cue"),
            ])
            .expect("parse command"),
            ClientCommand::Run {
                path: PathBuf::from("script.cue"),
                session_refresh: false,
            }
        );
    }

    #[test]
    fn parses_explicit_run_session_refresh() {
        assert_eq!(
            parse_command([
                OsString::from("cue-client"),
                OsString::from("run"),
                OsString::from("--session-refresh"),
                OsString::from("script.cue"),
            ])
            .expect("parse session recovery opt-in"),
            ClientCommand::Run {
                path: PathBuf::from("script.cue"),
                session_refresh: true,
            }
        );
    }

    #[test]
    fn rejects_unknown_or_duplicate_run_refresh_flags() {
        let unknown = parse_command([
            OsString::from("cue-client"),
            OsString::from("run"),
            OsString::from("script.cue"),
            OsString::from("--refresh"),
        ])
        .expect_err("session attach's broader refresh flag must not be accepted implicitly");
        assert!(format!("{unknown:#}").contains("unknown `cue-client run` option"));

        let duplicate = parse_command([
            OsString::from("cue-client"),
            OsString::from("run"),
            OsString::from("--session-refresh"),
            OsString::from("--session-refresh"),
            OsString::from("script.cue"),
        ])
        .expect_err("duplicate refresh opt-in should be rejected");
        assert!(format!("{duplicate:#}").contains("may only be specified once"));
    }

    #[test]
    fn rejects_non_cue_run_path() {
        let error = parse_command([
            OsString::from("cue-client"),
            OsString::from("run"),
            OsString::from("script.sh"),
        ])
        .expect_err("non-cue file should fail");
        assert!(format!("{error:#}").contains(".cue extension"));
    }

    #[test]
    fn parses_persistent_foreground_watch() {
        assert_eq!(
            parse_command([
                OsString::from("cue-client"),
                OsString::from("fg"),
                OsString::from("watch"),
                OsString::from("--jsonl"),
                OsString::from("--session"),
                OsString::from("shared-bench"),
                OsString::from("J42"),
                OsString::from("--session-refresh"),
            ])
            .expect("parse persistent foreground watch"),
            ClientCommand::Foreground(ForegroundCommand::Watch {
                id: "J42".into(),
                session: Some("shared-bench".into()),
                session_refresh: true,
                jsonl: true,
            })
        );
    }

    #[test]
    fn foreground_watch_parser_rejects_incomplete_or_ambiguous_input() {
        let missing_id = parse_command([
            OsString::from("cue-client"),
            OsString::from("fg"),
            OsString::from("watch"),
        ])
        .expect_err("watch requires one job ID");
        assert!(format!("{missing_id:#}").contains("expects one job ID"));

        let missing_session = parse_command([
            OsString::from("cue-client"),
            OsString::from("fg"),
            OsString::from("watch"),
            OsString::from("J1"),
            OsString::from("--session"),
        ])
        .expect_err("--session requires its selector");
        assert!(format!("{missing_session:#}").contains("expects a name or ID"));

        let extra_id = parse_command([
            OsString::from("cue-client"),
            OsString::from("fg"),
            OsString::from("watch"),
            OsString::from("J1"),
            OsString::from("J2"),
        ])
        .expect_err("watch must select one job");
        assert!(format!("{extra_id:#}").contains("exactly one job ID"));
    }

    #[test]
    fn explicit_foreground_session_overrides_environment() {
        assert_eq!(
            resolve_foreground_session(
                Some("from-flag".into()),
                Some(OsString::from("from-environment")),
            )
            .expect("resolve selector"),
            Some("from-flag".into())
        );
        assert_eq!(
            resolve_foreground_session(None, Some(OsString::from("from-environment")))
                .expect("resolve selector"),
            Some("from-environment".into())
        );
        assert_eq!(
            resolve_foreground_session(None, Some(OsString::new())).expect("empty selector"),
            None
        );
    }

    #[test]
    fn foreground_event_filter_requires_job_and_attachment_epoch() {
        let matched = matching_foreground_event(
            EventPayload::FgOutput {
                id: "J7".into(),
                attachment_id: 12,
                data: vec![0, 0xff],
            },
            "J7",
            12,
        );
        assert_eq!(
            matched,
            Some(MatchingForegroundEvent::Output {
                data: vec![0, 0xff]
            })
        );

        assert_eq!(
            matching_foreground_event(
                EventPayload::FgControlChanged {
                    id: "J8".into(),
                    attachment_id: 12,
                    control_available: true,
                },
                "J7",
                12,
            ),
            None
        );
        assert_eq!(
            matching_foreground_event(
                EventPayload::FgExited {
                    id: "J7".into(),
                    attachment_id: 11,
                    reason: "stale attachment".into(),
                },
                "J7",
                12,
            ),
            None
        );

        assert_eq!(
            matching_foreground_event(
                EventPayload::FgOutput {
                    id: String::new(),
                    attachment_id: 0,
                    data: b"legacy".to_vec(),
                },
                "J7",
                0,
            ),
            Some(MatchingForegroundEvent::Output {
                data: b"legacy".to_vec()
            })
        );
        assert_eq!(
            matching_foreground_event(
                EventPayload::FgOutput {
                    id: String::new(),
                    attachment_id: 0,
                    data: b"stale".to_vec(),
                },
                "J7",
                12,
            ),
            None,
            "a current attachment must ignore legacy epoch-zero events"
        );
    }

    #[test]
    fn raw_foreground_output_preserves_exact_pty_bytes() {
        let attachment = ForegroundAttachmentInfo {
            id: "J7".into(),
            attachment_id: 12,
            role: ForegroundRole::Observer,
            control_available: true,
            snapshot: vec![0, 0xff, b'\n'],
            snapshot_truncated: false,
        };
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();

        emit_foreground_snapshot(&mut stdout, &mut stderr, &attachment, false)
            .expect("emit raw snapshot");
        emit_foreground_event(
            &mut stdout,
            &mut stderr,
            "J7",
            12,
            MatchingForegroundEvent::ControlChanged {
                control_available: false,
            },
            false,
        )
        .expect("emit raw control status");
        emit_foreground_event(
            &mut stdout,
            &mut stderr,
            "J7",
            12,
            MatchingForegroundEvent::Output {
                data: vec![b'X', 0, 0xfe],
            },
            false,
        )
        .expect("emit raw output");

        assert_eq!(stdout, vec![0, 0xff, b'\n', b'X', 0, 0xfe]);
        let status = String::from_utf8(stderr).expect("status is UTF-8");
        assert!(status.contains("watching J7 attachment=12"));
        assert!(status.contains("available=false"));
    }

    #[test]
    fn jsonl_foreground_records_encode_binary_and_lifecycle_events() {
        let attachment = ForegroundAttachmentInfo {
            id: "J7".into(),
            attachment_id: 12,
            role: ForegroundRole::Observer,
            control_available: true,
            snapshot: vec![0, 0xff],
            snapshot_truncated: true,
        };
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();

        emit_foreground_snapshot(&mut stdout, &mut stderr, &attachment, true)
            .expect("emit JSONL snapshot");
        emit_foreground_event(
            &mut stdout,
            &mut stderr,
            "J7",
            12,
            MatchingForegroundEvent::Exited {
                reason: "process exited".into(),
            },
            true,
        )
        .expect("emit JSONL exit");

        assert!(stderr.is_empty());
        let records = String::from_utf8(stdout).expect("JSONL is UTF-8");
        let records = records
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("valid JSON record"))
            .collect::<Vec<_>>();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["type"], "snapshot");
        assert_eq!(records[0]["job_id"], "J7");
        assert_eq!(records[0]["attachment_id"], 12);
        assert_eq!(records[0]["data_base64"], "AP8=");
        assert_eq!(records[0]["snapshot_truncated"], true);
        assert_eq!(records[1]["type"], "exited");
        assert_eq!(records[1]["reason"], "process exited");
    }

    #[test]
    fn parses_session_create_and_attach_json() {
        assert_eq!(
            parse_command([
                OsString::from("cue-client"),
                OsString::from("session"),
                OsString::from("create"),
                OsString::from("shared-bench"),
                OsString::from("--json"),
            ])
            .expect("parse session create"),
            ClientCommand::Session(SessionCommand::Create {
                name: "shared-bench".into(),
                json: true,
            })
        );
        assert_eq!(
            parse_command([
                OsString::from("cue-client"),
                OsString::from("session"),
                OsString::from("attach"),
                OsString::from("--json"),
                OsString::from("--refresh"),
                OsString::from("S42"),
            ])
            .expect("parse session attach"),
            ClientCommand::Session(SessionCommand::Attach {
                selector: "S42".into(),
                json: true,
                refresh: true,
            })
        );
    }

    #[test]
    fn parses_session_list_and_info_selector() {
        assert_eq!(
            parse_command([
                OsString::from("cue-client"),
                OsString::from("session"),
                OsString::from("list"),
                OsString::from("--json"),
            ])
            .expect("parse session list"),
            ClientCommand::Session(SessionCommand::List {
                view: SessionListView::Active,
                json: true,
            })
        );
        assert_eq!(
            parse_command([
                OsString::from("cue-client"),
                OsString::from("session"),
                OsString::from("info"),
                OsString::from("shared-bench"),
            ])
            .expect("parse selected session info"),
            ClientCommand::Session(SessionCommand::Info {
                selector: "shared-bench".into(),
                json: false,
            })
        );
    }

    #[test]
    fn parses_session_archive_restore_and_filtered_lists() {
        assert_eq!(
            parse_command([
                OsString::from("cue-client"),
                OsString::from("session"),
                OsString::from("archive"),
                OsString::from("shared-bench"),
                OsString::from("--json"),
            ])
            .expect("parse session archive"),
            ClientCommand::Session(SessionCommand::Archive {
                selector: "shared-bench".into(),
                json: true,
            })
        );
        assert_eq!(
            parse_command([
                OsString::from("cue-client"),
                OsString::from("session"),
                OsString::from("restore"),
                OsString::from("S42"),
            ])
            .expect("parse session restore"),
            ClientCommand::Session(SessionCommand::Restore {
                selector: "S42".into(),
                json: false,
            })
        );
        assert_eq!(
            parse_command([
                OsString::from("cue-client"),
                OsString::from("session"),
                OsString::from("list"),
                OsString::from("--archived"),
            ])
            .expect("parse archived session list"),
            ClientCommand::Session(SessionCommand::List {
                view: SessionListView::Archived,
                json: false,
            })
        );
        assert_eq!(
            parse_command([
                OsString::from("cue-client"),
                OsString::from("session"),
                OsString::from("list"),
                OsString::from("--all"),
                OsString::from("--json"),
            ])
            .expect("parse all session list"),
            ClientCommand::Session(SessionCommand::List {
                view: SessionListView::All,
                json: true,
            })
        );

        let error = parse_command([
            OsString::from("cue-client"),
            OsString::from("session"),
            OsString::from("list"),
            OsString::from("--archived"),
            OsString::from("--all"),
        ])
        .expect_err("archived and all filters are mutually exclusive");
        assert!(format!("{error:#}").contains("mutually exclusive"));
    }

    #[test]
    fn session_commands_reject_missing_or_extra_selectors() {
        let missing = parse_command([
            OsString::from("cue-client"),
            OsString::from("session"),
            OsString::from("attach"),
        ])
        .expect_err("attach requires selector");
        assert!(format!("{missing:#}").contains("expects one session selector"));

        let missing_info = parse_command([
            OsString::from("cue-client"),
            OsString::from("session"),
            OsString::from("info"),
        ])
        .expect_err("info requires selector for a one-shot control connection");
        assert!(format!("{missing_info:#}").contains("expects one session selector"));

        let extra = parse_command([
            OsString::from("cue-client"),
            OsString::from("session"),
            OsString::from("list"),
            OsString::from("unexpected"),
        ])
        .expect_err("list rejects selector");
        assert!(format!("{extra:#}").contains("does not accept a session selector"));

        for command in ["archive", "restore"] {
            let missing = parse_command([
                OsString::from("cue-client"),
                OsString::from("session"),
                OsString::from(command),
            ])
            .expect_err("archive lifecycle commands require a selector");
            assert!(format!("{missing:#}").contains("expects one session selector"));
        }
    }

    #[test]
    fn parses_target_resolve_json() {
        assert_eq!(
            parse_command([
                OsString::from("cue-client"),
                OsString::from("target"),
                OsString::from("resolve"),
                OsString::from("remote"),
                OsString::from("--json"),
            ])
            .expect("parse command"),
            ClientCommand::Target(TargetCommand::Resolve {
                profile: Some("remote".into()),
                json: true,
            })
        );
    }

    #[test]
    fn parses_target_list_json() {
        assert_eq!(
            parse_command([
                OsString::from("cue-client"),
                OsString::from("target"),
                OsString::from("list"),
                OsString::from("--json"),
            ])
            .expect("parse command"),
            ClientCommand::Target(TargetCommand::List { json: true })
        );
    }

    #[test]
    fn resolved_unix_json_shape() {
        let rendered = ResolvedTargetJson::from_transport(crate::ResolvedTransport::Unix {
            profile_name: "local".into(),
            socket_path: PathBuf::from("/tmp/cued.sock"),
        });

        assert_eq!(rendered.schema_version, 1);
        assert_eq!(rendered.profile_name, "local");
        assert_eq!(rendered.transport, "unix");
        assert_eq!(rendered.socket_path, Some(PathBuf::from("/tmp/cued.sock")));
        assert!(rendered.destination.is_none());
    }

    #[test]
    fn resolved_ssh_json_shape() {
        let rendered = ResolvedTargetJson::from_transport(crate::ResolvedTransport::Ssh {
            profile_name: "remote".into(),
            destination: "devbox".into(),
            gateway_command: "cued gateway --stdio".into(),
            start_command: "cued start".into(),
        });

        assert_eq!(rendered.schema_version, 1);
        assert_eq!(rendered.profile_name, "remote");
        assert_eq!(rendered.transport, "ssh");
        assert_eq!(rendered.destination.as_deref(), Some("devbox"));
        assert_eq!(
            rendered.gateway_command.as_deref(),
            Some("cued gateway --stdio")
        );
    }

    #[test]
    fn target_list_json_shape() {
        let snapshot = crate::TransportSettingsSnapshot {
            source_path: std::path::Path::new("client.toml").to_path_buf(),
            auto_detect_ssh: true,
            default_profile: "local".into(),
            profiles: vec![crate::TransportProfileSummary {
                name: "local".into(),
                transport: crate::TransportProfileKind::Unix,
                detail: "socket: /tmp/cued.sock".into(),
                source: crate::TransportProfileSource::Local,
            }],
        };

        let rendered = TargetListJson::from_snapshot(snapshot);
        assert_eq!(rendered.schema_version, 1);
        assert_eq!(rendered.default_profile, "local");
        assert_eq!(rendered.profiles[0].source, "local");
        assert!(rendered.profiles[0].usable);
    }
}
