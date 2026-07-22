use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use cue_client::{CuedClient, ResolvedTransport, connect_ssh_transport, load_transport_config};
use cue_core::ipc::{Message, OkPayload, RequestPayload, ResponsePayload};
use cue_core::{EventChannel, Mode};
use serde::Serialize;

fn main() -> Result<()> {
    let cli = parse_cli(std::env::args().skip(1).collect())?;
    if matches!(cli.command, ClientCommand::Help) {
        print_help();
        return Ok(());
    }
    if matches!(cli.command, ClientCommand::Version) {
        println!("cue-client {}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("build tokio runtime")?;
    rt.block_on(run(cli))
}

async fn run(cli: Cli) -> Result<()> {
    match cli.command.clone() {
        ClientCommand::Help | ClientCommand::Version => unreachable!("handled before runtime"),
        ClientCommand::Profiles => {
            let snapshot = cue_client::load_transport_settings_snapshot()?;
            print_json(&snapshot)
        }
        ClientCommand::Ping => {
            let mut client = connect_from_cli(&cli).await?;
            println!("{}", client.ping_for_version().await?);
            Ok(())
        }
        ClientCommand::Watch {
            channels,
            max_events,
        } => {
            let mut client = connect_from_cli(&cli).await?;
            let request_id = client.subscribe(&channels).await?;
            wait_for_ack(&mut client, request_id).await?;
            let mut seen = 0usize;
            while max_events.is_none_or(|max| seen < max) {
                match client.recv().await? {
                    Message::Event { payload } => {
                        print_json(&payload)?;
                        seen += 1;
                    }
                    Message::Response { .. } => {}
                    Message::Request { .. } => bail!("daemon sent a request message to cue-client"),
                }
            }
            Ok(())
        }
        command => {
            let mut client = connect_from_cli(&cli).await?;
            let response = call(&mut client, command.into_request()?).await?;
            print_response(response)
        }
    }
}

async fn connect_from_cli(cli: &Cli) -> Result<CuedClient> {
    let transport = resolve_transport(cli)?;
    connect_transport(&transport).await
}

fn resolve_transport(cli: &Cli) -> Result<ResolvedTransport> {
    if cli.socket_override.is_some() && cli.profile.is_some() {
        bail!("--socket and --profile are mutually exclusive");
    }

    let config = load_transport_config()?;
    if let Some(profile) = &cli.profile {
        config.resolve_profile(profile)
    } else {
        config.resolve_transport(cli.socket_override.clone())
    }
}

async fn connect_transport(transport: &ResolvedTransport) -> Result<CuedClient> {
    match transport {
        ResolvedTransport::Unix { socket_path, .. } => CuedClient::connect(socket_path).await,
        ssh @ ResolvedTransport::Ssh { .. } => {
            let (client, _daemon_version) = connect_ssh_transport(ssh).await?;
            Ok(client)
        }
    }
}

async fn call(client: &mut CuedClient, request: RequestPayload) -> Result<ResponsePayload> {
    let request_id = client.send(request).await?;
    loop {
        match client.recv().await? {
            Message::Response { id, payload } if id == request_id => return Ok(payload),
            Message::Response { .. } | Message::Event { .. } => {}
            Message::Request { .. } => bail!("daemon sent a request message to cue-client"),
        }
    }
}

async fn wait_for_ack(client: &mut CuedClient, request_id: u32) -> Result<()> {
    loop {
        match client.recv().await? {
            Message::Response {
                id,
                payload: ResponsePayload::Ok(OkPayload::Ack {}),
            } if id == request_id => return Ok(()),
            Message::Response {
                id,
                payload: ResponsePayload::Err { code, message },
            } if id == request_id => bail!("daemon error [{code}]: {message}"),
            Message::Response { .. } | Message::Event { .. } => {}
            Message::Request { .. } => bail!("daemon sent a request message to cue-client"),
        }
    }
}

fn print_response(response: ResponsePayload) -> Result<()> {
    match response {
        ResponsePayload::Ok(payload) => print_json(&payload),
        ResponsePayload::Err { code, message } => bail!("daemon error [{code}]: {message}"),
    }
}

fn print_json(value: &impl Serialize) -> Result<()> {
    println!(
        "{}",
        serde_json::to_string_pretty(value).context("serialize response as JSON")?
    );
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Cli {
    socket_override: Option<PathBuf>,
    profile: Option<String>,
    command: ClientCommand,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ClientCommand {
    Help,
    Version,
    Profiles,
    Ping,
    Eval {
        mode: Mode,
        input: String,
    },
    RunScript {
        path: PathBuf,
    },
    ListJobs {
        limit: Option<usize>,
    },
    ListCrons {
        limit: Option<usize>,
    },
    ListScopes {
        limit: Option<usize>,
    },
    Log {
        id: Option<String>,
        limit: Option<usize>,
        tail_bytes: Option<usize>,
    },
    Output {
        id: String,
        stdout_bytes: Option<usize>,
        stderr_bytes: Option<usize>,
    },
    Kill {
        id: String,
    },
    RemoveCron {
        id: String,
    },
    Env {
        tail_bytes: Option<usize>,
    },
    Config {
        tail_bytes: Option<usize>,
    },
    Complete {
        input: String,
        cursor: usize,
    },
    Highlight {
        input: String,
    },
    Shutdown,
    Watch {
        channels: Vec<EventChannel>,
        max_events: Option<usize>,
    },
}

impl ClientCommand {
    fn into_request(self) -> Result<RequestPayload> {
        Ok(match self {
            Self::Help | Self::Version | Self::Profiles | Self::Ping | Self::Watch { .. } => {
                bail!("internal cue-client command cannot be converted to one IPC request")
            }
            Self::Eval { mode, input } => RequestPayload::Eval { input, mode },
            Self::RunScript { path } => RequestPayload::RunScript {
                path: path.display().to_string(),
                input: std::fs::read_to_string(&path)
                    .with_context(|| format!("read script {}", path.display()))?,
            },
            Self::ListJobs { limit } => RequestPayload::ListJobs { limit },
            Self::ListCrons { limit } => RequestPayload::ListCrons { limit },
            Self::ListScopes { limit } => RequestPayload::ListScopes { limit },
            Self::Log {
                id,
                limit,
                tail_bytes,
            } => RequestPayload::ShowLog {
                id,
                limit,
                tail_bytes,
            },
            Self::Output {
                id,
                stdout_bytes,
                stderr_bytes,
            } => RequestPayload::JobOutput {
                id,
                stdout_bytes,
                stderr_bytes,
            },
            Self::Kill { id } => RequestPayload::KillJob { id },
            Self::RemoveCron { id } => RequestPayload::RemoveCron { id },
            Self::Env { tail_bytes } => RequestPayload::ShowEnv { tail_bytes },
            Self::Config { tail_bytes } => RequestPayload::ShowConfig { tail_bytes },
            Self::Complete { input, cursor } => RequestPayload::Complete { input, cursor },
            Self::Highlight { input } => RequestPayload::Highlight { input },
            Self::Shutdown => RequestPayload::Shutdown {},
        })
    }
}

fn parse_cli(args: Vec<String>) -> Result<Cli> {
    let mut args = args.into_iter().peekable();
    let mut socket_override = None;
    let mut profile = None;

    while let Some(arg) = args.peek().cloned() {
        match arg.as_str() {
            "--socket" => {
                args.next();
                socket_override =
                    Some(PathBuf::from(args.next().ok_or_else(|| {
                        anyhow::anyhow!("--socket requires a path")
                    })?));
            }
            "--profile" => {
                args.next();
                profile = Some(
                    args.next()
                        .ok_or_else(|| anyhow::anyhow!("--profile requires a name"))?,
                );
            }
            "-h" | "--help" => return Ok(Cli::new(ClientCommand::Help)),
            "-V" | "--version" => return Ok(Cli::new(ClientCommand::Version)),
            _ if arg.starts_with("--socket=") => {
                args.next();
                socket_override = Some(PathBuf::from(arg.trim_start_matches("--socket=")));
            }
            _ if arg.starts_with("--profile=") => {
                args.next();
                profile = Some(arg.trim_start_matches("--profile=").to_string());
            }
            _ => break,
        }
    }

    let rest: Vec<String> = args.collect();
    let command = parse_command(rest)?;
    Ok(Cli {
        socket_override,
        profile,
        command,
    })
}

impl Cli {
    fn new(command: ClientCommand) -> Self {
        Self {
            socket_override: None,
            profile: None,
            command,
        }
    }
}

fn parse_command(args: Vec<String>) -> Result<ClientCommand> {
    let Some((command, rest)) = args.split_first() else {
        return Ok(ClientCommand::Help);
    };
    let rest = rest.to_vec();

    match command.as_str() {
        "help" => Ok(ClientCommand::Help),
        "version" => Ok(ClientCommand::Version),
        "profiles" | "targets" => no_args(command, &rest).map(|()| ClientCommand::Profiles),
        "ping" => no_args(command, &rest).map(|()| ClientCommand::Ping),
        "eval" => parse_eval(rest),
        "run-script" => parse_run_script(rest),
        "jobs" | "list-jobs" => {
            parse_limit_command(rest, |limit| ClientCommand::ListJobs { limit })
        }
        "crons" | "list-crons" => {
            parse_limit_command(rest, |limit| ClientCommand::ListCrons { limit })
        }
        "scopes" | "list-scopes" => {
            parse_limit_command(rest, |limit| ClientCommand::ListScopes { limit })
        }
        "log" => parse_log(rest),
        "output" => parse_output(rest),
        "kill" => parse_one_id("kill", rest, |id| ClientCommand::Kill { id }),
        "remove-cron" => parse_one_id("remove-cron", rest, |id| ClientCommand::RemoveCron { id }),
        "env" => parse_tail_command(rest, |tail_bytes| ClientCommand::Env { tail_bytes }),
        "config" => parse_tail_command(rest, |tail_bytes| ClientCommand::Config { tail_bytes }),
        "complete" => parse_complete(rest),
        "highlight" => parse_joined_input("highlight", rest, |input| ClientCommand::Highlight {
            input,
        }),
        "shutdown" => no_args(command, &rest).map(|()| ClientCommand::Shutdown),
        "watch" => parse_watch(rest),
        other => bail!("unknown cue-client command `{other}`; use `cue-client --help`"),
    }
}

fn parse_eval(mut args: Vec<String>) -> Result<ClientCommand> {
    let mut mode = Mode::Job;
    if matches!(args.first().map(String::as_str), Some("--mode")) {
        args.remove(0);
        mode = parse_mode(&args.remove(0))?;
    } else if args.first().is_some_and(|arg| arg.starts_with("--mode=")) {
        let value = args.remove(0).trim_start_matches("--mode=").to_string();
        mode = parse_mode(&value)?;
    }
    parse_joined_input("eval", args, |input| ClientCommand::Eval { mode, input })
}

fn parse_mode(value: &str) -> Result<Mode> {
    match value.to_ascii_lowercase().as_str() {
        "job" => Ok(Mode::Job),
        "cron" => Ok(Mode::Cron),
        _ => bail!("mode must be `job` or `cron`"),
    }
}

fn parse_run_script(args: Vec<String>) -> Result<ClientCommand> {
    let [path] = args.as_slice() else {
        bail!("run-script requires exactly one path")
    };
    Ok(ClientCommand::RunScript {
        path: PathBuf::from(path),
    })
}

fn parse_limit_command<F>(args: Vec<String>, build: F) -> Result<ClientCommand>
where
    F: FnOnce(Option<usize>) -> ClientCommand,
{
    let mut limit = None;
    parse_option_usize(args, "--limit", &mut limit)?;
    Ok(build(limit))
}

fn parse_tail_command<F>(args: Vec<String>, build: F) -> Result<ClientCommand>
where
    F: FnOnce(Option<usize>) -> ClientCommand,
{
    let mut tail_bytes = None;
    parse_option_usize(args, "--tail-bytes", &mut tail_bytes)?;
    Ok(build(tail_bytes))
}

fn parse_log(args: Vec<String>) -> Result<ClientCommand> {
    let mut id = None;
    let mut limit = None;
    let mut tail_bytes = None;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--limit" => limit = Some(parse_usize("--limit", args.next())?),
            "--tail-bytes" => tail_bytes = Some(parse_usize("--tail-bytes", args.next())?),
            _ if arg.starts_with("--limit=") => {
                limit = Some(parse_usize_value(
                    "--limit",
                    arg.trim_start_matches("--limit="),
                )?)
            }
            _ if arg.starts_with("--tail-bytes=") => {
                tail_bytes = Some(parse_usize_value(
                    "--tail-bytes",
                    arg.trim_start_matches("--tail-bytes="),
                )?)
            }
            _ if id.is_none() => id = Some(arg),
            _ => bail!("log accepts at most one id"),
        }
    }
    Ok(ClientCommand::Log {
        id,
        limit,
        tail_bytes,
    })
}

fn parse_output(args: Vec<String>) -> Result<ClientCommand> {
    let mut id = None;
    let mut stdout_bytes = None;
    let mut stderr_bytes = None;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--stdout-bytes" => stdout_bytes = Some(parse_usize("--stdout-bytes", args.next())?),
            "--stderr-bytes" => stderr_bytes = Some(parse_usize("--stderr-bytes", args.next())?),
            _ if arg.starts_with("--stdout-bytes=") => {
                stdout_bytes = Some(parse_usize_value(
                    "--stdout-bytes",
                    arg.trim_start_matches("--stdout-bytes="),
                )?)
            }
            _ if arg.starts_with("--stderr-bytes=") => {
                stderr_bytes = Some(parse_usize_value(
                    "--stderr-bytes",
                    arg.trim_start_matches("--stderr-bytes="),
                )?)
            }
            _ if id.is_none() => id = Some(arg),
            _ => bail!("output accepts exactly one job id"),
        }
    }
    Ok(ClientCommand::Output {
        id: id.ok_or_else(|| anyhow::anyhow!("output requires a job id"))?,
        stdout_bytes,
        stderr_bytes,
    })
}

fn parse_complete(args: Vec<String>) -> Result<ClientCommand> {
    let [cursor, input @ ..] = args.as_slice() else {
        bail!("complete requires a cursor and input")
    };
    Ok(ClientCommand::Complete {
        cursor: parse_usize_value("cursor", cursor)?,
        input: input.join(" "),
    })
}

fn parse_watch(args: Vec<String>) -> Result<ClientCommand> {
    let mut channels = Vec::new();
    let mut max_events = None;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--max-events" => max_events = Some(parse_usize("--max-events", args.next())?),
            _ if arg.starts_with("--max-events=") => {
                max_events = Some(parse_usize_value(
                    "--max-events",
                    arg.trim_start_matches("--max-events="),
                )?)
            }
            _ => channels.push(arg),
        }
    }
    if channels.is_empty() {
        bail!(
            "watch requires at least one event channel ({})",
            EventChannel::EXPECTED
        );
    }
    Ok(ClientCommand::Watch {
        channels: EventChannel::parse_list(&channels).map_err(anyhow::Error::new)?,
        max_events,
    })
}

fn parse_one_id<F>(command: &str, args: Vec<String>, build: F) -> Result<ClientCommand>
where
    F: FnOnce(String) -> ClientCommand,
{
    let [id] = args.as_slice() else {
        bail!("{command} requires exactly one id")
    };
    Ok(build(id.clone()))
}

fn parse_joined_input<F>(command: &str, args: Vec<String>, build: F) -> Result<ClientCommand>
where
    F: FnOnce(String) -> ClientCommand,
{
    if args.is_empty() {
        bail!("{command} requires input")
    }
    Ok(build(args.join(" ")))
}

fn parse_option_usize(args: Vec<String>, flag: &str, value: &mut Option<usize>) -> Result<()> {
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        if arg == flag {
            *value = Some(parse_usize(flag, args.next())?);
        } else if let Some(raw) = arg.strip_prefix(&format!("{flag}=")) {
            *value = Some(parse_usize_value(flag, raw)?);
        } else {
            bail!("unexpected argument `{arg}` for {flag} command")
        }
    }
    Ok(())
}

fn parse_usize(flag: &str, value: Option<String>) -> Result<usize> {
    parse_usize_value(
        flag,
        &value.ok_or_else(|| anyhow::anyhow!("{flag} requires a value"))?,
    )
}

fn parse_usize_value(label: &str, value: &str) -> Result<usize> {
    value
        .parse::<usize>()
        .with_context(|| format!("parse {label} value `{value}` as a non-negative integer"))
}

fn no_args(command: &str, rest: &[String]) -> Result<()> {
    if rest.is_empty() {
        Ok(())
    } else {
        bail!("{command} does not accept extra arguments")
    }
}

fn print_help() {
    println!(
        "cue-client {}\n\nUsage: cue-client [--socket PATH | --profile NAME] <command> [args...]\n\nCommands:\n  ping                         Print daemon version\n  profiles                     Print resolved transport profiles as JSON\n  eval [--mode job|cron] ...   Submit Eval input and print response JSON\n  run-script <file.cue>        Submit a .cue file through RunScript\n  jobs [--limit N]             List jobs\n  crons [--limit N]            List crons\n  scopes [--limit N]           List scopes\n  log [ID] [--limit N] [--tail-bytes N]\n  output ID [--stdout-bytes N] [--stderr-bytes N]\n  kill J<N>                    Kill a job\n  remove-cron C<N>             Remove a cron\n  env [--tail-bytes N]         Show HEAD environment\n  config [--tail-bytes N]      Show daemon config\n  complete CURSOR ...          Request completions\n  highlight ...                Request syntax highlighting\n  watch [--max-events N] CHANNEL...\n  shutdown                     Ask daemon to shut down\n\nOptions:\n  --socket PATH    Connect to a Unix socket path\n  --profile NAME   Use a configured transport profile\n  -h, --help       Print help\n  -V, --version    Print version information",
        env!("CARGO_PKG_VERSION")
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_global_socket_and_ping() {
        let cli = parse_cli(vec![
            "--socket".into(),
            "/tmp/cued.sock".into(),
            "ping".into(),
        ])
        .expect("parse cli");

        assert_eq!(cli.socket_override, Some(PathBuf::from("/tmp/cued.sock")));
        assert_eq!(cli.command, ClientCommand::Ping);
    }

    #[test]
    fn parses_eval_mode_and_joined_input() {
        let cli = parse_cli(vec![
            "eval".into(),
            "--mode=cron".into(),
            "every".into(),
            "5m".into(),
            "cargo".into(),
            "test".into(),
        ])
        .expect("parse cli");

        assert_eq!(
            cli.command,
            ClientCommand::Eval {
                mode: Mode::Cron,
                input: "every 5m cargo test".into(),
            }
        );
    }

    #[test]
    fn watch_rejects_invalid_channel() {
        let error = parse_cli(vec!["watch".into(), "output:C1".into()])
            .expect_err("invalid event channel should fail");

        assert!(format!("{error:#}").contains("invalid event channel output:C1"));
    }
}
