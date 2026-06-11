use std::ffi::OsString;
use std::path::PathBuf;

use anyhow::{Context, bail};
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq)]
enum ClientCommand {
    Help,
    Version,
    Run { path: PathBuf },
    Target(TargetCommand),
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
        ClientCommand::Run { path } => {
            let code = crate::script_runner::run(path)?;
            std::process::exit(code);
        }
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
        Some("run") => {
            let Some(path) = args.next() else {
                bail!("`cue-client run` expects a .cue file path");
            };
            if args.next().is_some() {
                bail!("`cue-client run` accepts exactly one .cue file path");
            }
            let path = PathBuf::from(path);
            if path.extension().and_then(|ext| ext.to_str()) != Some("cue") {
                bail!("`cue-client run` only accepts files with the .cue extension");
            }
            Ok(ClientCommand::Run { path })
        }
        Some("target") => Ok(ClientCommand::Target(parse_target_command(args.collect())?)),
        Some(other) => {
            bail!("unknown cue-client subcommand `{other}`; supported: help, version, run, target")
        }
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
    serde_json::to_writer_pretty(&mut handle, value).context("serialize target JSON")?;
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
        "cue-client {}\n\nUsage:\n  cue-client run <file.cue>\n  cue-client target <command> [args...]\n  cue-client --help\n  cue-client --version\n\nCommands:\n  run       Run a .cue script file\n  target    Client target/profile commands\n\nOptions:\n  -h, --help     Print help\n  -V, --version  Print version information",
        env!("CARGO_PKG_VERSION")
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
            }
        );
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
