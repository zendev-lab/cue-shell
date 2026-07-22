use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, bail};

#[derive(Debug, Clone, PartialEq, Eq)]
enum CueCommand {
    Help,
    Version,
    Forward {
        program: String,
        args: Vec<OsString>,
    },
    Extension {
        name: String,
        args: Vec<OsString>,
    },
}

fn main() -> anyhow::Result<()> {
    match parse_command(std::env::args_os())? {
        CueCommand::Help => {
            print_help();
            Ok(())
        }
        CueCommand::Version => {
            println!("cue {}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
        CueCommand::Forward { program, args } => exec_cue_program(&program, &args),
        CueCommand::Extension { name, args } => run_extension(&name, &args),
    }
}

fn parse_command(args: impl IntoIterator<Item = OsString>) -> anyhow::Result<CueCommand> {
    let mut args = args.into_iter();
    let _program = args.next();

    let command = args.next();
    let command = match command.as_deref() {
        Some(command) => Some(
            command
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("cue subcommand must be valid UTF-8"))?,
        ),
        None => None,
    };

    match command {
        None | Some("-h" | "--help" | "help") => {
            if args.next().is_some() {
                bail!("`cue help` does not accept extra arguments");
            }
            Ok(CueCommand::Help)
        }
        Some("-V" | "--version" | "version") => {
            if args.next().is_some() {
                bail!("`cue version` does not accept extra arguments");
            }
            Ok(CueCommand::Version)
        }
        Some("client") => Ok(CueCommand::Forward {
            program: "cue-client".into(),
            args: args.collect(),
        }),
        Some("session") => Ok(CueCommand::Forward {
            program: "cue-client".into(),
            args: std::iter::once(OsString::from("session"))
                .chain(args)
                .collect(),
        }),
        Some("fg") => Ok(CueCommand::Forward {
            program: "cue-client".into(),
            args: std::iter::once(OsString::from("fg")).chain(args).collect(),
        }),
        Some("tui") => Ok(CueCommand::Forward {
            program: "cue-tui".into(),
            args: args.collect(),
        }),
        Some("daemon") => Ok(CueCommand::Forward {
            program: "cue-daemon".into(),
            args: args.collect(),
        }),
        Some("run") => {
            let run_args = args.collect::<Vec<_>>();
            let mut path = None;
            let mut session_refresh_seen = false;
            for arg in &run_args {
                match arg.to_str() {
                    Some("--session-refresh") if !session_refresh_seen => {
                        session_refresh_seen = true;
                    }
                    Some("--session-refresh") => {
                        bail!("`--session-refresh` may only be specified once");
                    }
                    Some(value) if value.starts_with('-') => {
                        bail!("unknown `cue run` option `{value}`");
                    }
                    _ if path.replace(arg).is_some() => {
                        bail!("`cue run` accepts exactly one .cue file path");
                    }
                    _ => {}
                }
            }
            let Some(path) = path else {
                bail!("`cue run` expects a .cue file path");
            };
            let path = PathBuf::from(path);
            if path.extension().and_then(|ext| ext.to_str()) != Some("cue") {
                bail!("`cue run` only accepts files with the .cue extension");
            }
            let mut forwarded = vec![OsString::from("run")];
            forwarded.extend(run_args);
            Ok(CueCommand::Forward {
                program: "cue-client".into(),
                args: forwarded,
            })
        }
        Some("target") => {
            bail!(
                "`cue target` is not supported; use `cue client target ...` or `cue-client target ...`"
            )
        }
        Some(other) => Ok(CueCommand::Extension {
            name: other.to_string(),
            args: args.collect(),
        }),
    }
}

fn exec_cue_program(program: &str, args: &[OsString]) -> anyhow::Result<()> {
    for candidate in cue_program_candidates(program) {
        match Command::new(&candidate).args(args).status() {
            Ok(status) => std::process::exit(process_exit_code(status.code().unwrap_or(1))),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("failed to run `{}`", candidate.display()));
            }
        }
    }

    bail!(
        "required cue command `{program}` was not found next to `cue` or on PATH; install the full cue-shell command set"
    )
}

fn cue_program_candidates(program: &str) -> Vec<PathBuf> {
    let mut candidates = Vec::new();
    if let Ok(current_exe) = std::env::current_exe()
        && let Some(parent) = current_exe.parent()
    {
        push_executable_candidate(&mut candidates, parent.join(program));
        if parent.file_name().is_some_and(|name| name == "deps")
            && let Some(bin_dir) = parent.parent()
        {
            push_executable_candidate(&mut candidates, bin_dir.join(program));
        }
    }
    if let Some(path_candidate) = find_executable_on_path(program) {
        push_unique_path(&mut candidates, path_candidate);
    }
    push_unique_path(&mut candidates, PathBuf::from(program));
    candidates
}

fn push_executable_candidate(paths: &mut Vec<PathBuf>, path: PathBuf) {
    if is_executable_file(&path) {
        push_unique_path(paths, path);
    }
}

fn push_unique_path(paths: &mut Vec<PathBuf>, path: PathBuf) {
    if !paths.contains(&path) {
        paths.push(path);
    }
}

fn find_executable_on_path(program: &str) -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    std::env::split_paths(&path)
        .map(|dir| dir.join(program))
        .find(|candidate| is_executable_file(candidate))
}

#[cfg(unix)]
fn is_executable_file(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;

    std::fs::metadata(path)
        .map(|metadata| metadata.is_file() && metadata.permissions().mode() & 0o111 != 0)
        .unwrap_or(false)
}

#[cfg(not(unix))]
fn is_executable_file(path: &Path) -> bool {
    path.is_file()
}

fn run_extension(name: &str, args: &[OsString]) -> anyhow::Result<()> {
    let code = cue_cli::run_extension(name, args, supported_subcommands())?;
    std::process::exit(process_exit_code(code));
}

fn process_exit_code(code: i32) -> i32 {
    if code < 0 { 1 } else { code }
}

fn print_help() {
    println!("{}", help_text());
}

fn help_text() -> String {
    format!(
        "cue {}\n\nUsage:\n  cue <namespace> [args...]\n  cue run <file.cue> [--session-refresh]\n  cue fg watch <Jid> [--session <name-or-id>] [--session-refresh] [--jsonl]\n  cue --help\n  cue --version\n  cue <extension> [args...]\n\nNamespaces:\n  client      Client-side commands: target profiles, run, IPC utilities\n  session     Named persistent session management\n  tui         Interactive terminal UI\n  daemon      Daemon lifecycle and gateway commands\n  <extension>  Run a configured external command, or cue-<extension> when enabled\n\nShortcuts:\n  run         Alias for `cue client run`\n  fg          Alias for `cue client fg`\n  session     Alias for `cue client session`\n\nExamples:\n  cue session create dev\n  cue session list\n  cue session archive old-dev\n  cue session list --archived\n  cue fg watch J1 --session dev\n  cue client target resolve --json\n  cue run script.cue\n  cue tui --session dev\n  cue daemon status\n\nOptions:\n  --session-refresh  Explicitly recover a selected named session for `cue run` or `cue fg watch`\n  -h, --help          Print help\n  -V, --version       Print version information",
        env!("CARGO_PKG_VERSION"),
    )
}

fn supported_subcommands() -> &'static str {
    "client, session, fg, tui, daemon, run, help, version, <extension>"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_command_defaults_to_help() {
        assert_eq!(
            parse_command([OsString::from("cue")]).expect("parse command"),
            CueCommand::Help
        );
    }

    #[test]
    fn parse_command_accepts_help_and_version() {
        assert_eq!(
            parse_command([OsString::from("cue"), OsString::from("--help")])
                .expect("parse command"),
            CueCommand::Help
        );
        assert_eq!(
            parse_command([OsString::from("cue"), OsString::from("--version")])
                .expect("parse command"),
            CueCommand::Version
        );
    }

    #[cfg(unix)]
    #[test]
    fn parse_command_rejects_non_utf8_subcommand() {
        use std::os::unix::ffi::OsStringExt;

        let error = parse_command([OsString::from("cue"), OsString::from_vec(vec![0xff])])
            .expect_err("non-UTF-8 subcommand should fail");

        assert!(format!("{error:#}").contains("cue subcommand must be valid UTF-8"));
    }

    #[test]
    fn parse_command_forwards_namespaces() {
        assert_eq!(
            parse_command([
                OsString::from("cue"),
                OsString::from("client"),
                OsString::from("target"),
                OsString::from("list"),
            ])
            .expect("parse command"),
            CueCommand::Forward {
                program: "cue-client".into(),
                args: vec![OsString::from("target"), OsString::from("list")],
            }
        );
        assert_eq!(
            parse_command([OsString::from("cue"), OsString::from("tui")]).expect("parse command"),
            CueCommand::Forward {
                program: "cue-tui".into(),
                args: vec![],
            }
        );
        assert_eq!(
            parse_command([
                OsString::from("cue"),
                OsString::from("daemon"),
                OsString::from("status"),
            ])
            .expect("parse command"),
            CueCommand::Forward {
                program: "cue-daemon".into(),
                args: vec![OsString::from("status")],
            }
        );
    }

    #[test]
    fn parse_command_accepts_run_shortcut() {
        assert_eq!(
            parse_command([
                OsString::from("cue"),
                OsString::from("run"),
                OsString::from("build.cue"),
            ])
            .expect("parse command"),
            CueCommand::Forward {
                program: "cue-client".into(),
                args: vec![OsString::from("run"), OsString::from("build.cue")],
            }
        );

        assert_eq!(
            parse_command([
                OsString::from("cue"),
                OsString::from("run"),
                OsString::from("build.cue"),
                OsString::from("--session-refresh"),
            ])
            .expect("parse named-session recovery option"),
            CueCommand::Forward {
                program: "cue-client".into(),
                args: vec![
                    OsString::from("run"),
                    OsString::from("build.cue"),
                    OsString::from("--session-refresh"),
                ],
            }
        );
    }

    #[test]
    fn parse_command_accepts_session_shortcut() {
        assert_eq!(
            parse_command([
                OsString::from("cue"),
                OsString::from("session"),
                OsString::from("attach"),
                OsString::from("dev"),
            ])
            .expect("parse command"),
            CueCommand::Forward {
                program: "cue-client".into(),
                args: vec![
                    OsString::from("session"),
                    OsString::from("attach"),
                    OsString::from("dev"),
                ],
            }
        );
    }

    #[test]
    fn parse_command_accepts_foreground_watch_shortcut() {
        assert_eq!(
            parse_command([
                OsString::from("cue"),
                OsString::from("fg"),
                OsString::from("watch"),
                OsString::from("J3"),
                OsString::from("--session"),
                OsString::from("dev"),
                OsString::from("--jsonl"),
            ])
            .expect("parse foreground shortcut"),
            CueCommand::Forward {
                program: "cue-client".into(),
                args: vec![
                    OsString::from("fg"),
                    OsString::from("watch"),
                    OsString::from("J3"),
                    OsString::from("--session"),
                    OsString::from("dev"),
                    OsString::from("--jsonl"),
                ],
            }
        );
    }

    #[test]
    fn parse_command_rejects_invalid_run_args() {
        let missing = parse_command([OsString::from("cue"), OsString::from("run")])
            .expect_err("missing path should fail");
        assert!(format!("{missing:#}").contains("expects a .cue file path"));

        let non_cue = parse_command([
            OsString::from("cue"),
            OsString::from("run"),
            OsString::from("build.sh"),
        ])
        .expect_err("non-.cue path should fail");
        assert!(format!("{non_cue:#}").contains(".cue extension"));

        let extra = parse_command([
            OsString::from("cue"),
            OsString::from("run"),
            OsString::from("build.cue"),
            OsString::from("extra"),
        ])
        .expect_err("extra args should fail");
        assert!(format!("{extra:#}").contains("exactly one .cue file path"));
    }

    #[test]
    fn parse_command_rejects_target_namespace() {
        let error = parse_command([
            OsString::from("cue"),
            OsString::from("target"),
            OsString::from("list"),
        ])
        .expect_err("cue target should not be supported");
        assert!(format!("{error:#}").contains("`cue target` is not supported"));
        assert!(format!("{error:#}").contains("cue client target"));
    }

    #[test]
    fn parse_command_treats_unknown_namespace_as_extension() {
        assert_eq!(
            parse_command([
                OsString::from("cue"),
                OsString::from("foo"),
                OsString::from("--bar"),
            ])
            .expect("parse command"),
            CueCommand::Extension {
                name: "foo".into(),
                args: vec![OsString::from("--bar")],
            }
        );
    }

    #[test]
    fn help_text_mentions_aggregator_namespaces_and_extensions() {
        let text = help_text();
        assert!(text.contains("cue <namespace> [args...]"));
        assert!(text.contains("cue <extension> [args...]"));
        assert!(text.contains("client"));
        assert!(text.contains("session"));
        assert!(text.contains("tui"));
        assert!(text.contains("daemon"));
        assert!(text.contains("<extension>  Run a configured external command"));
        assert!(text.contains("Alias for `cue client run`"));
        assert!(text.contains("Alias for `cue client session`"));
        assert!(text.contains("cue client target resolve --json"));
    }
}
