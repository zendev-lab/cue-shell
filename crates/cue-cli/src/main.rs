use anyhow::bail;
use std::ffi::OsString;
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Eq)]
enum CueCommand {
    Help,
    Tui,
    Version,
    Run { path: PathBuf },
    Extension { name: String, args: Vec<OsString> },
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
        CueCommand::Tui => run_tui(),
        CueCommand::Run { path } => run_script(path),
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
        Some("-h" | "--help" | "help") => {
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
        None | Some("tui") => {
            if args.next().is_some() {
                bail!("`cue tui` does not accept extra arguments");
            }
            Ok(CueCommand::Tui)
        }
        Some("run") => {
            let Some(path) = args.next() else {
                bail!("`cue run` expects a .cue file path");
            };
            if args.next().is_some() {
                bail!("`cue run` accepts exactly one .cue file path");
            }
            let path = PathBuf::from(path);
            if path.extension().and_then(|ext| ext.to_str()) != Some("cue") {
                bail!("`cue run` only accepts files with the .cue extension");
            }
            Ok(CueCommand::Run { path })
        }
        Some(other) => Ok(CueCommand::Extension {
            name: other.to_string(),
            args: args.collect(),
        }),
    }
}

#[cfg(feature = "tui")]
fn run_tui() -> anyhow::Result<()> {
    cue_cli::run_tui()
}

#[cfg(all(not(feature = "tui"), feature = "extensions"))]
fn run_tui() -> anyhow::Result<()> {
    run_extension("tui", &[])
}

#[cfg(all(not(feature = "tui"), not(feature = "extensions")))]
fn run_tui() -> anyhow::Result<()> {
    bail!(
        "`cue tui` is unavailable because cue-cli was built without the `tui` or `extensions` feature"
    )
}

#[cfg(feature = "script")]
fn run_script(path: PathBuf) -> anyhow::Result<()> {
    let code = cue_cli::run_script(path)?;
    std::process::exit(process_exit_code(code));
}

#[cfg(not(feature = "script"))]
fn run_script(_path: PathBuf) -> anyhow::Result<()> {
    bail!("`cue run` is unavailable because cue-cli was built without the `script` feature")
}

#[cfg(feature = "extensions")]
fn run_extension(name: &str, args: &[OsString]) -> anyhow::Result<()> {
    let code = cue_cli::run_extension(name, args, supported_subcommands())?;
    std::process::exit(process_exit_code(code));
}

#[cfg(not(feature = "extensions"))]
fn run_extension(name: &str, _args: &[OsString]) -> anyhow::Result<()> {
    bail!(
        "unknown cue subcommand `{name}`; supported: {} (external extensions unavailable in this build)",
        supported_subcommands()
    )
}

#[cfg(any(feature = "extensions", feature = "script", test))]
fn process_exit_code(code: i32) -> i32 {
    if code < 0 { 1 } else { code }
}

fn print_help() {
    println!("{}", help_text());
}

fn help_text() -> String {
    let tui_help = if cfg!(feature = "tui") {
        "  tui        Start the terminal UI (default)"
    } else if cfg!(feature = "extensions") {
        "  tui        Start the terminal UI via the external cue-tui extension (default)"
    } else {
        "  tui        Unavailable in this build (enable the `tui` or `extensions` feature)"
    };
    let script_usage = if cfg!(feature = "script") {
        "\n       cue run <file.cue>"
    } else {
        ""
    };
    let script_help = if cfg!(feature = "script") {
        "\n  run        Run a .cue script file"
    } else {
        "\n  run        Unavailable in this build (enable the `script` feature)"
    };
    let extension_usage = if cfg!(feature = "extensions") {
        "\n       cue <extension> [args...]"
    } else {
        ""
    };
    let extension_help = if cfg!(feature = "extensions") {
        "\n  <extension> Run a configured external command, or cue-<extension> when enabled"
    } else {
        ""
    };

    format!(
        "cue {}\n\nUsage: cue [tui]{script_usage}{extension_usage}\n       cue --version\n\nCommands:\n{tui_help}{script_help}{extension_help}\n\nOptions:\n  -h, --help     Print help\n  -V, --version  Print version information",
        env!("CARGO_PKG_VERSION"),
    )
}

fn supported_subcommands() -> &'static str {
    match (
        cfg!(any(feature = "tui", feature = "extensions")),
        cfg!(feature = "script"),
    ) {
        (true, true) => "tui, run, help, version",
        (true, false) => "tui, help, version",
        (false, true) => "run, help, version",
        (false, false) => "help, version",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_command_defaults_to_tui() {
        assert_eq!(
            parse_command([OsString::from("cue")]).expect("parse command"),
            CueCommand::Tui
        );
    }

    #[test]
    fn parse_command_accepts_tui_subcommand() {
        assert_eq!(
            parse_command([OsString::from("cue"), OsString::from("tui")]).expect("parse command"),
            CueCommand::Tui
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

    #[test]
    fn parse_command_rejects_extra_tui_args() {
        let error = parse_command([
            OsString::from("cue"),
            OsString::from("tui"),
            OsString::from("extra"),
        ])
        .expect_err("extra tui args should fail");

        assert!(format!("{error:#}").contains("`cue tui` does not accept extra arguments"));
    }

    #[cfg(unix)]
    #[test]
    fn parse_command_rejects_non_utf8_subcommand() {
        use std::os::unix::ffi::OsStringExt;

        let error = parse_command([OsString::from("cue"), OsString::from_vec(vec![0xff])])
            .expect_err("non-UTF-8 subcommand should not fall back to default tui");

        assert!(format!("{error:#}").contains("cue subcommand must be valid UTF-8"));
    }

    #[test]
    fn parse_command_accepts_run_cue_file() {
        assert_eq!(
            parse_command([
                OsString::from("cue"),
                OsString::from("run"),
                OsString::from("build.cue")
            ])
            .expect("parse command"),
            CueCommand::Run {
                path: PathBuf::from("build.cue"),
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
    fn parse_command_treats_unknown_subcommand_as_extension() {
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
    fn process_exit_code_maps_internal_unavailable_sentinel() {
        assert_eq!(process_exit_code(-1), 1);
        assert_eq!(process_exit_code(0), 0);
        assert_eq!(process_exit_code(7), 7);
    }

    #[test]
    fn help_text_matches_enabled_features() {
        let text = help_text();
        if cfg!(feature = "extensions") {
            assert!(text.contains("cue <extension> [args...]"));
        } else {
            assert!(!text.contains("cue <extension> [args...]"));
        }
        if cfg!(feature = "tui") {
            assert!(text.contains("Start the terminal UI"));
        } else if cfg!(feature = "extensions") {
            assert!(text.contains("external cue-tui extension"));
        } else {
            assert!(text.contains("Unavailable in this build"));
        }
        if cfg!(feature = "script") {
            assert!(text.contains("cue run <file.cue>"));
            assert!(text.contains("Run a .cue script file"));
        } else {
            assert!(!text.contains("cue run <file.cue>"));
            assert!(text.contains("enable the `script` feature"));
        }
    }
}
