use anyhow::bail;
use std::ffi::OsString;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CueCommand {
    Help,
    Tui,
    Version,
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
    }
}

fn parse_command(args: impl IntoIterator<Item = OsString>) -> anyhow::Result<CueCommand> {
    let mut args = args.into_iter();
    let _program = args.next();

    match args.next().as_deref().and_then(|arg| arg.to_str()) {
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
        Some(other) => bail!("unknown cue subcommand `{other}`; supported: tui"),
    }
}

#[cfg(feature = "tui")]
fn run_tui() -> anyhow::Result<()> {
    cue_cli::run_tui()
}

#[cfg(not(feature = "tui"))]
fn run_tui() -> anyhow::Result<()> {
    bail!("`cue tui` is unavailable because cue-cli was built without the `tui` feature")
}

fn print_help() {
    let tui_help = if cfg!(feature = "tui") {
        "  tui        Start the terminal UI (default)"
    } else {
        "  tui        Unavailable in this build (enable the `tui` feature)"
    };

    println!(
        "cue {}\n\nUsage: cue [tui]\n       cue --version\n\nCommands:\n{tui_help}\n\nOptions:\n  -h, --help     Print help\n  -V, --version  Print version information",
        env!("CARGO_PKG_VERSION")
    );
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

    #[test]
    fn parse_command_rejects_unknown_subcommand() {
        let error = parse_command([OsString::from("cue"), OsString::from("bogus")])
            .expect_err("unknown command should fail");
        assert!(format!("{error:#}").contains("unknown cue subcommand `bogus`"));
    }
}
