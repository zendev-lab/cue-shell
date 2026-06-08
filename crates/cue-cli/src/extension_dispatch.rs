use std::ffi::{OsStr, OsString};
use std::io;
use std::path::PathBuf;
use std::process::Command;

use anyhow::{Context, bail};
use cue_core::process_status::exit_code_from_status;

#[derive(Debug, Clone, PartialEq, Eq)]
enum ResolvedExtensionCommand {
    ConfiguredProgram(String),
    Path(PathBuf),
}

pub(crate) fn run(
    name: &str,
    args: &[OsString],
    supported_subcommands: &str,
) -> anyhow::Result<i32> {
    run_with(
        name,
        args,
        supported_subcommands,
        crate::config::Config::load_for_extension_dispatch,
        first_party_extension_binary,
        crate::path_lookup::find_executable_on_path,
        exec_resolved_extension_command,
    )
}

fn run_with<L, F, G, E>(
    name: &str,
    args: &[OsString],
    supported_subcommands: &str,
    mut load_config: L,
    mut find_first_party_binary: F,
    mut find_path_binary: G,
    mut exec_command: E,
) -> anyhow::Result<i32>
where
    L: FnMut() -> anyhow::Result<crate::config::Config>,
    F: FnMut(&str) -> anyhow::Result<Option<PathBuf>>,
    G: FnMut(&str) -> Option<PathBuf>,
    E: FnMut(ResolvedExtensionCommand, &[OsString]) -> anyhow::Result<i32>,
{
    if let Some(command) =
        resolve_first_party_extension_command(name, &mut find_first_party_binary)?
    {
        return exec_command(command, args);
    }

    if is_first_party_extension(name) {
        bail!(
            "`cue {name}` is available as a first-party external extension, but `cue-{name}` was not found next to `cue`; supported: {supported_subcommands}",
        );
    }
    crate::config::validate_extension_name(name, "extension subcommand")?;

    let config = load_config()?;
    if let Some(command) = resolve_user_extension_command(&config, name, &mut find_path_binary) {
        return exec_command(command, args);
    }

    bail!("unknown cue subcommand `{name}`; supported: {supported_subcommands}")
}

fn resolve_first_party_extension_command<F>(
    name: &str,
    mut find_first_party_binary: F,
) -> anyhow::Result<Option<ResolvedExtensionCommand>>
where
    F: FnMut(&str) -> anyhow::Result<Option<PathBuf>>,
{
    if let Some(program) = first_party_extension_program(name)
        && let Some(path) = find_first_party_binary(program)?
    {
        return Ok(Some(ResolvedExtensionCommand::Path(path)));
    }

    Ok(None)
}

fn resolve_user_extension_command<G>(
    config: &crate::config::Config,
    name: &str,
    mut find_path_binary: G,
) -> Option<ResolvedExtensionCommand>
where
    G: FnMut(&str) -> Option<PathBuf>,
{
    if let Some(extension) = config.extensions.commands.get(name) {
        return Some(ResolvedExtensionCommand::ConfiguredProgram(
            extension.program.clone(),
        ));
    }

    if config.extensions.path_lookup {
        return find_path_binary(&format!("cue-{name}")).map(ResolvedExtensionCommand::Path);
    }

    None
}

fn first_party_extension_binary(program: &str) -> anyhow::Result<Option<PathBuf>> {
    first_party_extension_binary_from_runtime_sources(
        program,
        std::env::current_exe(),
        crate::companion_binary::argv0_path(),
    )
}

fn first_party_extension_binary_from_runtime_sources(
    program: &str,
    current_exe: io::Result<PathBuf>,
    argv0_path: anyhow::Result<Option<PathBuf>>,
) -> anyhow::Result<Option<PathBuf>> {
    let current_exe =
        current_exe.context("resolve current executable path for first-party extension lookup")?;
    if let Some(path) = crate::companion_binary::companion_binary_for_path(&current_exe, program) {
        return Ok(Some(path));
    }

    Ok(argv0_path?
        .as_deref()
        .and_then(|path| crate::companion_binary::companion_binary_for_path(path, program)))
}

#[cfg(test)]
fn first_party_extension_binary_from_sources(
    program: &str,
    current_exe: Option<PathBuf>,
    argv0: Option<PathBuf>,
) -> Option<PathBuf> {
    crate::companion_binary::companion_binary_from_sources(program, current_exe, argv0)
}

fn is_first_party_extension(name: &str) -> bool {
    first_party_extension_program(name).is_some()
}

fn first_party_extension_program(name: &str) -> Option<&'static str> {
    match name {
        "tui" => Some("cue-tui"),
        _ => None,
    }
}

fn exec_resolved_extension_command(
    command: ResolvedExtensionCommand,
    args: &[OsString],
) -> anyhow::Result<i32> {
    match command {
        ResolvedExtensionCommand::ConfiguredProgram(program) => {
            exec_configured_extension_program(&program, args)
        }
        ResolvedExtensionCommand::Path(program) => exec_program(program.as_os_str(), args),
    }
}

fn exec_configured_extension_program(program: &str, args: &[OsString]) -> anyhow::Result<i32> {
    if program.trim().is_empty() {
        bail!("extension program is empty");
    }
    if program.trim() != program {
        bail!("extension program must not have leading or trailing whitespace");
    }
    exec_program(OsStr::new(program), args)
}

fn exec_program(program: &OsStr, args: &[OsString]) -> anyhow::Result<i32> {
    let status = Command::new(program)
        .args(args)
        .status()
        .with_context(|| format!("failed to run extension `{}`", program.to_string_lossy()))?;
    Ok(exit_code_from_status(status, 1))
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::path::Path;

    #[test]
    fn first_party_tui_extension_does_not_resolve_without_companion_binary() {
        let command = resolve_first_party_extension_command("tui", |name| {
            assert_eq!(name, "cue-tui");
            Ok(None)
        })
        .expect("first-party resolver should not fail");

        assert_eq!(command, None);
    }

    #[test]
    fn run_first_party_extension_skips_user_registry_config() {
        let args = [OsString::from("--smoke")];

        let code = run_with(
            "tui",
            &args,
            "tui, run",
            || panic!("first-party dispatch should not load user extension config"),
            |name| {
                assert_eq!(name, "cue-tui");
                Ok(Some(PathBuf::from("/install/bin/cue-tui")))
            },
            |_| panic!("sibling first-party extension should not consult user PATH lookup"),
            |command, forwarded_args| {
                assert_eq!(
                    command,
                    ResolvedExtensionCommand::Path(PathBuf::from("/install/bin/cue-tui"))
                );
                assert_eq!(forwarded_args, &args);
                Ok(7)
            },
        )
        .expect("first-party extension should dispatch");

        assert_eq!(code, 7);
    }

    #[test]
    fn run_missing_first_party_extension_does_not_load_user_registry_config() {
        let error = run_with(
            "tui",
            &[],
            "tui, run",
            || panic!("missing first-party extension should not load user extension config"),
            |_| Ok(None),
            |_| panic!("missing first-party extension should not fall back to PATH"),
            |_, _| panic!("missing first-party extension should not execute"),
        )
        .expect_err("missing first-party extension should report installation problem");

        assert!(format!("{error:#}").contains("first-party external extension"));
    }

    #[test]
    fn run_invalid_extension_name_does_not_load_config_or_probe_path() {
        for name in ["foo_bar", "foo/bar", "-foo", "foo--bar"] {
            let error = run_with(
                name,
                &[],
                "tui, run",
                || panic!("invalid extension names should not load user extension config"),
                |_| panic!("invalid extension names should not probe first-party binaries"),
                |_| panic!("invalid extension names should not fall back to PATH"),
                |_, _| panic!("invalid extension names should not execute"),
            )
            .expect_err("invalid extension subcommand should fail at the dispatch boundary");

            assert_eq!(
                format!("{error:#}"),
                format!(
                    "extension subcommand `{name}` must be kebab-case ASCII, for example `foo` or `foo-bar`"
                )
            );
        }
    }

    #[test]
    fn run_first_party_extension_reports_lookup_error_without_user_registry_fallback() {
        let error = run_with(
            "tui",
            &[],
            "tui, run",
            || panic!("first-party lookup errors should not load user extension config"),
            |_| Err(anyhow::anyhow!("current directory was removed")),
            |_| panic!("first-party lookup errors should not fall back to PATH"),
            |_, _| panic!("failed first-party lookup should not execute"),
        )
        .expect_err("first-party lookup error should be reported");

        assert_eq!(format!("{error:#}"), "current directory was removed");
    }

    #[test]
    fn first_party_sibling_resolves_as_first_party_command() {
        let command = resolve_first_party_extension_command("tui", |name| {
            assert_eq!(name, "cue-tui");
            Ok(Some(PathBuf::from("/install/bin/cue-tui")))
        })
        .expect("first-party resolver should not fail");

        assert_eq!(
            command,
            Some(ResolvedExtensionCommand::Path(PathBuf::from(
                "/install/bin/cue-tui"
            )))
        );
    }

    #[test]
    fn non_first_party_extension_respects_global_path_lookup_flag() {
        let config = crate::config::Config::default();

        let command = resolve_user_extension_command(&config, "foo", |_| {
            panic!("PATH lookup should be disabled for ordinary extensions")
        });

        assert_eq!(command, None);
    }

    #[test]
    fn non_first_party_extension_uses_path_lookup_when_enabled() {
        let mut config = crate::config::Config::default();
        config.extensions.path_lookup = true;

        let command = resolve_user_extension_command(&config, "foo", |name| {
            assert_eq!(name, "cue-foo");
            Some(PathBuf::from("/tools/cue-foo"))
        });

        assert_eq!(
            command,
            Some(ResolvedExtensionCommand::Path(PathBuf::from(
                "/tools/cue-foo"
            )))
        );
    }

    #[test]
    fn first_party_binary_uses_current_exe_sibling() {
        let dir = make_temp_bin_dir("sibling");
        let cue = dir.join("cue");
        let tui = dir.join("cue-tui");
        touch(&cue);
        write_executable(&tui);

        assert_eq!(
            first_party_extension_binary_from_sources("cue-tui", Some(cue), None),
            Some(tui)
        );

        std::fs::remove_dir_all(dir).expect("remove temp bin dir");
    }

    #[test]
    fn first_party_binary_uses_cargo_deps_sibling() {
        let dir = make_temp_bin_dir("cargo-deps");
        let deps = dir.join("deps");
        std::fs::create_dir_all(&deps).expect("create deps dir");
        let cue = deps.join("cue-123");
        let tui = dir.join("cue-tui");
        touch(&cue);
        write_executable(&tui);

        assert_eq!(
            first_party_extension_binary_from_sources("cue-tui", Some(cue), None),
            Some(tui)
        );

        std::fs::remove_dir_all(dir).expect("remove temp bin dir");
    }

    #[test]
    fn first_party_binary_falls_back_to_argv0_when_current_exe_has_no_companion() {
        let current_dir = make_temp_bin_dir("current-no-companion");
        let argv0_dir = make_temp_bin_dir("argv0-companion");
        let current_cue = current_dir.join("cue");
        let argv0_cue = argv0_dir.join("cue");
        let argv0_tui = argv0_dir.join("cue-tui");
        touch(&current_cue);
        touch(&argv0_cue);
        write_executable(&argv0_tui);

        let resolved = first_party_extension_binary_from_runtime_sources(
            "cue-tui",
            Ok(current_cue),
            Ok(Some(argv0_cue)),
        )
        .expect("argv0 lookup should succeed");

        assert_eq!(resolved, Some(argv0_tui));
        std::fs::remove_dir_all(current_dir).expect("remove current temp bin dir");
        std::fs::remove_dir_all(argv0_dir).expect("remove argv0 temp bin dir");
    }

    #[test]
    fn first_party_binary_reports_current_exe_failure() {
        let error = first_party_extension_binary_from_runtime_sources(
            "cue-tui",
            Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "current executable disappeared",
            )),
            Ok(Some(PathBuf::from("/ignored/cue"))),
        )
        .expect_err("current_exe errors should not be treated as missing companions");

        let message = format!("{error:#}");
        assert!(message.contains("resolve current executable path"));
        assert!(message.contains("current executable disappeared"));
    }

    #[test]
    fn first_party_extension_set_is_explicit() {
        assert!(is_first_party_extension("tui"));
        assert!(!is_first_party_extension("foo"));
    }

    #[test]
    fn configured_extension_program_rejects_empty_program() {
        let error = exec_configured_extension_program("   ", &[])
            .expect_err("empty configured extension program should fail before spawn");

        assert_eq!(format!("{error:#}"), "extension program is empty");
    }

    #[test]
    fn configured_extension_program_rejects_padded_program_without_trimming() {
        let error = exec_configured_extension_program(" sh", &[])
            .expect_err("padded configured extension program should fail before spawn");

        assert_eq!(
            format!("{error:#}"),
            "extension program must not have leading or trailing whitespace"
        );
    }

    #[cfg(unix)]
    #[test]
    fn exec_program_returns_child_exit_code_without_exiting() {
        let code = exec_program(
            OsStr::new("sh"),
            &[OsString::from("-c"), OsString::from("exit 7")],
        )
        .expect("run child extension");

        assert_eq!(code, 7);
    }

    #[cfg(unix)]
    #[test]
    fn exec_program_maps_signal_status_to_shell_exit_code() {
        let code = exec_program(
            OsStr::new("sh"),
            &[OsString::from("-c"), OsString::from("kill -TERM $$")],
        )
        .expect("run child extension");

        assert_eq!(code, 128 + libc::SIGTERM);
    }

    fn make_temp_bin_dir(name: &str) -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};

        static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

        let dir = std::env::temp_dir().join(format!(
            "cue-extension-bin-test-{name}-{}-{}",
            std::process::id(),
            TEMP_COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).expect("create temp bin dir");
        dir
    }

    fn touch(path: &Path) {
        std::fs::write(path, []).expect("create temp file");
    }

    #[cfg(unix)]
    fn write_executable(path: &Path) {
        use std::os::unix::fs::PermissionsExt;

        std::fs::write(path, "#!/bin/sh\n").expect("write executable");
        let mut permissions = std::fs::metadata(path)
            .expect("stat executable")
            .permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(path, permissions).expect("chmod executable");
    }

    #[cfg(not(unix))]
    fn write_executable(path: &Path) {
        std::fs::write(path, "").expect("write executable");
    }
}
