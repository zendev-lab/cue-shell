#[cfg(any(feature = "extensions", feature = "script", feature = "tui"))]
mod companion_binary;
#[cfg(any(feature = "extensions", feature = "script", feature = "tui"))]
mod config;
#[cfg(any(feature = "script", feature = "tui"))]
mod daemon_lifecycle;
#[cfg(feature = "extensions")]
mod extension_dispatch;
#[cfg(any(feature = "script", feature = "tui"))]
mod frontend_connection;
#[cfg(any(feature = "extensions", feature = "script", feature = "tui"))]
mod path_lookup;
#[cfg(feature = "script")]
mod script_runner;
#[cfg(any(feature = "script", feature = "tui"))]
mod tracing_config;
#[cfg(feature = "tui")]
mod tui;
#[cfg(any(feature = "script", feature = "tui"))]
mod version_check;

#[cfg(feature = "tui")]
pub fn run_tui() -> anyhow::Result<()> {
    tui::run()
}

#[cfg(feature = "script")]
pub fn run_script(path: std::path::PathBuf) -> anyhow::Result<i32> {
    script_runner::run(path)
}

#[cfg(feature = "extensions")]
pub fn run_extension(
    name: &str,
    args: &[std::ffi::OsString],
    supported_subcommands: &str,
) -> anyhow::Result<i32> {
    extension_dispatch::run(name, args, supported_subcommands)
}
