#[cfg(any(feature = "extensions", feature = "tui"))]
pub mod config;
#[cfg(feature = "tui")]
mod daemon_lifecycle;
#[cfg(any(feature = "extensions", feature = "tui"))]
pub mod path_lookup;
#[cfg(feature = "tui")]
mod script_runner;
#[cfg(feature = "tui")]
mod tui;
#[cfg(feature = "tui")]
pub mod version_check;

#[cfg(feature = "tui")]
pub fn run_tui() -> anyhow::Result<()> {
    tui::run()
}

#[cfg(feature = "tui")]
pub fn run_script(path: std::path::PathBuf) -> anyhow::Result<i32> {
    script_runner::run(path)
}
