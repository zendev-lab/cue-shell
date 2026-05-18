#[cfg(feature = "tui")]
mod config;
#[cfg(feature = "tui")]
mod tui;

#[cfg(feature = "tui")]
pub fn run_tui() -> anyhow::Result<()> {
    tui::run()
}
