//! TUI frontend for cue-shell.
//!
//! Architecture: TEA (The Elm Architecture) + Component hybrid.
//! - Global [`AppState`] + [`AppMsg`] enum + pure `update` function
//! - Panels rendered by independent [`Component`] implementors
//! - ratatui 0.30 + crossterm 0.29

pub mod ansi;
pub mod app;
pub mod client;
pub mod component;
pub mod event;
pub mod history;
mod target_config;
pub mod ui;

pub use app::{AppMsg, AppState, FocusArea, MouseMode};
pub use client::CuedClient;

use anyhow::{Context, Result};
use crossterm::event::{
    DisableBracketedPaste, DisableMouseCapture, EnableBracketedPaste, EnableMouseCapture,
    KeyboardEnhancementFlags, PopKeyboardEnhancementFlags, PushKeyboardEnhancementFlags,
};

/// Run the TUI application.
///
/// Accepts an optional pre-connected client (from `ensure_daemon_running`)
/// to avoid double-connecting. If `None`, starts in offline mode.
/// Auto-reconnects on disconnect using `connector`.
pub async fn run(
    client_connector: client::ClientConnector,
    client: Option<CuedClient>,
    session_profile_name: Option<String>,
) -> Result<()> {
    // Split client into reader/writer handle if connected.
    let (socket_reader, writer_handle, connected) = match client {
        Some(c) => {
            let (reader, writer) = c.into_split();
            (Some(reader), Some(client::spawn_writer_task(writer)), true)
        }
        None => (None, None, false),
    };

    // Initialize terminal.
    let mut terminal = ratatui::init();
    crossterm::execute!(std::io::stdout(), EnableBracketedPaste)
        .context("enable bracketed paste")?;
    let keyboard_enhancements_enabled = crossterm::execute!(
        std::io::stdout(),
        PushKeyboardEnhancementFlags(KeyboardEnhancementFlags::DISAMBIGUATE_ESCAPE_CODES)
    )
    .is_ok();
    let mut mouse_capture_enabled = false;

    // Install a panic hook that also restores terminal input modes.
    let original_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let _ = crossterm::execute!(
            std::io::stdout(),
            DisableMouseCapture,
            DisableBracketedPaste,
            PopKeyboardEnhancementFlags
        );
        original_hook(info);
    }));

    // Build app state.
    let mut state = AppState::new();
    state.set_session_profile_name(session_profile_name);
    if let Err(error) = history::load_history().map(|items| state.input.replace_history(items)) {
        tracing::warn!(%error, "failed to load prompt history");
    }
    let (w, h) = crossterm::terminal::size().unwrap_or((80, 24));
    state.terminal_width = w;
    state.terminal_height = h;
    let mut persisted_history = state.input.history.clone();

    if let Some(wh) = writer_handle {
        state.writer = Some(wh);
        if connected {
            state.update(AppMsg::Connected);
        }
    }

    if state.mouse_mode.capture_enabled() {
        crossterm::execute!(std::io::stdout(), EnableMouseCapture)
            .context("enable initial mouse capture")?;
        mouse_capture_enabled = true;
    }

    // Spawn event loop with the shared connector for auto-reconnect.
    let (mut rx, reconnect_tx) = event::spawn_event_loop(socket_reader, client_connector)?;
    state.set_reconnect_tx(reconnect_tx);

    // Main loop.
    let result = loop {
        if let Err(e) = terminal.draw(|frame| ui::draw(frame, &state)) {
            break Err(e).context("draw frame");
        }

        match rx.recv().await {
            Some(msg) => state.update(msg),
            None => break Ok(()),
        }

        if state.input.history != persisted_history {
            if let Err(error) = history::save_history(&state.input.history) {
                tracing::warn!(%error, "failed to save prompt history");
            } else {
                persisted_history = state.input.history.clone();
            }
        }

        let desired_mouse_capture = state.mouse_mode.capture_enabled();
        if desired_mouse_capture != mouse_capture_enabled {
            if desired_mouse_capture {
                crossterm::execute!(std::io::stdout(), EnableMouseCapture)
                    .context("enable mouse capture")?;
            } else {
                crossterm::execute!(std::io::stdout(), DisableMouseCapture)
                    .context("disable mouse capture")?;
            }
            mouse_capture_enabled = desired_mouse_capture;
        }

        if state.should_quit {
            break Ok(());
        }
    };

    // Restore terminal.
    crossterm::execute!(
        std::io::stdout(),
        DisableMouseCapture,
        DisableBracketedPaste
    )
    .context("restore terminal input modes")?;
    if keyboard_enhancements_enabled {
        crossterm::execute!(std::io::stdout(), PopKeyboardEnhancementFlags)
            .context("disable keyboard enhancements")?;
    }
    ratatui::restore();

    result
}
