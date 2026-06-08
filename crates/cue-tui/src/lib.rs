//! TUI frontend for cue-shell.
//!
//! Architecture: TEA (The Elm Architecture) + Component hybrid.
//! - Global app state plus message-driven update function
//! - Panels rendered by independent component implementors
//! - ratatui 0.30 + crossterm 0.29

mod ansi;
mod app;
mod client;
mod completion;
mod component;
mod event;
mod history;
mod target_config;
mod terminal;
mod ui;

use anyhow::{Context, Result};
use app::{AppMsg, AppState};
use crossterm::event::{
    DisableMouseCapture, EnableBracketedPaste, EnableMouseCapture, KeyboardEnhancementFlags,
    PushKeyboardEnhancementFlags,
};
use cue_client::{ClientConnector, CuedClient, RestartHandle};
use terminal::{PanicHookGuard, TerminalRestoreGuard, initial_terminal_size};

/// Inputs needed to start the TUI.
///
/// Keeping this as a named boundary avoids a long positional `run(...)`
/// signature and lets the CLI assemble transport details without making the
/// TUI crate re-export `cue-client` types as its own public API.
pub struct RunOptions {
    client_connector: ClientConnector,
    client: Option<CuedClient>,
    session_profile_name: Option<String>,
    restart_handle: Option<RestartHandle>,
}

impl RunOptions {
    pub fn new(client_connector: ClientConnector) -> Self {
        Self {
            client_connector,
            client: None,
            session_profile_name: None,
            restart_handle: None,
        }
    }

    pub fn with_client(mut self, client: CuedClient) -> Self {
        self.client = Some(client);
        self
    }

    pub fn with_optional_client(mut self, client: Option<CuedClient>) -> Self {
        self.client = client;
        self
    }

    pub fn with_session_profile_name(mut self, session_profile_name: Option<String>) -> Self {
        self.session_profile_name = session_profile_name;
        self
    }

    pub fn with_restart_handle(mut self, restart_handle: Option<RestartHandle>) -> Self {
        self.restart_handle = restart_handle;
        self
    }
}

/// Run the TUI application.
///
/// [`RunOptions`] accepts an optional pre-connected client (from
/// `ensure_daemon_running`) to avoid double-connecting. If `None`, the app
/// starts in offline mode and auto-reconnects using the provided connector.
pub async fn run(options: RunOptions) -> Result<()> {
    let RunOptions {
        client_connector,
        client,
        session_profile_name,
        restart_handle,
    } = options;

    // Split client into reader/writer handle if connected.
    let (socket_reader, writer_handle, connected) = match client {
        Some(c) => {
            let (reader, writer) = c.into_reader_and_writer_handle();
            (Some(reader), Some(writer), true)
        }
        None => (None, None, false),
    };

    // Initialize terminal.
    let mut terminal = ratatui::init();
    let mut terminal_restore = TerminalRestoreGuard::new();
    crossterm::execute!(std::io::stdout(), EnableBracketedPaste)
        .context("enable bracketed paste")?;
    let keyboard_enhancements_enabled = crossterm::execute!(
        std::io::stdout(),
        PushKeyboardEnhancementFlags(KeyboardEnhancementFlags::DISAMBIGUATE_ESCAPE_CODES)
    )
    .is_ok();
    terminal_restore.set_keyboard_enhancements_enabled(keyboard_enhancements_enabled);
    let mut mouse_capture_enabled = false;

    // Install a panic hook that also restores terminal input modes.
    let _panic_hook_guard = PanicHookGuard::install(keyboard_enhancements_enabled);

    // Build app state.
    let mut state = AppState::new();
    state.set_session_profile_name(session_profile_name);
    state.set_restart_handle(restart_handle);
    if let Err(error) = history::load_history().map(|items| state.input.replace_history(items)) {
        tracing::warn!(%error, "failed to load prompt history");
    }
    let (w, h) = initial_terminal_size(crossterm::terminal::size)?;
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
    let (mut rx, connection_controller) = event::spawn_event_loop(socket_reader, client_connector)?;
    state.set_connection_controller(connection_controller);

    // Main loop.
    let result = loop {
        if let Err(e) = terminal.draw(|frame| ui::draw(frame, &state)) {
            break Err(e).context("draw frame");
        }

        match rx.recv().await {
            Some(AppMsg::FatalError { message }) => {
                state.update(AppMsg::FatalError {
                    message: message.clone(),
                });
                break Err(anyhow::anyhow!(message));
            }
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

    terminal_restore.restore()?;

    result
}
