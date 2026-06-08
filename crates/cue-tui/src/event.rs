//! Event loop — merges terminal events, socket messages, and a tick timer
//! into a single stream of [`AppMsg`].
//!
//! The socket connection manager handles auto-reconnect: when the daemon
//! disconnects, it retries every 3 seconds and sends `Reconnected` with a
//! new [`crate::client::WriterHandle`] on success.

use std::fmt::Display;
use std::time::Duration;

use anyhow::Result;
use crossterm::event::{self, Event as CtEvent};
use tokio::sync::mpsc;

use crate::app::AppMsg;
use crate::client::{
    ClientConnector, ClientReader, ConnectionController, ConnectionEvent,
    spawn_connection_manager_controllable,
};
use cue_core::ipc::Message;

/// Spawn the event-producing tasks and return a receiver of [`AppMsg`] and a
/// controller for the connection manager.
///
/// Three sources feed the channel:
/// 1. **Terminal events** — crossterm key/mouse/resize (blocking thread)
/// 2. **Socket connection manager** — read + auto-reconnect (async task)
/// 3. **Tick timer** — periodic refresh for the status bar clock (async task)
pub(crate) fn spawn_event_loop(
    socket_reader: Option<ClientReader>,
    connector: ClientConnector,
) -> Result<(mpsc::UnboundedReceiver<AppMsg>, ConnectionController)> {
    let (tx, rx) = mpsc::unbounded_channel();

    // 1. Terminal events (blocking thread)
    let tx_term = tx.clone();
    std::thread::Builder::new()
        .name("tui-events".into())
        .spawn(move || {
            loop {
                match event::poll(Duration::from_millis(100)) {
                    Ok(true) => match event::read() {
                        Ok(ev) => {
                            let Some(msg) = terminal_event_msg(ev) else {
                                continue;
                            };
                            if tx_term.send(msg).is_err() {
                                break;
                            }
                        }
                        Err(error) => {
                            let _ = tx_term.send(terminal_event_error("read", error));
                            break;
                        }
                    },
                    Ok(false) => continue,
                    Err(error) => {
                        let _ = tx_term.send(terminal_event_error("poll", error));
                        break;
                    }
                }
            }
        })?;

    // 2. Socket connection manager (read + auto-reconnect + controllable)
    let tx_sock = tx.clone();
    let (mut socket_events, controller) =
        spawn_connection_manager_controllable(socket_reader, connector);
    tokio::spawn(async move {
        while let Some(event) = socket_events.recv().await {
            let msg = match event {
                ConnectionEvent::Incoming(msg) => match msg {
                    Message::Response { id, payload } => AppMsg::Response { id, payload },
                    Message::Event { payload } => AppMsg::ServerEvent(payload),
                    Message::Request { .. } => continue,
                },
                ConnectionEvent::Disconnected => AppMsg::Disconnected,
                ConnectionEvent::ReconnectFailed { message } => AppMsg::ReconnectFailed { message },
                ConnectionEvent::Reconnected { writer } => AppMsg::Reconnected { writer },
            };
            if tx_sock.send(msg).is_err() {
                break;
            }
        }
    });

    // 3. Tick timer
    let tx_tick = tx;
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            interval.tick().await;
            if tx_tick.send(AppMsg::Tick).is_err() {
                break;
            }
        }
    });

    Ok((rx, controller))
}

fn terminal_event_msg(event: CtEvent) -> Option<AppMsg> {
    match event {
        CtEvent::Key(key) => Some(AppMsg::KeyEvent(key)),
        CtEvent::Mouse(mouse) => Some(AppMsg::MouseEvent(mouse)),
        CtEvent::Paste(data) => Some(AppMsg::Paste(data)),
        CtEvent::Resize(w, h) => Some(AppMsg::Resize(w, h)),
        _ => None,
    }
}

fn terminal_event_error(action: &str, error: impl Display) -> AppMsg {
    AppMsg::FatalError {
        message: format!("terminal event {action} failed: {error}"),
    }
}

#[cfg(test)]
mod tests {
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

    use super::*;

    #[test]
    fn terminal_event_msg_maps_supported_events_and_ignores_focus() {
        assert!(matches!(
            terminal_event_msg(CtEvent::Key(KeyEvent::new(
                KeyCode::Char('x'),
                KeyModifiers::NONE,
            ))),
            Some(AppMsg::KeyEvent(_))
        ));
        assert!(matches!(
            terminal_event_msg(CtEvent::Resize(120, 40)),
            Some(AppMsg::Resize(120, 40))
        ));
        assert!(terminal_event_msg(CtEvent::FocusGained).is_none());
    }

    #[test]
    fn terminal_event_error_is_fatal_and_names_failed_action() {
        let msg = terminal_event_error(
            "poll",
            std::io::Error::new(std::io::ErrorKind::BrokenPipe, "tty closed"),
        );

        match msg {
            AppMsg::FatalError { message } => {
                assert!(message.contains("terminal event poll failed"), "{message}");
                assert!(message.contains("tty closed"), "{message}");
            }
            _ => panic!("expected fatal error"),
        }
    }
}
