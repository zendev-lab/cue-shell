//! Event loop — merges terminal events, socket messages, and a tick timer
//! into a single stream of [`AppMsg`].
//!
//! The socket connection manager handles auto-reconnect: when the daemon
//! disconnects, it retries every 3 seconds and sends `Reconnected` with a
//! new [`crate::client::WriterHandle`] on success.

use std::time::Duration;

use anyhow::Result;
use crossterm::event::{self, Event as CtEvent};
use tokio::sync::mpsc;

use crate::app::AppMsg;
use crate::client::{
    ClientConnector, ClientReader, ConnectionEvent, ReconnectCmd,
    spawn_connection_manager_controllable,
};
use cue_core::ipc::Message;

/// Spawn the event-producing tasks and return a receiver of [`AppMsg`] and a
/// sender for [`ReconnectCmd`]s that control the connection manager.
///
/// Three sources feed the channel:
/// 1. **Terminal events** — crossterm key/mouse/resize (blocking thread)
/// 2. **Socket connection manager** — read + auto-reconnect (async task)
/// 3. **Tick timer** — periodic refresh for the status bar clock (async task)
pub fn spawn_event_loop(
    socket_reader: Option<ClientReader>,
    connector: ClientConnector,
) -> Result<(mpsc::UnboundedReceiver<AppMsg>, mpsc::Sender<ReconnectCmd>)> {
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
                            let msg = match ev {
                                CtEvent::Key(key) => AppMsg::KeyEvent(key),
                                CtEvent::Mouse(mouse) => AppMsg::MouseEvent(mouse),
                                CtEvent::Paste(data) => AppMsg::Paste(data),
                                CtEvent::Resize(w, h) => AppMsg::Resize(w, h),
                                _ => continue,
                            };
                            if tx_term.send(msg).is_err() {
                                break;
                            }
                        }
                        Err(_) => break,
                    },
                    Ok(false) => continue,
                    Err(_) => break,
                }
            }
        })?;

    // 2. Socket connection manager (read + auto-reconnect + controllable)
    let tx_sock = tx.clone();
    let (mut socket_events, cmd_tx) =
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

    Ok((rx, cmd_tx))
}
