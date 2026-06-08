//! Component trait and concrete panel implementations.

pub(crate) mod input_line;
pub(crate) mod main_view;
pub(crate) mod sidebar;
pub(crate) mod status_bar;

use crossterm::event::{KeyEvent, MouseEvent};
use ratatui::Frame;
use ratatui::layout::Rect;

use crate::app::AppMsg;

/// A self-contained UI panel that owns its local state.
///
/// Each component can:
/// - Accept its own `Message` type for internal updates
/// - Render itself into a given area
/// - Convert raw key/mouse events into app-level messages
pub(crate) trait Component {
    /// Component-local message type for internal state changes.
    type Message;

    /// Apply a component-local message to internal state.
    fn update(&mut self, msg: Self::Message);

    /// Render the component into the given terminal area.
    fn render(&self, frame: &mut Frame, area: Rect);

    /// Translate a key event into an optional app-level message.
    fn handle_key(&mut self, key: KeyEvent) -> Option<AppMsg>;

    /// Translate a mouse event into an optional app-level message.
    /// Default: ignore all mouse events.
    fn handle_mouse(&mut self, _mouse: MouseEvent) -> Option<AppMsg> {
        None
    }
}
