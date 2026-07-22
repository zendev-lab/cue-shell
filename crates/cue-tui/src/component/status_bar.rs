//! Header bar component — top line showing session state and actions.

use crossterm::event::{KeyEvent, MouseEvent};
use cue_core::Mode;
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;

use super::Component;
use crate::component::sidebar::OverviewCounts;
use crate::message::AppMsg;
use crate::mouse_mode::MouseMode;

// ── Component messages ──

/// Messages local to the status bar.
pub(crate) enum StatusBarMsg {
    /// Update connection state.
    Connected(bool),
    /// Update mouse interaction mode.
    MouseMode(MouseMode),
    /// Update the active input mode.
    Mode(Mode),
    /// Update whether clear display is currently safe.
    ClearEnabled(bool),
    /// Update current overview counts.
    Overview(OverviewCounts),
    /// Update the durable named session bound to this TUI.
    NamedSession(Option<String>),
}

// ── StatusBar ──

pub(crate) struct StatusBar {
    /// Whether we are connected to cued.
    pub(crate) connected: bool,
    /// Whether mouse is captured by the UI or left to terminal selection.
    pub(crate) mouse_mode: MouseMode,
    /// The currently active input mode.
    pub(crate) mode: Mode,
    /// Whether the clear-display action is currently enabled.
    pub(crate) clear_enabled: bool,
    /// Aggregate counts shown in the session header.
    pub(crate) overview: OverviewCounts,
    /// Durable named-session selector shown as the workspace identity.
    pub(crate) named_session: Option<String>,
}

impl StatusBar {
    pub(crate) fn new() -> Self {
        Self {
            connected: false,
            mouse_mode: MouseMode::UiCapture,
            mode: Mode::default(),
            clear_enabled: true,
            overview: OverviewCounts::default(),
            named_session: None,
        }
    }

    fn mode_label(&self) -> &'static str {
        match self.mode {
            Mode::Job => "JOB",
            Mode::Cron => "CRON",
        }
    }

    fn action_labels(&self) -> Vec<(&'static str, Style, AppMsg)> {
        vec![
            (
                "[clear]",
                if self.clear_enabled {
                    Style::default().fg(Color::Cyan)
                } else {
                    Style::default().fg(Color::DarkGray)
                },
                AppMsg::ClearDisplay,
            ),
            (
                "[sidebar ^B]",
                Style::default().fg(Color::Gray),
                AppMsg::ToggleSidebar,
            ),
            (
                "[copy ^Y]",
                Style::default().fg(Color::Gray),
                AppMsg::CopyFocus,
            ),
            (
                "[targets ^T]",
                Style::default().fg(Color::Gray),
                AppMsg::OpenTargetSettings,
            ),
            (
                "[kill ^C]",
                Style::default().fg(Color::Gray),
                AppMsg::OpenJobPicker,
            ),
            (
                "[mouse]",
                Style::default().fg(Color::Gray),
                AppMsg::ToggleMouseMode,
            ),
            ("[quit ^D]", Style::default().fg(Color::Gray), AppMsg::Quit),
        ]
    }

    fn action_text_width(&self) -> u16 {
        let labels = self.action_labels();
        let chars = labels
            .iter()
            .map(|(label, _, _)| label.chars().count())
            .sum::<usize>()
            + labels.len().saturating_sub(1);
        chars.min(u16::MAX as usize) as u16
    }

    fn rendered_action_width(&self, area_width: u16) -> u16 {
        let action_width = self.action_text_width();
        let Some(selector) = self.named_session.as_deref() else {
            return action_width.min(area_width);
        };

        // A named session is the identity of this workspace. At narrow widths,
        // keep that identity visible and drop the optional mouse action labels
        // as a group instead of letting them consume the whole header.
        let identity_width = 6usize
            .saturating_add(compact_session_badge(selector).chars().count())
            .min(u16::MAX as usize) as u16;
        if area_width.saturating_sub(identity_width) >= action_width {
            action_width
        } else {
            0
        }
    }

    pub(crate) fn action_at(&self, area: Rect, column: u16) -> Option<AppMsg> {
        let actions = self.action_labels();
        let width = self.rendered_action_width(area.width);
        if width == 0 {
            return None;
        }
        let start = area.x + area.width - width;
        if column < start || column >= area.x + area.width {
            return None;
        }

        let mut cursor = start;
        for (index, (label, _, msg)) in actions.into_iter().enumerate() {
            let label_width = label.chars().count() as u16;
            if column >= cursor && column < cursor + label_width {
                return Some(msg);
            }
            cursor += label_width;
            if index + 1 < self.action_labels().len() {
                cursor += 1;
            }
        }
        None
    }
}

impl Default for StatusBar {
    fn default() -> Self {
        Self::new()
    }
}

impl Component for StatusBar {
    type Message = StatusBarMsg;

    fn update(&mut self, msg: StatusBarMsg) {
        match msg {
            StatusBarMsg::Connected(c) => self.connected = c,
            StatusBarMsg::MouseMode(mode) => self.mouse_mode = mode,
            StatusBarMsg::Mode(mode) => self.mode = mode,
            StatusBarMsg::ClearEnabled(enabled) => self.clear_enabled = enabled,
            StatusBarMsg::Overview(overview) => self.overview = overview,
            StatusBarMsg::NamedSession(selector) => self.named_session = selector,
        }
    }

    fn render(&self, frame: &mut Frame, area: Rect) {
        let conn_status = if self.connected { "cued:ok" } else { "cued:--" };
        let conn_color = if self.connected {
            Color::Green
        } else {
            Color::Red
        };

        // Get current time.
        let now = {
            use std::time::{SystemTime, UNIX_EPOCH};
            let secs = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            // Simple HH:MM from unix timestamp (local time approximation via libc).
            // For a skeleton, UTC is acceptable.
            let hours = (secs / 3600) % 24;
            let minutes = (secs / 60) % 60;
            format!("{hours:02}:{minutes:02}")
        };

        let running = if self.overview.jobs_running > 0 {
            format!("({} running)", self.overview.jobs_running)
        } else {
            "(-)".to_string()
        };
        let counts = format!(
            "J:{} {}  C:{}",
            self.overview.jobs, running, self.overview.crons
        );
        let mut left_spans = vec![Span::styled(
            format!(" {} ", self.mode_label()),
            Style::default().fg(Color::Black).bg(Color::Cyan),
        )];
        if let Some(selector) = self.named_session.as_deref() {
            left_spans.push(Span::raw(" "));
            left_spans.push(Span::styled(
                compact_session_badge(selector),
                Style::default().fg(Color::Yellow),
            ));
        }
        left_spans.extend([
            Span::raw(" "),
            Span::styled(counts, Style::default().fg(Color::White)),
            Span::raw("  "),
            Span::styled(conn_status, Style::default().fg(conn_color)),
            Span::raw("  "),
            Span::styled(
                format!("mouse:{}", self.mouse_mode.label()),
                Style::default().fg(Color::DarkGray),
            ),
            Span::raw("  "),
            Span::styled(now, Style::default().fg(Color::DarkGray)),
        ]);
        let left = Line::from(left_spans);

        let action_width = self.rendered_action_width(area.width);
        let sections = Layout::horizontal([
            Constraint::Min(0),
            Constraint::Length(action_width.min(area.width)),
        ])
        .split(area);

        let mut action_spans = Vec::new();
        for (index, (label, style, _)) in self.action_labels().into_iter().enumerate() {
            action_spans.push(Span::styled(label, style));
            if index + 1 < self.action_labels().len() {
                action_spans.push(Span::raw(" "));
            }
        }

        let left_bar =
            Paragraph::new(left).style(Style::default().bg(Color::DarkGray).fg(Color::White));
        let right_bar = Paragraph::new(Line::from(action_spans))
            .style(Style::default().bg(Color::DarkGray).fg(Color::White));
        frame.render_widget(left_bar, sections[0]);
        frame.render_widget(right_bar, sections[1]);
    }

    fn handle_key(&mut self, _key: KeyEvent) -> Option<AppMsg> {
        // Status bar does not consume key events.
        None
    }

    fn handle_mouse(&mut self, _mouse: MouseEvent) -> Option<AppMsg> {
        None
    }
}

pub(crate) fn compact_session_badge(selector: &str) -> String {
    const MAX_SELECTOR_CHARS: usize = 20;

    let mut chars = selector.chars();
    let prefix: String = chars.by_ref().take(MAX_SELECTOR_CHARS).collect();
    if chars.next().is_some() {
        format!("session:{prefix}…")
    } else {
        format!("session:{prefix}")
    }
}

#[cfg(test)]
mod tests {
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    use super::*;

    fn rendered_header(status_bar: &StatusBar, width: u16) -> String {
        let backend = TestBackend::new(width, 1);
        let mut terminal = Terminal::new(backend).expect("terminal");
        terminal
            .draw(|frame| status_bar.render(frame, Rect::new(0, 0, width, 1)))
            .expect("draw status bar");
        terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect()
    }

    #[test]
    fn named_session_identity_remains_visible_in_a_narrow_header() {
        let mut status_bar = StatusBar::new();
        status_bar.update(StatusBarMsg::NamedSession(Some("shared".into())));

        let rendered = rendered_header(&status_bar, 32);

        assert!(rendered.contains("session:shared"), "{rendered:?}");
        assert!(
            !rendered.contains("[clear]"),
            "optional action labels should yield to the workspace identity: {rendered:?}"
        );
    }

    #[test]
    fn long_named_session_identity_is_compact() {
        assert_eq!(
            compact_session_badge("abcdefghijklmnopqrstuv"),
            "session:abcdefghijklmnopqrst…"
        );
    }
}
