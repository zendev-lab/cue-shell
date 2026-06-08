//! Header bar component — top line showing session state and actions.

use crossterm::event::{KeyEvent, MouseEvent};
use cue_core::Mode;
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;

use super::Component;
use crate::app::{AppMsg, MouseMode};
use crate::component::sidebar::OverviewCounts;

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
}

impl StatusBar {
    pub(crate) fn new() -> Self {
        Self {
            connected: false,
            mouse_mode: MouseMode::UiCapture,
            mode: Mode::default(),
            clear_enabled: true,
            overview: OverviewCounts::default(),
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

    pub(crate) fn action_at(&self, area: Rect, column: u16) -> Option<AppMsg> {
        let actions = self.action_labels();
        let width = self.action_text_width();
        if width == 0 || area.width < width {
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
        let left = Line::from(vec![
            Span::styled(
                format!(" {} ", self.mode_label()),
                Style::default().fg(Color::Black).bg(Color::Cyan),
            ),
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

        let action_width = self.action_text_width();
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
