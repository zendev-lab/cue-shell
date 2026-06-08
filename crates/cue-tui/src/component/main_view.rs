//! Main view component — the lower command log pane.
//!
//! Displays a vertical stack of structured command records. Cards are colored
//! by status (green=ok, red=error, yellow=pending/streaming).

use crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyModifiers, MouseEvent, MouseEventKind};
use cue_core::Mode;
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Color, Style};
use ratatui::text::Line;
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};

use super::Component;
use crate::ansi;
use crate::app::AppMsg;

// ── Card types ──

/// Visual status of a card, determines border color.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CardStatus {
    Success,
    Error,
    Pending,
    Streaming,
}

impl CardStatus {
    fn border_color(self) -> Color {
        match self {
            Self::Success => Color::Green,
            Self::Error => Color::Red,
            Self::Pending => Color::Yellow,
            Self::Streaming => Color::Cyan,
        }
    }
}

/// A single REPL card: one command + its output.
#[derive(Debug, Clone)]
pub(crate) struct Card {
    /// Which mode this card belongs to.
    pub(crate) mode: Mode,
    /// What the user typed.
    pub(crate) input: String,
    /// Response / accumulated output.
    pub(crate) output: String,
    /// Current status.
    pub(crate) status: CardStatus,
    /// Optional short ID label (e.g. "J1").
    pub(crate) label: Option<String>,
    /// Optional chain step label (e.g. "chain:CH1/1/3").
    pub(crate) chain_label: Option<String>,
}

impl Card {
    pub(crate) fn new(input: String, mode: Mode) -> Self {
        Self {
            mode,
            input,
            output: String::new(),
            status: CardStatus::Pending,
            label: None,
            chain_label: None,
        }
    }

    /// Height in terminal rows this card would occupy (including border).
    fn height(&self) -> u16 {
        // 2 for top/bottom border + 1 for each output line (min 1)
        let output_lines = self.output.lines().count().max(1) as u16;
        2 + output_lines
    }
}

// ── Component messages ──

/// Messages local to the main view.
pub(crate) enum MainViewMsg {
    /// Switch the active mode so empty-state guidance can mention the prompt mode.
    SetMode(Mode),
    /// Update the latest card's output (streaming).
    AppendOutput { data: String },
    /// Set a specific card's chain step label.
    SetCardChainLabel { index: usize, label: String },
}

// ── MainView ──

pub(crate) struct MainView {
    /// Active input mode.
    pub(crate) mode: Mode,
    /// All cards, oldest first.
    pub(crate) cards: Vec<Card>,
    /// Scroll offset (0 = bottom, i.e. show newest).
    pub(crate) scroll_offset: usize,
}

impl MainView {
    pub(crate) fn new() -> Self {
        Self {
            mode: Mode::default(),
            cards: Vec::new(),
            scroll_offset: 0,
        }
    }

    pub(crate) fn push_card(&mut self, input: String, mode: Mode) -> usize {
        self.cards.push(Card::new(input, mode));
        self.scroll_offset = 0;
        self.cards.len() - 1
    }

    pub(crate) fn set_card_output(&mut self, index: usize, data: String) {
        if let Some(card) = self.cards.get_mut(index) {
            card.output = data;
        }
    }

    pub(crate) fn append_card_output(&mut self, index: usize, data: &str) {
        if let Some(card) = self.cards.get_mut(index) {
            card.output.push_str(data);
        }
    }

    pub(crate) fn set_card_status(&mut self, index: usize, status: CardStatus) {
        if let Some(card) = self.cards.get_mut(index) {
            card.status = status;
        }
    }

    pub(crate) fn set_card_label(&mut self, index: usize, label: String) {
        if let Some(card) = self.cards.get_mut(index) {
            card.label = Some(label);
        }
    }

    pub(crate) fn set_card_chain_label(&mut self, index: usize, label: String) {
        if let Some(card) = self.cards.get_mut(index) {
            card.chain_label = Some(label);
        }
    }

    pub(crate) fn clear_all(&mut self) {
        self.cards.clear();
        self.scroll_offset = 0;
    }

    pub(crate) fn card_at_point(&self, area: Rect, point: Rect) -> Option<usize> {
        if !crate::app::contains(area, point) {
            return None;
        }

        let visible_indices = self.visible_indices();
        if visible_indices.is_empty() {
            return None;
        }

        let mut y = area.bottom();
        let visible_len = visible_indices.len();
        let start = visible_len.saturating_sub(self.scroll_offset);

        for &idx in visible_indices[..start].iter().rev() {
            let card = &self.cards[idx];
            let h = card.height().min(y - area.y);
            if h == 0 {
                break;
            }
            y -= h;
            let card_area = Rect::new(area.x, y, area.width, h);
            if crate::app::contains(card_area, point) {
                return Some(idx);
            }
            if y <= area.y {
                break;
            }
        }

        None
    }

    fn visible_indices(&self) -> Vec<usize> {
        (0..self.cards.len()).collect()
    }

    fn latest_visible_card_mut(&mut self) -> Option<&mut Card> {
        self.cards.iter_mut().last()
    }
}

impl Default for MainView {
    fn default() -> Self {
        Self::new()
    }
}

impl Component for MainView {
    type Message = MainViewMsg;

    fn update(&mut self, msg: MainViewMsg) {
        match msg {
            MainViewMsg::SetMode(mode) => self.mode = mode,
            MainViewMsg::AppendOutput { data } => {
                if let Some(card) = self.latest_visible_card_mut() {
                    card.output.push_str(&data);
                    card.status = CardStatus::Streaming;
                }
            }
            MainViewMsg::SetCardChainLabel { index, label } => {
                self.set_card_chain_label(index, label);
            }
        }
    }

    fn render(&self, frame: &mut Frame, area: Rect) {
        let visible_indices = self.visible_indices();
        if visible_indices.is_empty() {
            let prompt_mode = match self.mode {
                Mode::Job => "JOB",
                Mode::Cron => "CRON",
            };
            let welcome = Paragraph::new(vec![
                Line::raw(""),
                Line::raw("  Command Log"),
                Line::raw("  Structured command records appear here after responses arrive."),
                Line::raw(
                    "  Use the upper display tabs for `:out`, `:tail`, `:err`, and previews.",
                ),
                Line::raw(""),
                Line::raw(format!("  Current mode: {prompt_mode}")),
                Line::raw("  Shift+Tab: switch job/cron mode   Ctrl+B: toggle sidebar"),
                Line::raw("  Tab: complete   Ctrl+C: kill/remove   Ctrl+D: quit   Ctrl+L: clear"),
            ])
            .style(Style::default().fg(Color::DarkGray));
            frame.render_widget(welcome, area);
            return;
        }

        // Render cards bottom-up, filling available height.
        let mut y = area.bottom();
        let visible_len = visible_indices.len();
        let start = visible_len.saturating_sub(self.scroll_offset);

        for &idx in visible_indices[..start].iter().rev() {
            let card = &self.cards[idx];
            let h = card.height().min(y - area.y);
            if h == 0 {
                break;
            }
            y -= h;
            let card_area = Rect::new(area.x, y, area.width, h);
            render_card(frame, card, card_area);
            if y <= area.y {
                break;
            }
        }
    }

    fn handle_key(&mut self, key: KeyEvent) -> Option<AppMsg> {
        if key.kind != KeyEventKind::Press {
            return None;
        }
        match key.code {
            KeyCode::PageUp => {
                self.scroll_offset =
                    (self.scroll_offset + 5).min(self.visible_indices().len().saturating_sub(1));
            }
            KeyCode::PageDown => {
                self.scroll_offset = self.scroll_offset.saturating_sub(5);
            }
            KeyCode::Char('b') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                return Some(AppMsg::ToggleSidebar);
            }
            _ => {}
        }
        None
    }

    fn handle_mouse(&mut self, mouse: MouseEvent) -> Option<AppMsg> {
        match mouse.kind {
            MouseEventKind::ScrollUp => {
                self.scroll_offset =
                    (self.scroll_offset + 3).min(self.visible_indices().len().saturating_sub(1));
            }
            MouseEventKind::ScrollDown => {
                self.scroll_offset = self.scroll_offset.saturating_sub(3);
            }
            _ => {}
        }
        None
    }
}

/// Format a chain step label string, e.g. `chain:CH1/1/3`.
pub(crate) fn chain_step_label(chain_id: &str, step_index: usize, total: usize) -> String {
    format!("chain:{}/{}/{}", chain_id, step_index + 1, total)
}

/// Render a single card into the given area.
fn render_card(frame: &mut Frame, card: &Card, area: Rect) {
    let border_color = card.status.border_color();
    let mode = match card.mode {
        Mode::Job => "JOB",
        Mode::Cron => "CRON",
    };

    let title = match (&card.label, &card.chain_label) {
        (Some(label), Some(chain_label)) => {
            format!(" {mode} > {} [{}] [{}] ", card.input, label, chain_label)
        }
        (Some(label), None) => format!(" {mode} > {} [{}] ", card.input, label),
        (None, Some(chain_label)) => format!(" {mode} > {} [{}] ", card.input, chain_label),
        (None, None) => format!(" {mode} > {} ", card.input),
    };

    let block = Block::new()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(border_color))
        .title(title);

    let output = if card.output.is_empty() {
        match card.status {
            CardStatus::Pending => "⏳ waiting...".to_string(),
            CardStatus::Streaming => "▌".to_string(),
            _ => String::new(),
        }
    } else {
        card.output.clone()
    };

    let paragraph = Paragraph::new(ansi::to_text(&output))
        .block(block)
        .wrap(Wrap { trim: false });

    frame.render_widget(paragraph, area);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn card_at_point_hits_latest_card() {
        let mut view = MainView::new();
        view.push_card("cargo test".into(), Mode::Job);
        let second = view.push_card("cargo clippy".into(), Mode::Job);
        view.set_card_output(second, "done".into());

        let hit = view.card_at_point(Rect::new(0, 0, 40, 8), Rect::new(2, 6, 1, 1));

        assert_eq!(hit, Some(second));
    }

    #[test]
    fn chain_step_label_format() {
        assert_eq!(chain_step_label("CH1", 0, 3), "chain:CH1/1/3");
        assert_eq!(chain_step_label("CH2", 2, 3), "chain:CH2/3/3");
        assert_eq!(chain_step_label("CH10", 0, 1), "chain:CH10/1/1");
    }

    #[test]
    fn card_chain_label_field() {
        let mut card = Card::new("sleep 1 -> echo done".into(), Mode::Job);
        assert!(card.chain_label.is_none());
        card.chain_label = Some(chain_step_label("CH1", 0, 2));
        assert_eq!(card.chain_label.as_deref(), Some("chain:CH1/1/2"));
    }

    #[test]
    fn set_card_chain_label_via_view_msg() {
        let mut view = MainView::new();
        let idx = view.push_card("cmd1 -> cmd2".into(), Mode::Job);
        view.update(MainViewMsg::SetCardChainLabel {
            index: idx,
            label: "chain:CH1/1/2".into(),
        });
        assert_eq!(
            view.cards[idx].chain_label.as_deref(),
            Some("chain:CH1/1/2")
        );
    }
}
