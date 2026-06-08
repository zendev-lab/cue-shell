//! Sidebar component — mode-specific entity list.

use std::cell::Cell;

use crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use cue_core::Mode;
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, List, ListItem, ListState, Paragraph};

use super::Component;
use crate::app::AppMsg;

// ── Types ──

/// A single item in the sidebar list.
#[derive(Debug, Clone)]
pub(crate) struct SidebarItem {
    /// Short identifier, e.g. "J1", "C2".
    pub(crate) id: String,
    /// Human-readable label (command text).
    pub(crate) label: String,
    /// Status icon.
    pub(crate) status_icon: &'static str,
}

/// Aggregate counts mirrored into the top header.
#[derive(Debug, Clone, Default)]
pub(crate) struct OverviewCounts {
    pub(crate) jobs: u32,
    pub(crate) jobs_running: u32,
    pub(crate) crons: u32,
}

// ── Component messages ──

/// Messages local to the sidebar.
pub(crate) enum SidebarMsg {
    /// Switch the active mode so titles and empty state match.
    Mode(Mode),
    /// Reflect whether the sidebar currently owns focus.
    Focused(bool),
    /// Replace the item list entirely.
    Items(Vec<SidebarItem>),
    /// Update overview counts.
    Overview(OverviewCounts),
}

// ── Sidebar ──

pub(crate) struct Sidebar {
    /// Active input mode.
    pub(crate) mode: Mode,
    /// Whether the sidebar currently owns focus.
    pub(crate) focused: bool,
    /// Current list items.
    pub(crate) items: Vec<SidebarItem>,
    /// Currently selected index (for highlight).
    pub(crate) selected: Option<usize>,
    /// Aggregate counts mirrored into the top header.
    pub(crate) overview: OverviewCounts,
    /// Scroll offset for the first visible row.
    list_offset: Cell<usize>,
}

impl Sidebar {
    pub(crate) fn new() -> Self {
        Self {
            mode: Mode::default(),
            focused: false,
            items: Vec::new(),
            selected: None,
            overview: OverviewCounts::default(),
            list_offset: Cell::new(0),
        }
    }

    fn list_title(&self) -> &'static str {
        match self.mode {
            Mode::Job => " Jobs ",
            Mode::Cron => " Crons ",
        }
    }

    fn empty_hint(&self) -> &'static str {
        match self.mode {
            Mode::Job => "No jobs yet.",
            Mode::Cron => "No crons yet.",
        }
    }

    pub(crate) fn move_selection(&mut self, delta: isize) {
        if self.items.is_empty() {
            self.selected = None;
            self.list_offset.set(0);
            return;
        }

        let current = self.selected.unwrap_or(0) as isize;
        let next = (current + delta).clamp(0, self.items.len().saturating_sub(1) as isize);
        self.selected = Some(next as usize);
    }

    pub(crate) fn select_visible_row(&mut self, row: usize) -> Option<usize> {
        let index = self.visible_row_index(row)?;
        self.selected = Some(index);
        Some(index)
    }

    pub(crate) fn visible_row_index(&self, row: usize) -> Option<usize> {
        let index = self.list_offset.get().saturating_add(row);
        (index < self.items.len()).then_some(index)
    }
}

impl Default for Sidebar {
    fn default() -> Self {
        Self::new()
    }
}

impl Component for Sidebar {
    type Message = SidebarMsg;

    fn update(&mut self, msg: SidebarMsg) {
        match msg {
            SidebarMsg::Mode(mode) => self.mode = mode,
            SidebarMsg::Focused(focused) => self.focused = focused,
            SidebarMsg::Items(items) => {
                self.items = items;
                // Keep selection in bounds.
                if self.selected.is_some_and(|sel| sel >= self.items.len()) {
                    self.selected = self.items.len().checked_sub(1);
                }
                if self.items.is_empty() {
                    self.list_offset.set(0);
                }
            }
            SidebarMsg::Overview(counts) => {
                self.overview = counts;
            }
        }
    }

    fn render(&self, frame: &mut Frame, area: Rect) {
        let border_style = if self.focused {
            Style::default().fg(Color::Cyan)
        } else {
            Style::default().fg(Color::DarkGray)
        };
        // ── Item list ──
        let list_block = Block::new()
            .borders(Borders::ALL)
            .border_style(border_style)
            .title(self.list_title());
        if self.items.is_empty() {
            self.list_offset.set(0);
            let empty = Paragraph::new(self.empty_hint())
                .block(list_block)
                .style(Style::default().fg(Color::DarkGray));
            frame.render_widget(empty, area);
        } else {
            let list_inner = list_block.inner(area);
            frame.render_widget(list_block, area);
            let items: Vec<ListItem> = self
                .items
                .iter()
                .enumerate()
                .map(|(i, item)| {
                    let style = if self.selected == Some(i) {
                        Style::default()
                            .fg(Color::Black)
                            .bg(Color::White)
                            .add_modifier(Modifier::BOLD)
                    } else {
                        Style::default()
                    };
                    ListItem::new(Line::from(vec![
                        Span::styled(
                            format!("{} ", item.status_icon),
                            Style::default().fg(Color::Yellow),
                        ),
                        Span::styled(&item.id, Style::default().fg(Color::Cyan)),
                        Span::raw(" "),
                        Span::raw(&item.label),
                    ]))
                    .style(style)
                })
                .collect();

            let list = List::new(items);
            let mut list_state = ListState::default()
                .with_offset(self.list_offset.get())
                .with_selected(self.selected);
            frame.render_stateful_widget(list, list_inner, &mut list_state);
            self.list_offset.set(list_state.offset());
        }
    }

    fn handle_key(&mut self, key: KeyEvent) -> Option<AppMsg> {
        if key.kind != KeyEventKind::Press {
            return None;
        }
        match (key.modifiers, key.code) {
            (_, KeyCode::Up) => {
                if let Some(sel) = self.selected.as_mut() {
                    *sel = sel.saturating_sub(1);
                } else if !self.items.is_empty() {
                    self.selected = Some(self.items.len() - 1);
                }
            }
            (_, KeyCode::Down) => {
                if let Some(sel) = self.selected.as_mut() {
                    *sel = (*sel + 1).min(self.items.len().saturating_sub(1));
                } else if !self.items.is_empty() {
                    self.selected = Some(0);
                }
            }
            (KeyModifiers::CONTROL, KeyCode::Char('c')) => {
                return Some(AppMsg::OpenJobPicker);
            }
            (_, KeyCode::Backspace | KeyCode::Delete) => {
                return Some(AppMsg::KillSelection);
            }
            (KeyModifiers::CONTROL, KeyCode::Char('b')) => return Some(AppMsg::ToggleSidebar),
            _ => {}
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    use super::*;
    use crate::component::Component;

    fn sidebar_with_items(count: usize) -> Sidebar {
        let mut sidebar = Sidebar::new();
        sidebar.update(SidebarMsg::Items(
            (0..count)
                .map(|index| SidebarItem {
                    id: format!("J{}", index + 1),
                    label: format!("item {index}"),
                    status_icon: "•",
                })
                .collect(),
        ));
        sidebar
    }

    #[test]
    fn render_tracks_scroll_offset_for_selected_item() {
        let backend = TestBackend::new(24, 8);
        let mut terminal = Terminal::new(backend).expect("terminal");
        let mut sidebar = sidebar_with_items(10);
        sidebar.selected = Some(7);

        terminal
            .draw(|frame| sidebar.render(frame, Rect::new(0, 0, 24, 8)))
            .expect("draw sidebar");

        assert_eq!(sidebar.visible_row_index(0), Some(2));
        assert_eq!(sidebar.visible_row_index(5), Some(7));
    }

    #[test]
    fn selecting_visible_row_uses_current_scroll_offset() {
        let backend = TestBackend::new(24, 8);
        let mut terminal = Terminal::new(backend).expect("terminal");
        let mut sidebar = sidebar_with_items(10);
        sidebar.selected = Some(7);

        terminal
            .draw(|frame| sidebar.render(frame, Rect::new(0, 0, 24, 8)))
            .expect("draw sidebar");

        assert_eq!(sidebar.select_visible_row(1), Some(3));
        assert_eq!(sidebar.selected, Some(3));
    }

    #[test]
    fn render_keeps_items_within_inner_list_width() {
        let backend = TestBackend::new(24, 8);
        let mut terminal = Terminal::new(backend).expect("terminal");
        let sidebar = sidebar_with_items(10);

        terminal
            .draw(|frame| sidebar.render(frame, Rect::new(0, 0, 24, 8)))
            .expect("draw sidebar");

        let buffer = terminal.backend().buffer();
        assert!(
            buffer
                .content()
                .iter()
                .all(|cell| !matches!(cell.symbol(), "█" | "║"))
        );
    }
}
