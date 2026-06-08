//! Input line component — the command prompt at the bottom of the TUI.
//!
//! Renders: `[JOB ⚡] > {content}` with a blinking cursor.
//! Keys: printable→insert, Backspace→delete, Left/Right→move cursor,
//!        Enter→submit, ↑↓→history, Shift+Tab→mode cycle.

use std::ops::Range;

use crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyModifiers, MouseEvent};
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Paragraph, Wrap};

use cue_core::Mode;

use super::Component;
use crate::app::AppMsg;

/// Messages local to the input line.
pub(crate) enum InputMsg {
    /// Set the active mode (reflects global mode changes).
    SetMode(Mode),
    /// Clear content after submission.
    Clear,
}

pub(crate) struct InputLine {
    /// Current text content.
    pub(crate) content: String,
    /// Byte-offset cursor position within `content`.
    pub(crate) cursor: usize,
    /// Command history (newest last).
    pub(crate) history: Vec<String>,
    /// Current position in history browsing (`None` = not browsing).
    history_idx: Option<usize>,
    /// Stashed content when browsing history.
    stashed: String,
    /// Active input mode.
    pub(crate) mode: Mode,
}

impl InputLine {
    const MAX_VISIBLE_LINES: u16 = 6;

    pub(crate) fn new() -> Self {
        Self {
            content: String::new(),
            cursor: 0,
            history: Vec::new(),
            history_idx: None,
            stashed: String::new(),
            mode: Mode::default(),
        }
    }

    /// Take the current content (for submission) and push to history.
    pub(crate) fn take_input(&mut self) -> String {
        let input = std::mem::take(&mut self.content);
        self.cursor = 0;
        self.history_idx = None;
        if !input.trim().is_empty() {
            self.history.push(input.clone());
        }
        input
    }

    fn history_up(&mut self) {
        if self.history.is_empty() {
            return;
        }
        let idx = match self.history_idx {
            None => {
                self.stashed = self.content.clone();
                self.history.len() - 1
            }
            Some(0) => return,
            Some(i) => i - 1,
        };
        self.history_idx = Some(idx);
        self.content.clone_from(&self.history[idx]);
        self.cursor = self.content.len();
    }

    fn history_down(&mut self) {
        let Some(idx) = self.history_idx else {
            return;
        };
        if idx + 1 >= self.history.len() {
            // Restore stashed content.
            self.history_idx = None;
            self.content.clone_from(&self.stashed);
        } else {
            self.history_idx = Some(idx + 1);
            self.content.clone_from(&self.history[idx + 1]);
        }
        self.cursor = self.content.len();
    }

    pub(crate) fn desired_height(&self) -> u16 {
        self.content
            .split('\n')
            .count()
            .max(1)
            .min(Self::MAX_VISIBLE_LINES as usize) as u16
    }

    pub(crate) fn insert_text(&mut self, text: &str) {
        self.content.insert_str(self.cursor, text);
        self.cursor += text.len();
        self.history_idx = None;
    }

    pub(crate) fn replace_history(&mut self, history: Vec<String>) {
        self.history = history;
        self.history_idx = None;
        self.stashed.clear();
    }

    pub(crate) fn current_word_range(&self) -> Range<usize> {
        let cursor = self.cursor.min(self.content.len());
        let start = self.content[..cursor]
            .rfind(char::is_whitespace)
            .map_or(0, |idx| idx + 1);
        let end = cursor
            + self.content[cursor..]
                .find(char::is_whitespace)
                .unwrap_or(self.content[cursor..].len());
        start..end
    }

    pub(crate) fn replace_range(&mut self, range: Range<usize>, text: &str) {
        self.content.replace_range(range.clone(), text);
        self.cursor = range.start + text.len();
        self.history_idx = None;
    }

    fn move_left(&mut self) {
        if self.cursor > 0 {
            let prev = self.content[..self.cursor]
                .chars()
                .next_back()
                .map_or(0, char::len_utf8);
            self.cursor -= prev;
        }
    }

    fn move_right(&mut self) {
        if self.cursor < self.content.len() {
            let next = self.content[self.cursor..]
                .chars()
                .next()
                .map_or(0, char::len_utf8);
            self.cursor += next;
        }
    }

    fn move_to_line_start(&mut self) {
        self.cursor = self.content[..self.cursor]
            .rfind('\n')
            .map_or(0, |idx| idx + 1);
    }

    fn move_to_line_end(&mut self) {
        self.cursor += self.content[self.cursor..]
            .find('\n')
            .unwrap_or(self.content[self.cursor..].len());
    }

    fn current_line_bounds(&self) -> (usize, usize) {
        let start = self.content[..self.cursor]
            .rfind('\n')
            .map_or(0, |idx| idx + 1);
        let end = self.cursor
            + self.content[self.cursor..]
                .find('\n')
                .unwrap_or(self.content[self.cursor..].len());
        (start, end)
    }

    fn byte_offset_for_char_column(line: &str, column: usize) -> usize {
        if column == 0 {
            return 0;
        }
        line.char_indices()
            .nth(column)
            .map(|(idx, _)| idx)
            .unwrap_or(line.len())
    }

    fn move_up_line(&mut self) -> bool {
        let (start, _) = self.current_line_bounds();
        if start == 0 {
            return false;
        }
        let column = self.content[start..self.cursor].chars().count();
        let prev_end = start - 1;
        let prev_start = self.content[..prev_end]
            .rfind('\n')
            .map_or(0, |idx| idx + 1);
        let prev_line = &self.content[prev_start..prev_end];
        self.cursor = prev_start + Self::byte_offset_for_char_column(prev_line, column);
        true
    }

    fn move_down_line(&mut self) -> bool {
        let (start, end) = self.current_line_bounds();
        if end >= self.content.len() {
            return false;
        }
        let column = self.content[start..self.cursor].chars().count();
        let next_start = end + 1;
        let next_end = next_start
            + self.content[next_start..]
                .find('\n')
                .unwrap_or(self.content[next_start..].len());
        let next_line = &self.content[next_start..next_end];
        self.cursor = next_start + Self::byte_offset_for_char_column(next_line, column);
        true
    }

    fn move_word_left(&mut self) {
        while self.cursor > 0 {
            let prev = self.content[..self.cursor].chars().next_back().unwrap();
            if !prev.is_whitespace() {
                break;
            }
            self.cursor -= prev.len_utf8();
        }
        while self.cursor > 0 {
            let prev = self.content[..self.cursor].chars().next_back().unwrap();
            if prev.is_whitespace() {
                break;
            }
            self.cursor -= prev.len_utf8();
        }
    }

    fn move_word_right(&mut self) {
        while self.cursor < self.content.len() {
            let next = self.content[self.cursor..].chars().next().unwrap();
            if !next.is_whitespace() {
                break;
            }
            self.cursor += next.len_utf8();
        }
        while self.cursor < self.content.len() {
            let next = self.content[self.cursor..].chars().next().unwrap();
            if next.is_whitespace() {
                break;
            }
            self.cursor += next.len_utf8();
        }
    }

    fn delete_word_left(&mut self) {
        let old_cursor = self.cursor;
        self.move_word_left();
        self.content.drain(self.cursor..old_cursor);
    }

    fn delete_to_line_start(&mut self) {
        let start = self.current_line_bounds().0;
        if start < self.cursor {
            self.content.drain(start..self.cursor);
            self.cursor = start;
        }
    }

    fn delete_to_line_end(&mut self) {
        let end = self.current_line_bounds().1;
        if self.cursor < end {
            self.content.drain(self.cursor..end);
        }
    }

    fn prompt_text(&self) -> String {
        format!(" [{}] > ", self.mode.indicator())
    }

    pub(crate) fn set_cursor_from_point(&mut self, area: Rect, column: u16, row: u16) {
        let line_index = row.saturating_sub(area.y) as usize;
        let prompt_width = self.prompt_text().chars().count();
        let visual_column = column.saturating_sub(area.x) as usize;
        let content_column = visual_column.saturating_sub(prompt_width);
        let lines: Vec<&str> = self.content.split('\n').collect();
        let target_line = line_index.min(lines.len().saturating_sub(1));
        let line = lines.get(target_line).copied().unwrap_or("");
        let mut cursor = 0;
        for (index, existing) in lines.iter().enumerate() {
            if index == target_line {
                cursor += Self::byte_offset_for_char_column(line, content_column);
                break;
            }
            cursor += existing.len() + 1;
        }
        self.cursor = cursor.min(self.content.len());
        self.history_idx = None;
    }

    fn render_lines(&self, mode_style: Style) -> Vec<Line<'static>> {
        let prompt = self.prompt_text();
        let indent = " ".repeat(prompt.chars().count());
        let cursor = self.cursor.min(self.content.len());
        let mut lines: Vec<Vec<Span<'static>>> = vec![vec![Span::styled(prompt, mode_style)]];
        let mut cursor_rendered = false;

        for (idx, ch) in self.content.char_indices() {
            if idx == cursor {
                cursor_rendered = true;
                if ch == '\n' {
                    lines.last_mut().unwrap().push(Span::styled(
                        " ".to_string(),
                        Style::default().fg(Color::Black).bg(Color::White),
                    ));
                    lines.push(vec![Span::raw(indent.clone())]);
                    continue;
                }
                lines.last_mut().unwrap().push(Span::styled(
                    ch.to_string(),
                    Style::default().fg(Color::Black).bg(Color::White),
                ));
                continue;
            }

            if ch == '\n' {
                lines.push(vec![Span::raw(indent.clone())]);
            } else {
                lines.last_mut().unwrap().push(Span::raw(ch.to_string()));
            }
        }

        if !cursor_rendered {
            lines.last_mut().unwrap().push(Span::styled(
                " ".to_string(),
                Style::default().fg(Color::Black).bg(Color::White),
            ));
        }

        lines.into_iter().map(Line::from).collect()
    }
}

impl Default for InputLine {
    fn default() -> Self {
        Self::new()
    }
}

impl Component for InputLine {
    type Message = InputMsg;

    fn update(&mut self, msg: InputMsg) {
        match msg {
            InputMsg::SetMode(mode) => self.mode = mode,
            InputMsg::Clear => {
                self.content.clear();
                self.cursor = 0;
                self.history_idx = None;
            }
        }
    }

    fn render(&self, frame: &mut Frame, area: Rect) {
        let mode_style = match self.mode {
            Mode::Job => Style::default().fg(Color::Yellow),
            Mode::Cron => Style::default().fg(Color::Magenta),
        };

        let paragraph = Paragraph::new(self.render_lines(mode_style)).wrap(Wrap { trim: false });
        frame.render_widget(paragraph, area);
    }

    fn handle_key(&mut self, key: KeyEvent) -> Option<AppMsg> {
        // Only handle Press events.
        if key.kind != KeyEventKind::Press {
            return None;
        }

        match (key.modifiers, key.code) {
            // Shift+Tab → mode cycle
            (KeyModifiers::SHIFT, KeyCode::BackTab) | (_, KeyCode::BackTab) => {
                return Some(AppMsg::ModeSwitch);
            }

            // Ctrl+B → toggle sidebar
            (KeyModifiers::CONTROL, KeyCode::Char('b')) => {
                return Some(AppMsg::ToggleSidebar);
            }

            // Ctrl+C is handled globally by AppState.
            (KeyModifiers::CONTROL, KeyCode::Char('c')) => return None,

            // Enter / newline
            (mods, KeyCode::Enter)
                if mods.contains(KeyModifiers::SHIFT) || mods.contains(KeyModifiers::CONTROL) =>
            {
                self.insert_text("\n");
            }
            (KeyModifiers::NONE, KeyCode::Enter) => {
                let input = self.take_input();
                if !input.is_empty() {
                    return Some(AppMsg::Submit(input));
                }
            }

            // History navigation / multiline cursor movement
            (_, KeyCode::Up) if !self.move_up_line() => self.history_up(),
            (_, KeyCode::Up) => {}
            (_, KeyCode::Down) if !self.move_down_line() => self.history_down(),
            (_, KeyCode::Down) => {}

            // Cursor movement
            (KeyModifiers::ALT, KeyCode::Left) => self.move_word_left(),
            (KeyModifiers::ALT, KeyCode::Right) => self.move_word_right(),
            (mods, KeyCode::Left) if mods.contains(KeyModifiers::SUPER) => {
                self.move_to_line_start()
            }
            (mods, KeyCode::Right) if mods.contains(KeyModifiers::SUPER) => self.move_to_line_end(),
            (KeyModifiers::CONTROL, KeyCode::Char('a')) => self.cursor = 0,
            (KeyModifiers::CONTROL, KeyCode::Char('e')) => self.cursor = self.content.len(),
            (KeyModifiers::CONTROL, KeyCode::Char('u')) => self.delete_to_line_start(),
            (KeyModifiers::CONTROL, KeyCode::Char('k')) => self.delete_to_line_end(),
            (KeyModifiers::CONTROL, KeyCode::Char('w')) => self.delete_word_left(),
            (_, KeyCode::Left) => self.move_left(),
            (_, KeyCode::Right) => self.move_right(),
            (_, KeyCode::Home) => self.move_to_line_start(),
            (_, KeyCode::End) => self.move_to_line_end(),

            // Backspace
            (_, KeyCode::Backspace) if self.cursor > 0 => {
                let prev = self.content[..self.cursor]
                    .chars()
                    .next_back()
                    .map_or(0, char::len_utf8);
                self.content.drain(self.cursor - prev..self.cursor);
                self.cursor -= prev;
            }
            (_, KeyCode::Backspace) => {}

            // Delete
            (_, KeyCode::Delete) if self.cursor < self.content.len() => {
                let next = self.content[self.cursor..]
                    .chars()
                    .next()
                    .map_or(0, char::len_utf8);
                self.content.drain(self.cursor..self.cursor + next);
            }
            (_, KeyCode::Delete) => {}

            // Printable character
            (_, KeyCode::Char(c)) => {
                self.insert_text(&c.to_string());
            }

            _ => {}
        }

        None
    }

    fn handle_mouse(&mut self, _mouse: MouseEvent) -> Option<AppMsg> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shift_enter_inserts_newline() {
        let mut input = InputLine::new();
        input.insert_text("echo hi");

        input.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::SHIFT));

        assert_eq!(input.content, "echo hi\n");
        assert_eq!(input.desired_height(), 2);
    }

    #[test]
    fn super_arrows_move_to_line_edges() {
        let mut input = InputLine::new();
        input.insert_text("echo hi\nsleep 4");
        input.cursor = "echo hi\nsleep".len();

        input.handle_key(KeyEvent::new(KeyCode::Left, KeyModifiers::SUPER));
        assert_eq!(input.cursor, "echo hi\n".len());

        input.handle_key(KeyEvent::new(KeyCode::Right, KeyModifiers::SUPER));
        assert_eq!(input.cursor, input.content.len());
    }

    #[test]
    fn up_down_move_within_multiline_before_history() {
        let mut input = InputLine::new();
        input.insert_text("alpha\nbeta");
        input.cursor = input.content.len();

        input.handle_key(KeyEvent::new(KeyCode::Up, KeyModifiers::NONE));
        assert_eq!(input.cursor, 4);

        input.handle_key(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE));
        assert_eq!(input.cursor, input.content.len());
    }

    #[test]
    fn alt_arrows_and_ctrl_w_move_and_delete_words() {
        let mut input = InputLine::new();
        input.insert_text("echo hello world");
        input.cursor = input.content.len();

        input.handle_key(KeyEvent::new(KeyCode::Left, KeyModifiers::ALT));
        assert_eq!(&input.content[input.cursor..], "world");

        input.handle_key(KeyEvent::new(KeyCode::Char('w'), KeyModifiers::CONTROL));
        assert_eq!(input.content, "echo world");
        assert_eq!(input.cursor, "echo ".len());
    }

    #[test]
    fn ctrl_c_is_ignored_by_input_component() {
        let mut input = InputLine::new();
        input.insert_text("sleep 4");

        let msg = input.handle_key(KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL));

        assert!(msg.is_none());
        assert_eq!(input.content, "sleep 4");
        assert_eq!(input.cursor, "sleep 4".len());
    }

    #[test]
    fn current_word_range_tracks_cursor_token() {
        let mut input = InputLine::new();
        input.insert_text(":ki J");

        assert_eq!(input.current_word_range(), 4..5);
    }

    #[test]
    fn replace_range_updates_cursor_and_content() {
        let mut input = InputLine::new();
        input.insert_text(":ki");

        input.replace_range(0..3, ":kill ");

        assert_eq!(input.content, ":kill ");
        assert_eq!(input.cursor, ":kill ".len());
    }

    #[test]
    fn clicking_input_line_moves_cursor_with_prompt_offset() {
        let mut input = InputLine::new();
        input.insert_text("echo hi\npwd");

        input.set_cursor_from_point(Rect::new(0, 0, 40, 2), 10, 1);

        assert_eq!(input.cursor, "echo hi\n".len());
    }
}
