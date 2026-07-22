//! Root render function — lays out all components and draws one frame.
//!
//! ```text
//! ┌──────────┬──────────────────────────┐
//! │ header                              │ ← Header
//! ├──────────┬──────────────────────────┤
//! │ Sidebar  │  Main REPL Area          │
//! │          │                          │
//! ├──────────┴──────────────────────────┤
//! │  [MODE] > input                     │ ← Input Line
//! ├─────────────────────────────────────┤
//! │ footer                              │ ← Context Footer
//! └─────────────────────────────────────┘
//! ```

use ratatui::Frame;
use ratatui::layout::{Alignment, Constraint, Layout, Rect};
use ratatui::style::{Color, Style};
use ratatui::text::Line;
use ratatui::widgets::{Block, Borders, Clear, List, ListItem, Paragraph, Wrap};
use tui_term::widget::PseudoTerminal;

use crate::ansi;
use crate::app::AppState;
use crate::component::Component;
use crate::focus::FocusArea;
use crate::job_picker::job_picker_popup_rect;
use crate::target_settings::{target_settings_modal_footer_text, target_settings_popup_rect};

/// Render the entire TUI into the current frame.
pub(crate) fn draw(frame: &mut Frame, state: &AppState) {
    let area = frame.area();
    if state.fg_active() {
        let vertical = Layout::vertical([Constraint::Min(1), Constraint::Length(1)]).split(area);
        let body_area = vertical[0];
        let status_area = vertical[1];
        let block = Block::new()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Cyan))
            .title(state.fg_title());
        let inner = block.inner(body_area);
        frame.render_widget(block, body_area);
        if let Some(screen) = state.fg_screen() {
            frame.render_widget(PseudoTerminal::new(screen), inner);
        }
        let status = Paragraph::new(Line::from(state.fg_footer_text()))
            .style(Style::default().bg(Color::DarkGray).fg(Color::White));
        frame.render_widget(status, status_area);
        return;
    }

    let input_height = state
        .input
        .desired_height()
        .min(area.height.saturating_sub(4).max(1));

    // ── Vertical split: header | body | input line | footer ──
    let vertical = Layout::vertical([
        Constraint::Length(1), // header
        Constraint::Min(3),    // body (sidebar + main)
        Constraint::Length(input_height),
        Constraint::Length(1), // footer
    ])
    .split(area);

    let header_area = vertical[0];
    let body_area = vertical[1];
    let input_area = vertical[2];
    let footer_area = vertical[3];

    // ── Focus indicator styles ──
    let focus_style = Style::default().fg(Color::Cyan);
    let dim_style = Style::default().fg(Color::DarkGray);

    // ── Body: optional sidebar | main view ──
    if state.sidebar_visible() {
        // Sidebar width: 20–40 cols, ~25% of terminal width.
        let sidebar_width = (area.width / 4)
            .clamp(20, 40)
            .min(area.width.saturating_sub(30));

        let horizontal =
            Layout::horizontal([Constraint::Length(sidebar_width), Constraint::Min(30)])
                .split(body_area);

        state.sidebar.render(frame, horizontal[0]);
        render_workspace(frame, state, horizontal[1], focus_style, dim_style);
    } else {
        render_workspace(frame, state, body_area, focus_style, dim_style);
    }

    // ── Input line ──
    state.input.render(frame, input_area);

    // ── Header + footer ──
    state.status_bar.render(frame, header_area);
    render_footer(frame, state, footer_area);

    if state.job_picker_open() {
        render_job_picker(frame, state, area);
    }
    if state.target_settings_open() {
        render_target_settings_modal(frame, state, area);
    }
}

fn render_footer(frame: &mut Frame, state: &AppState, area: Rect) {
    let footer = Paragraph::new(Line::from(state.footer_text()))
        .style(Style::default().bg(Color::Black).fg(Color::DarkGray));
    frame.render_widget(footer, area);
}

fn main_title() -> &'static str {
    " Command Log "
}

fn render_workspace(
    frame: &mut Frame,
    state: &AppState,
    area: Rect,
    focus_style: Style,
    dim_style: Style,
) {
    let pane_style = if state.focus == FocusArea::MainView {
        focus_style
    } else {
        dim_style
    };
    let panes = Layout::vertical([Constraint::Percentage(60), Constraint::Min(6)]).split(area);

    let display_block = Block::new()
        .borders(Borders::ALL)
        .border_style(pane_style)
        .title(state.display_pane_title());
    let display_inner = display_block.inner(panes[0]);
    frame.render_widget(display_block, panes[0]);
    let display_content_area = if state.display_pane_has_target() {
        let chunks =
            Layout::vertical([Constraint::Length(1), Constraint::Min(1)]).split(display_inner);
        render_display_tabs(frame, state, chunks[0]);
        chunks[1]
    } else {
        display_inner
    };
    frame.render_widget(
        Paragraph::new(ansi::to_text(state.display_pane_content()))
            .wrap(Wrap { trim: false })
            .style(if state.display_pane_has_target() {
                Style::default().fg(Color::White)
            } else {
                Style::default().fg(Color::DarkGray)
            }),
        display_content_area,
    );

    let results_block = Block::new()
        .borders(Borders::ALL)
        .border_style(pane_style)
        .title(main_title());
    let results_inner = results_block.inner(panes[1]);
    frame.render_widget(results_block, panes[1]);
    state.main_view.render(frame, results_inner);
}

fn render_display_tabs(frame: &mut Frame, state: &AppState, area: Rect) {
    let active = state.active_display_tab();
    let mut spans = Vec::new();
    for (index, label) in state.display_tab_labels().into_iter().enumerate() {
        let style = if active == Some(index) {
            Style::default().fg(Color::Black).bg(Color::White)
        } else {
            Style::default().fg(Color::White).bg(Color::DarkGray)
        };
        spans.push(ratatui::text::Span::styled(label, style));
    }
    frame.render_widget(Paragraph::new(Line::from(spans)), area);
}

fn render_job_picker(frame: &mut Frame, state: &AppState, area: Rect) {
    let popup = job_picker_popup_rect(area);
    frame.render_widget(Clear, popup);

    let items = state.job_picker_items();
    let selected = state.job_picker_selected();
    let block = Block::new()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Cyan))
        .title(format!(" {} ", state.job_picker_title()))
        .title_bottom(
            Line::from(format!(
                " Enter {}   Esc close ",
                state.job_picker_submit_label()
            ))
            .alignment(Alignment::Center),
        );
    let inner = block.inner(popup);
    frame.render_widget(block, popup);

    if items.is_empty() {
        frame.render_widget(
            Paragraph::new(state.job_picker_empty_text())
                .alignment(Alignment::Center)
                .style(Style::default().fg(Color::DarkGray)),
            inner,
        );
        return;
    }

    let rows: Vec<ListItem> = items
        .into_iter()
        .enumerate()
        .map(|(index, item)| {
            let style = if selected == Some(index) {
                Style::default().fg(Color::Black).bg(Color::White)
            } else {
                Style::default().fg(Color::White)
            };
            ListItem::new(Line::from(format!(
                "{} {} {}",
                item.status_icon, item.id, item.label
            )))
            .style(style)
        })
        .collect();
    frame.render_widget(List::new(rows), inner);
}

fn render_target_settings_modal(frame: &mut Frame, state: &AppState, area: Rect) {
    let popup = target_settings_popup_rect(area);
    frame.render_widget(Clear, popup);
    let footer = target_settings_modal_footer_text(state.target_settings_can_save());

    let block = Block::new()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Cyan))
        .title(" Targets ")
        .title_bottom(Line::from(footer).alignment(Alignment::Center));
    let inner = block.inner(popup);
    frame.render_widget(block, popup);
    frame.render_widget(
        Paragraph::new(state.target_settings_content().unwrap_or_default())
            .wrap(Wrap { trim: false })
            .style(Style::default().fg(Color::White)),
        inner,
    );
}
