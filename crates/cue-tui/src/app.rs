//! App state and TEA update loop.
//!
//! Central state machine: all mutations flow through [`AppState::update`]
//! which pattern-matches on [`AppMsg`] and delegates to components.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::io::Write as _;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use crossterm::event::{
    KeyCode, KeyEvent, KeyEventKind, KeyModifiers, MouseButton, MouseEvent, MouseEventKind,
};

use cue_core::Mode;
use cue_core::command_spec::command_names;
use cue_core::cron::CronStatus;
use cue_core::ipc::{
    CronInfo, EventPayload, JobInfo, JobOpenHint, OkPayload, RequestPayload, ResponsePayload,
    ScriptItemInfo, ScriptItemResult, ScriptSubmitError, Stream,
};
use cue_core::job::JobStatus;
use ratatui::layout::{Constraint, Layout, Rect};
use tui_term::vt100;

use crate::client::{ReconnectCmd, RestartHandle, WriterHandle};
use crate::component::Component;
use crate::component::input_line::{InputLine, InputMsg};
use crate::component::main_view::{Card, CardStatus, MainView, MainViewMsg, chain_step_label};
use crate::component::sidebar::{OverviewCounts, Sidebar, SidebarItem, SidebarMsg};
use crate::component::status_bar::{StatusBar, StatusBarMsg};
use crate::target_config::{
    TargetProfileSource, TargetSettingsSnapshot, connector_for_profile, display_path,
    load_target_settings, save_default_profile,
};

// ── Focus ──

/// Which panel currently owns keyboard focus.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FocusArea {
    Input,
    MainView,
    Sidebar,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MouseMode {
    TextSelect,
    UiCapture,
}

impl MouseMode {
    pub fn label(self) -> &'static str {
        match self {
            Self::TextSelect => "text",
            Self::UiCapture => "ui",
        }
    }

    pub fn capture_enabled(self) -> bool {
        matches!(self, Self::UiCapture)
    }

    fn toggle(self) -> Self {
        match self {
            Self::TextSelect => Self::UiCapture,
            Self::UiCapture => Self::TextSelect,
        }
    }
}

// ── App-level message ──

/// All events that can mutate [`AppState`].
#[allow(clippy::large_enum_variant)]
pub enum AppMsg {
    // Raw terminal events
    KeyEvent(KeyEvent),
    MouseEvent(MouseEvent),
    Paste(String),
    Resize(u16, u16),

    // User actions
    Submit(String),
    ModeSwitch,
    ToggleSidebar,
    ToggleMouseMode,
    CopyFocus,
    ClearDisplay,
    OpenTargetSettings,
    OpenJobPicker,
    KillSelection,

    // Socket lifecycle
    Connected,
    Disconnected,
    Reconnected { writer: WriterHandle },
    Response { id: u32, payload: ResponsePayload },
    ServerEvent(EventPayload),

    // System
    Tick,
    Quit,
}

#[derive(Debug, Clone)]
struct JobRow {
    id: String,
    label: String,
    status: JobStatus,
    start_scope: Option<String>,
    end_scope: Option<String>,
    open_hint: JobOpenHint,
}

#[derive(Debug, Clone)]
struct CronRow {
    id: String,
    label: String,
    status: CronStatus,
}

#[derive(Debug, Clone)]
struct PendingSubmission {
    card_index: Option<usize>,
    input: String,
    mode: Mode,
    warnings: Vec<String>,
    silent: bool,
}

enum FgSessionKind {
    Job {
        card_index: Option<usize>,
        parser: Box<vt100::Parser>,
    },
}

struct FgSession {
    id: String,
    kind: FgSessionKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DisplayStream {
    Stdout,
    Stderr,
}

impl DisplayStream {
    fn label(self) -> &'static str {
        match self {
            Self::Stdout => "stdout",
            Self::Stderr => "stderr",
        }
    }
}

#[derive(Debug, Clone)]
struct DisplayTab {
    target: DisplayTarget,
    content: String,
    follow: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CopyTarget {
    label: String,
    content: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum DisplayTarget {
    Output { id: String, stream: DisplayStream },
    Preview { key: String, title: String },
}

#[derive(Debug, Clone)]
struct JobPickerState {
    selected: Option<usize>,
}

#[derive(Debug, Clone)]
struct TargetSettingsState {
    snapshot: TargetSettingsSnapshot,
    selected: usize,
    notice: Option<String>,
    /// Profile name waiting for an R-key reconnect trigger.
    /// `None` when no pending live-reconnect is available.
    pending_reconnect_profile: Option<String>,
}

#[derive(Debug, Clone)]
struct TargetSettingsView {
    content: String,
    profile_line_rows: Vec<usize>,
}

impl TargetSettingsState {
    fn new(snapshot: TargetSettingsSnapshot) -> Self {
        let selected = snapshot
            .profiles
            .iter()
            .position(|profile| profile.name == snapshot.default_profile)
            .unwrap_or(0);
        Self {
            snapshot,
            selected,
            notice: None,
            pending_reconnect_profile: None,
        }
    }

    fn with_notice(snapshot: TargetSettingsSnapshot, notice: String) -> Self {
        let mut state = Self::new(snapshot);
        state.notice = Some(notice);
        state
    }

    fn move_selection(&mut self, delta: isize) {
        if self.snapshot.profiles.is_empty() {
            self.selected = 0;
            return;
        }
        let max = self.snapshot.profiles.len().saturating_sub(1) as isize;
        let next = (self.selected as isize + delta).clamp(0, max);
        self.selected = next as usize;
    }

    fn selected_profile_name(&self) -> Option<&str> {
        self.snapshot
            .profiles
            .get(self.selected)
            .map(|profile| profile.name.as_str())
    }

    fn select_first(&mut self) {
        self.selected = 0;
    }

    fn select_last(&mut self) {
        if !self.snapshot.profiles.is_empty() {
            self.selected = self.snapshot.profiles.len() - 1;
        }
    }

    fn select_profile_name(&mut self, profile_name: &str) {
        if let Some(index) = self
            .snapshot
            .profiles
            .iter()
            .position(|profile| profile.name == profile_name)
        {
            self.selected = index;
        }
    }

    fn select_index(&mut self, index: usize) {
        if index < self.snapshot.profiles.len() {
            self.selected = index;
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DisplayTabHit {
    Activate(usize),
    Close(usize),
}

impl PendingSubmission {
    fn user(card_index: Option<usize>, input: String, mode: Mode, warnings: Vec<String>) -> Self {
        Self {
            card_index,
            input,
            mode,
            warnings,
            silent: false,
        }
    }

    fn silent() -> Self {
        Self {
            card_index: None,
            input: String::new(),
            mode: Mode::default(),
            warnings: Vec::new(),
            silent: true,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct UiRegions {
    header: Rect,
    input: Rect,
    main: Rect,
    display: Rect,
    results: Rect,
    results_inner: Rect,
    sidebar: Option<Rect>,
    sidebar_list: Option<Rect>,
}

// ── App state ──

/// Root application state.  Owns all component state and connection info.
pub struct AppState {
    // Components
    pub input: InputLine,
    pub main_view: MainView,
    pub sidebar: Sidebar,
    pub status_bar: StatusBar,

    // Connection
    pub writer: Option<WriterHandle>,
    pub connected: bool,
    /// Sender for live reconnect / target-switch commands.
    reconnect_tx: Option<tokio::sync::mpsc::Sender<ReconnectCmd>>,
    restart_handle: Option<RestartHandle>,
    /// Profile name we are reconnecting to; set when the user triggers a live
    /// reconnect, cleared and applied to `session_profile_name` on success.
    pending_reconnect_profile_name: Option<String>,

    // Entity state mirrored into the mode-specific sidebar.
    jobs: Vec<JobRow>,
    crons: Vec<CronRow>,
    job_cards: HashMap<String, usize>,
    cron_job_cards: HashMap<String, (String, usize)>,
    /// Maps chain_id → card_index for chain cards.
    chain_cards: HashMap<String, usize>,
    fg_session: Option<FgSession>,
    display_tabs: Vec<DisplayTab>,
    active_display_tab: Option<usize>,
    display_subscriptions: Vec<String>,
    job_picker: Option<JobPickerState>,
    target_settings: Option<TargetSettingsState>,
    target_settings_error: Option<String>,
    pending_submissions: BTreeMap<u32, PendingSubmission>,
    session_profile_name: Option<String>,

    // UI state
    pub mode: Mode,
    /// `None` = auto (show when width ≥ 100), `Some` = manual override.
    pub show_sidebar: Option<bool>,
    pub focus: FocusArea,
    pub mouse_mode: MouseMode,
    pub should_quit: bool,
    pub terminal_width: u16,
    pub terminal_height: u16,
}

impl AppState {
    pub fn new() -> Self {
        let mut state = Self {
            input: InputLine::new(),
            main_view: MainView::new(),
            sidebar: Sidebar::new(),
            status_bar: StatusBar::new(),
            writer: None,
            connected: false,
            reconnect_tx: None,
            restart_handle: None,
            pending_reconnect_profile_name: None,
            jobs: Vec::new(),
            crons: Vec::new(),
            job_cards: HashMap::new(),
            cron_job_cards: HashMap::new(),
            chain_cards: HashMap::new(),
            fg_session: None,
            display_tabs: Vec::new(),
            active_display_tab: None,
            display_subscriptions: Vec::new(),
            job_picker: None,
            target_settings: None,
            target_settings_error: None,
            pending_submissions: BTreeMap::new(),
            session_profile_name: None,
            mode: Mode::default(),
            show_sidebar: None,
            focus: FocusArea::Input,
            mouse_mode: MouseMode::UiCapture,
            should_quit: false,
            terminal_width: 80,
            terminal_height: 24,
        };
        state.sync_mode_views();
        state.set_focus(FocusArea::Input);
        state
            .status_bar
            .update(StatusBarMsg::SetMouseMode(state.mouse_mode));
        state.refresh_clear_action();
        state
    }

    pub fn set_session_profile_name(&mut self, session_profile_name: Option<String>) {
        self.session_profile_name = session_profile_name;
    }

    /// Store the control channel for the connection manager so the TUI can
    /// trigger live target switches.
    pub fn set_reconnect_tx(&mut self, tx: tokio::sync::mpsc::Sender<ReconnectCmd>) {
        self.reconnect_tx = Some(tx);
    }

    pub fn set_restart_handle(&mut self, restart_handle: Option<RestartHandle>) {
        self.restart_handle = restart_handle;
    }

    /// Whether the sidebar should be visible for the current terminal width.
    pub fn sidebar_visible(&self) -> bool {
        match self.show_sidebar {
            Some(v) => v,
            None => self.terminal_width >= 100,
        }
    }

    pub fn fg_active(&self) -> bool {
        self.fg_session.is_some()
    }

    pub fn fg_id(&self) -> Option<&str> {
        self.fg_session.as_ref().map(|session| session.id.as_str())
    }

    pub fn fg_screen(&self) -> Option<&vt100::Screen> {
        let session = self.fg_session.as_ref()?;
        let FgSessionKind::Job { parser, .. } = &session.kind;
        Some(parser.screen())
    }

    pub fn display_pane_title(&self) -> String {
        " Display ".to_string()
    }

    pub fn display_pane_content(&self) -> &str {
        self.active_display_tab
            .and_then(|index| self.display_tabs.get(index))
            .map(|tab| tab.content.as_str())
            .unwrap_or(
                "Use `:out J1` for a stdout snapshot, `:tail J1` to follow live stdout, or `:err J1` for stderr.",
            )
    }

    pub fn display_pane_has_target(&self) -> bool {
        self.active_display_tab.is_some()
    }

    pub fn target_settings_open(&self) -> bool {
        self.target_settings.is_some() || self.target_settings_error.is_some()
    }

    pub fn target_settings_can_save(&self) -> bool {
        self.target_settings.is_some()
    }

    pub fn footer_text(&self) -> String {
        if self.job_picker_open() {
            return match self.mode {
                Mode::Job => "Kill picker: Enter kill  •  Esc close".to_string(),
                Mode::Cron => "Remove picker: Enter remove  •  Esc close".to_string(),
            };
        }

        if self.target_settings_open() {
            let reconnect_hint = if self
                .target_settings
                .as_ref()
                .and_then(|s| s.pending_reconnect_profile.as_ref())
                .is_some()
            {
                "  •  R reconnect now"
            } else {
                ""
            };
            let primary_action = if self.target_settings_can_save() {
                "Enter save default  •  "
            } else {
                ""
            };
            return format!(
                "Targets: Up/Down/Home/End select  •  {primary_action}Ctrl+R reload  •  \
                 Esc/Ctrl+T close  •  Ctrl+Y copy  •  Shift+Tab mode{reconnect_hint}"
            );
        }

        match self.focus {
            FocusArea::Input => match self.mode {
                Mode::Job => {
                    "JOB: Enter submit  •  Shift+Enter newline  •  Tab complete  •  Shift+Tab mode"
                        .to_string()
                }
                Mode::Cron => {
                    "CRON: Enter schedule + command  •  Shift+Enter newline  •  Tab complete  •  Shift+Tab mode"
                        .to_string()
                }
            },
            FocusArea::Sidebar => {
                "Sidebar: Click row to open  •  Del/Backspace kill or remove  •  Up/Down move  •  Enter open  •  Shift+Tab mode  •  Ctrl+B toggle".to_string()
            }
            FocusArea::MainView => {
                if self.display_pane_has_target() {
                    "Display: Click tab to switch  •  × closes tab  •  Ctrl+Y copy active tab  •  Shift+Tab mode  •  Ctrl+L clears when idle".to_string()
                } else {
                    "Command log: Click cards to inspect  •  Ctrl+Y copy latest record  •  Shift+Tab mode  •  :out/:err snapshot  •  :tail follows live output".to_string()
                }
            }
        }
    }

    pub fn display_tab_labels(&self) -> Vec<String> {
        self.display_tabs
            .iter()
            .map(|tab| match &tab.target {
                DisplayTarget::Output { id, stream } => {
                    let prefix = if tab.follow { " follow" } else { "" };
                    format!("{prefix} {} {}  × ", stream.label(), id)
                }
                DisplayTarget::Preview { title, .. } => format!(" {title}  × "),
            })
            .collect()
    }

    pub fn active_display_tab(&self) -> Option<usize> {
        self.active_display_tab
    }

    fn copy_target(&self) -> Option<CopyTarget> {
        if let Some(job_id) = self.fg_id().filter(|_| self.fg_active()) {
            return Some(CopyTarget {
                label: format!("fg {job_id}"),
                content: self
                    .fg_screen()
                    .map(|screen| screen.contents().to_string())
                    .unwrap_or_default(),
            });
        }

        if self.target_settings_open()
            && let Some(content) = self.render_target_settings_content()
        {
            return Some(CopyTarget {
                label: "targets".into(),
                content,
            });
        }

        if let Some(tab) = self
            .active_display_tab
            .and_then(|index| self.display_tabs.get(index))
        {
            let label = match &tab.target {
                DisplayTarget::Output { id, stream } => format!("{} {id}", stream.label()),
                DisplayTarget::Preview { title, .. } => title.clone(),
            };
            return Some(CopyTarget {
                label,
                content: tab.content.clone(),
            });
        }

        self.main_view.cards.last().map(|card| CopyTarget {
            label: card
                .label
                .clone()
                .unwrap_or_else(|| "command-record".to_string()),
            content: format_card_preview(card),
        })
    }

    pub fn job_picker_open(&self) -> bool {
        self.job_picker.is_some()
    }

    pub fn job_picker_selected(&self) -> Option<usize> {
        self.job_picker.as_ref().and_then(|picker| picker.selected)
    }

    pub fn job_picker_title(&self) -> &'static str {
        match self.mode {
            Mode::Job => "Running Jobs",
            Mode::Cron => "Crons",
        }
    }

    pub fn job_picker_empty_text(&self) -> &'static str {
        match self.mode {
            Mode::Job => "No running jobs.",
            Mode::Cron => "No crons.",
        }
    }

    pub fn job_picker_submit_label(&self) -> &'static str {
        match self.mode {
            Mode::Job => "kill",
            Mode::Cron => "remove",
        }
    }

    pub fn job_picker_items(&self) -> Vec<(String, String, &'static str)> {
        match self.mode {
            Mode::Job => self
                .jobs
                .iter()
                .filter(|job| matches!(job.status, JobStatus::Running))
                .map(|job| {
                    (
                        job.id.clone(),
                        job.label.clone(),
                        job_status_icon(&job.status),
                    )
                })
                .collect(),
            Mode::Cron => self
                .crons
                .iter()
                .map(|cron| {
                    (
                        cron.id.clone(),
                        cron.label.clone(),
                        cron_status_icon(cron.status),
                    )
                })
                .collect(),
        }
    }

    pub fn target_settings_content(&self) -> Option<String> {
        self.render_target_settings_content()
    }

    fn fg_application_cursor(&self) -> bool {
        self.fg_screen()
            .is_some_and(vt100::Screen::application_cursor)
    }

    fn fg_bracketed_paste(&self) -> bool {
        self.fg_screen().is_some_and(vt100::Screen::bracketed_paste)
    }

    fn resize_fg_session(&mut self, cols: u16, rows: u16) {
        if let Some(FgSession {
            kind: FgSessionKind::Job { parser, .. },
            ..
        }) = self.fg_session.as_mut()
        {
            parser.screen_mut().set_size(rows, cols);
        }
    }

    fn sync_mode_views(&mut self) {
        self.input.update(InputMsg::SetMode(self.mode));
        self.main_view.update(MainViewMsg::SetMode(self.mode));
        self.sidebar.update(SidebarMsg::SetMode(self.mode));
        self.status_bar.update(StatusBarMsg::SetMode(self.mode));
        self.sync_sidebar_items();
    }

    fn set_focus(&mut self, focus: FocusArea) {
        self.focus = focus;
        self.sidebar
            .update(SidebarMsg::SetFocused(focus == FocusArea::Sidebar));
    }

    fn layout_regions(&self) -> UiRegions {
        let area = Rect::new(0, 0, self.terminal_width, self.terminal_height);
        let input_height = self
            .input
            .desired_height()
            .min(self.terminal_height.saturating_sub(5).max(1));
        let vertical = Layout::vertical([
            Constraint::Length(1),
            Constraint::Min(3),
            Constraint::Length(input_height),
            Constraint::Length(1),
        ])
        .split(area);

        let header = vertical[0];
        let body = vertical[1];
        let input = vertical[2];

        if self.sidebar_visible() {
            let sidebar_width = (self.terminal_width / 4)
                .clamp(20, 40)
                .min(self.terminal_width.saturating_sub(30));
            let horizontal =
                Layout::horizontal([Constraint::Length(sidebar_width), Constraint::Min(30)])
                    .split(body);
            let sidebar = horizontal[0];
            let main = horizontal[1];
            let panes =
                Layout::vertical([Constraint::Percentage(60), Constraint::Min(6)]).split(main);
            let sidebar_inner = inner_rect(sidebar);
            let results_inner = inner_rect(panes[1]);
            UiRegions {
                header,
                input,
                main,
                display: panes[0],
                results: panes[1],
                results_inner,
                sidebar: Some(sidebar),
                sidebar_list: Some(sidebar_inner),
            }
        } else {
            let panes =
                Layout::vertical([Constraint::Percentage(60), Constraint::Min(6)]).split(body);
            let results_inner = inner_rect(panes[1]);
            UiRegions {
                header,
                input,
                main: body,
                display: panes[0],
                results: panes[1],
                results_inner,
                sidebar: None,
                sidebar_list: None,
            }
        }
    }

    fn sync_sidebar_items(&mut self) {
        let items = match self.mode {
            Mode::Job => self.jobs.iter().rev().map(job_sidebar_item).collect(),
            Mode::Cron => self.crons.iter().rev().map(cron_sidebar_item).collect(),
        };
        self.sidebar.update(SidebarMsg::SetItems(items));
        self.refresh_overview();
    }

    fn refresh_overview(&mut self) {
        let counts = OverviewCounts {
            jobs: self.jobs.len() as u32,
            jobs_running: self
                .jobs
                .iter()
                .filter(|job| matches!(job.status, JobStatus::Running))
                .count() as u32,
            crons: self.crons.len() as u32,
        };
        self.set_overview(counts);
    }

    fn refresh_clear_action(&mut self) {
        self.status_bar.update(StatusBarMsg::SetClearEnabled(
            self.pending_submissions.is_empty(),
        ));
    }

    fn fg_terminal_size(&self) -> (u16, u16) {
        let cols = self.terminal_width.saturating_sub(2).max(1);
        let rows = self.terminal_height.saturating_sub(3).max(1);
        (cols, rows)
    }

    fn show_submission_result(
        &mut self,
        pending: &PendingSubmission,
        body: String,
        status: CardStatus,
        label: Option<String>,
    ) -> usize {
        let card_index = pending.card_index.unwrap_or_else(|| {
            self.main_view
                .push_card(pending.input.clone(), pending.mode)
        });
        if let Some(label) = label {
            self.main_view.set_card_label(card_index, label);
        }
        self.main_view.set_card_output(
            card_index,
            decorate_submission_output(&pending.warnings, body),
        );
        self.main_view.set_card_status(card_index, status);
        card_index
    }

    fn sync_display_subscriptions(&mut self) {
        let desired = self
            .display_tabs
            .iter()
            .filter_map(|tab| match (&tab.target, tab.follow) {
                (DisplayTarget::Output { id, .. }, true) => Some(id.clone()),
                _ => None,
            })
            .collect::<BTreeSet<_>>();
        let current = self
            .display_subscriptions
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();

        for id in desired.difference(&current) {
            let _ = self.enqueue_silent_request(
                RequestPayload::Subscribe {
                    channels: vec![format!("output:{id}")],
                },
                "output subscribe",
            );
        }
        for id in current.difference(&desired) {
            let _ = self.enqueue_silent_request(
                RequestPayload::Unsubscribe {
                    channels: vec![format!("output:{id}")],
                },
                "output unsubscribe",
            );
        }

        self.display_subscriptions = desired.into_iter().collect();
    }

    fn restore_display_subscriptions(&mut self) {
        for id in self.display_subscriptions.clone() {
            let _ = self.enqueue_silent_request(
                RequestPayload::Subscribe {
                    channels: vec![format!("output:{id}")],
                },
                "output resubscribe",
            );
        }
    }

    fn open_preview_display(&mut self, key: String, title: String, content: String) {
        if let Some(index) = self.display_tabs.iter().position(|tab| {
            tab.target
                == DisplayTarget::Preview {
                    key: key.clone(),
                    title: title.clone(),
                }
        }) {
            self.display_tabs[index].content = content;
            self.active_display_tab = Some(index);
            return;
        }

        self.display_tabs.push(DisplayTab {
            target: DisplayTarget::Preview { key, title },
            content,
            follow: false,
        });
        self.active_display_tab = Some(self.display_tabs.len() - 1);
    }

    fn show_output_display(
        &mut self,
        id: String,
        stream: DisplayStream,
        data: String,
        truncated: bool,
        follow: bool,
    ) {
        let content = if truncated {
            format!("{data}\n--- (truncated) ---")
        } else {
            data
        };
        if let Some(index) = self.display_tabs.iter().position(|tab| {
            matches!(
                &tab.target,
                DisplayTarget::Output {
                    id: existing_id,
                    stream: existing_stream,
                } if *existing_id == id && *existing_stream == stream
            )
        }) {
            self.display_tabs[index].content = content;
            self.display_tabs[index].follow = follow;
            self.active_display_tab = Some(index);
        } else {
            self.display_tabs.push(DisplayTab {
                target: DisplayTarget::Output {
                    id: id.clone(),
                    stream,
                },
                content,
                follow,
            });
            self.active_display_tab = Some(self.display_tabs.len() - 1);
        }
        self.sync_display_subscriptions();
    }

    fn append_display_output(&mut self, id: &str, stream: Stream, data: &str) {
        for tab in &mut self.display_tabs {
            if tab.follow
                && matches!(
                    (&tab.target, stream),
                    (
                        DisplayTarget::Output {
                            id: existing_id,
                            stream: DisplayStream::Stdout,
                        },
                        Stream::Stdout
                    ) if existing_id == id
                )
            {
                tab.content.push_str(data);
            }
            if tab.follow
                && matches!(
                    (&tab.target, stream),
                    (
                        DisplayTarget::Output {
                            id: existing_id,
                            stream: DisplayStream::Stderr,
                        },
                        Stream::Stderr
                    ) if existing_id == id
                )
            {
                tab.content.push_str(data);
            }
        }
    }

    fn clear_display_pane(&mut self) {
        let subscribed = std::mem::take(&mut self.display_subscriptions);
        for id in subscribed {
            let _ = self.enqueue_silent_request(
                RequestPayload::Unsubscribe {
                    channels: vec![format!("output:{id}")],
                },
                "output unsubscribe",
            );
        }
        self.display_tabs.clear();
        self.active_display_tab = None;
        self.close_target_settings();
    }

    fn activate_display_tab(&mut self, index: usize) {
        if index < self.display_tabs.len() {
            self.active_display_tab = Some(index);
        }
    }

    fn close_display_tab(&mut self, index: usize) {
        if self.display_tabs.get(index).is_none() {
            return;
        }
        self.display_tabs.remove(index);
        self.sync_display_subscriptions();

        self.active_display_tab = match self.display_tabs.is_empty() {
            true => None,
            false if index >= self.display_tabs.len() => Some(self.display_tabs.len() - 1),
            false => Some(index),
        };
    }

    fn display_tab_bar_rect(&self, display_area: Rect) -> Option<Rect> {
        if self.display_tabs.is_empty() || display_area.width <= 2 || display_area.height <= 2 {
            return None;
        }
        Some(Rect::new(
            display_area.x + 1,
            display_area.y + 1,
            display_area.width.saturating_sub(2),
            1,
        ))
    }

    fn display_tab_hit(&self, display_area: Rect, point: Rect) -> Option<DisplayTabHit> {
        let tab_bar = self.display_tab_bar_rect(display_area)?;
        if !contains(tab_bar, point) {
            return None;
        }

        let mut x = tab_bar.x;
        for (index, label) in self.display_tab_labels().into_iter().enumerate() {
            let width = label.chars().count() as u16;
            let start = x;
            let close_x = start + width.saturating_sub(3);
            let end = start + width;
            if point.x >= start && point.x < end {
                return if point.x >= close_x {
                    Some(DisplayTabHit::Close(index))
                } else {
                    Some(DisplayTabHit::Activate(index))
                };
            }
            x = end;
        }
        None
    }

    fn inspect_card(&mut self, index: usize) {
        let Some(card) = self.main_view.cards.get(index).cloned() else {
            return;
        };

        // For running jobs, route to the live view based on open_hint instead
        // of showing a static preview.
        if let Some(job_id) = self
            .job_cards
            .iter()
            .find(|&(_, &card_idx)| card_idx == index)
            .map(|(id, _)| id.clone())
            && let Some(job) = self.jobs.iter().find(|j| j.id == job_id)
            && matches!(job.status, JobStatus::Running)
        {
            let command = if matches!(job.open_hint, JobOpenHint::Fg) {
                format!(":fg {}", job.id)
            } else {
                format!(":tail {}", job.id)
            };
            self.update(AppMsg::Submit(command));
            return;
        }

        let title = card
            .label
            .clone()
            .map(|label| format!("record {label}"))
            .unwrap_or_else(|| "record".to_string());
        self.open_preview_display(format!("card:{index}"), title, format_card_preview(&card));
    }

    fn open_job_picker(&mut self) {
        self.close_target_settings();
        let items = self.job_picker_items();
        self.job_picker = Some(JobPickerState {
            selected: items.len().checked_sub(1),
        });
    }

    fn open_target_settings(&mut self) {
        if self.target_settings_open() {
            self.close_target_settings();
            return;
        }
        self.close_job_picker();
        self.load_target_settings(None);
        self.set_focus(FocusArea::MainView);
    }

    fn close_target_settings(&mut self) {
        self.target_settings = None;
        self.target_settings_error = None;
    }

    fn load_target_settings(&mut self, notice: Option<String>) {
        let preferred_selection = self
            .target_settings
            .as_ref()
            .and_then(|state| state.selected_profile_name().map(str::to_string));
        match load_target_settings() {
            Ok(snapshot) => {
                let mut state = match notice {
                    Some(notice) => TargetSettingsState::with_notice(snapshot, notice),
                    None => TargetSettingsState::new(snapshot),
                };
                if let Some(profile_name) = preferred_selection.as_deref() {
                    state.select_profile_name(profile_name);
                }
                self.target_settings = Some(state);
                self.target_settings_error = None;
            }
            Err(error) => {
                self.target_settings = None;
                self.target_settings_error =
                    Some(format!("failed to load target settings: {error}"));
            }
        }
    }

    fn render_target_settings_content(&self) -> Option<String> {
        if let Some(error) = &self.target_settings_error {
            return Some(error.clone());
        }
        self.render_target_settings_view().map(|view| view.content)
    }

    fn render_target_settings_view(&self) -> Option<TargetSettingsView> {
        self.target_settings
            .as_ref()
            .map(|state| format_target_settings_view(state, self.session_profile_name.as_deref()))
    }

    fn target_settings_popup_rect(&self) -> Rect {
        centered_rect(
            Rect::new(0, 0, self.terminal_width, self.terminal_height),
            82,
            78,
        )
    }

    fn target_settings_content_rect(&self) -> Option<Rect> {
        if !self.target_settings_open() {
            return None;
        }
        let popup = self.target_settings_popup_rect();
        Some(Rect::new(
            popup.x + 1,
            popup.y + 1,
            popup.width.saturating_sub(2),
            popup.height.saturating_sub(2),
        ))
    }

    fn move_target_selection(&mut self, delta: isize) {
        let Some(target_settings) = self.target_settings.as_mut() else {
            return;
        };
        target_settings.move_selection(delta);
    }

    fn move_target_selection_to_edge(&mut self, last: bool) {
        let Some(target_settings) = self.target_settings.as_mut() else {
            return;
        };
        if last {
            target_settings.select_last();
        } else {
            target_settings.select_first();
        }
    }

    fn reload_target_settings(&mut self) {
        self.load_target_settings(Some("reloaded target profiles from disk".into()));
        self.set_focus(FocusArea::MainView);
    }

    fn select_target_profile(&mut self, index: usize) {
        let Some(target_settings) = self.target_settings.as_mut() else {
            return;
        };
        target_settings.select_index(index);
    }

    fn target_settings_profile_hit(&self, point: Rect) -> Option<usize> {
        let content_area = self.target_settings_content_rect()?;
        if !contains(content_area, point) {
            return None;
        }
        let relative_y = point.y.saturating_sub(content_area.y) as usize;
        let view = self.render_target_settings_view()?;
        view.profile_line_rows
            .iter()
            .position(|line| *line == relative_y)
    }

    fn save_selected_target_profile(&mut self) {
        let Some((snapshot, profile_name)) = self.target_settings.as_ref().and_then(|state| {
            state
                .selected_profile_name()
                .map(|profile_name| (state.snapshot.clone(), profile_name.to_string()))
        }) else {
            return;
        };

        if snapshot.default_profile == profile_name {
            if let Some(state) = self.target_settings.as_mut() {
                state.notice = Some(format!(
                    "`{profile_name}` is already the default target for the next launch"
                ));
                state.pending_reconnect_profile = None;
            }
            return;
        }

        match save_default_profile(&profile_name, &snapshot) {
            Ok(snapshot) => {
                let source = display_path(&snapshot.source_path);
                let selected_profile = snapshot.profiles.iter().find(|p| p.name == profile_name);
                let can_live_reconnect = self.reconnect_tx.is_some()
                    && selected_profile.is_some_and(target_profile_supports_live_reconnect);
                let notice = if self.session_profile_name.as_deref() == Some(profile_name.as_str())
                {
                    format!(
                        "saved default profile `{profile_name}` to {source}; current session already uses it"
                    )
                } else if let Some(current_session) = self.session_profile_name.as_deref() {
                    if selected_profile.is_some_and(target_profile_is_ssh) {
                        format!(
                            "saved default profile `{profile_name}` to {source}; current session still uses `{current_session}`. SSH applies after restart/reconnect because live reconnect is unsupported"
                        )
                    } else if can_live_reconnect {
                        format!(
                            "saved default profile `{profile_name}` to {source}; current session still uses `{current_session}`. Press R to reconnect now"
                        )
                    } else {
                        format!(
                            "saved default profile `{profile_name}` to {source}; current session still uses `{current_session}` until reconnect/restart"
                        )
                    }
                } else if selected_profile.is_some_and(target_profile_is_ssh) {
                    format!(
                        "saved default profile `{profile_name}` to {source}; SSH applies on the next restart/reconnect because live reconnect is unsupported"
                    )
                } else {
                    format!(
                        "saved default profile `{profile_name}` to {source}; reconnect/restart cue to apply"
                    )
                };
                let mut next_state = TargetSettingsState::with_notice(snapshot, notice);
                if can_live_reconnect
                    && self.session_profile_name.as_deref() != Some(profile_name.as_str())
                {
                    next_state.pending_reconnect_profile = Some(profile_name.clone());
                }
                self.target_settings = Some(next_state);
                self.target_settings_error = None;
            }
            Err(error) => {
                if let Some(state) = self.target_settings.as_mut() {
                    state.notice = Some(format!("save failed: {error}"));
                    state.pending_reconnect_profile = None;
                }
            }
        }
    }

    /// Dispatch a live `SwitchTarget` command to the connection manager for
    /// the profile stored in `pending_reconnect_profile`.
    fn trigger_reconnect_now(&mut self) {
        let profile_name = self
            .target_settings
            .as_ref()
            .and_then(|s| s.pending_reconnect_profile.clone());
        let Some(profile_name) = profile_name else {
            return;
        };

        let connector = match connector_for_profile(&profile_name) {
            Ok(c) => c,
            Err(error) => {
                if let Some(state) = self.target_settings.as_mut() {
                    state.notice = Some(format!("reconnect failed: {error}"));
                    state.pending_reconnect_profile = None;
                }
                return;
            }
        };

        if let Some(ref tx) = self.reconnect_tx {
            if tx.try_send(ReconnectCmd::SwitchTarget(connector)).is_err() {
                if let Some(state) = self.target_settings.as_mut() {
                    state.notice = Some("reconnect command could not be sent; try again".into());
                    state.pending_reconnect_profile = None;
                }
                return;
            }
            self.pending_reconnect_profile_name = Some(profile_name.clone());
            if let Some(state) = self.target_settings.as_mut() {
                state.notice = Some(format!("reconnecting to `{profile_name}`…"));
                state.pending_reconnect_profile = None;
            }
        }
    }

    fn restart_daemon(&mut self) {
        let card_index = self.main_view.push_card(":restart".into(), self.mode);
        match &self.restart_handle {
            Some(handle) => match handle.restart() {
                Ok(()) => {
                    self.main_view.set_card_output(
                        card_index,
                        "daemon restart requested; waiting for reconnect".into(),
                    );
                    self.main_view
                        .set_card_status(card_index, CardStatus::Pending);
                }
                Err(error) => {
                    self.main_view.set_card_output(
                        card_index,
                        format!("Error [restart]: failed to restart daemon: {error:#}"),
                    );
                    self.main_view
                        .set_card_status(card_index, CardStatus::Error);
                }
            },
            None => {
                self.main_view.set_card_output(
                    card_index,
                    "Error [restart]: restart is unavailable for this session".into(),
                );
                self.main_view
                    .set_card_status(card_index, CardStatus::Error);
            }
        }
    }

    fn close_job_picker(&mut self) {
        self.job_picker = None;
    }

    fn move_job_picker(&mut self, delta: isize) {
        let items_len = self.job_picker_items().len();
        let Some(picker) = self.job_picker.as_mut() else {
            return;
        };
        if items_len == 0 {
            picker.selected = None;
            return;
        }

        let current = picker.selected.unwrap_or(items_len - 1) as isize;
        let next = (current + delta).clamp(0, items_len.saturating_sub(1) as isize);
        picker.selected = Some(next as usize);
    }

    fn kill_selected_job_from_picker(&mut self) {
        let Some(selected) = self.job_picker.as_ref().and_then(|picker| picker.selected) else {
            self.close_job_picker();
            return;
        };
        let items = self.job_picker_items();
        let Some((target_id, _, _)) = items.get(selected).cloned() else {
            self.close_job_picker();
            return;
        };
        self.close_job_picker();
        self.update(AppMsg::Submit(format!(":kill {target_id}")));
    }

    fn enqueue_silent_request(&mut self, payload: RequestPayload, description: &str) -> bool {
        let Some(writer) = &self.writer else {
            return false;
        };
        match writer.try_send(payload) {
            Ok(request_id) => {
                self.track_pending_submission(request_id, PendingSubmission::silent());
                true
            }
            Err(error) => {
                tracing::warn!("failed to send {description}: {error}");
                false
            }
        }
    }

    fn subscribe_core_channels(&mut self) {
        let _ = self.enqueue_silent_request(
            RequestPayload::Subscribe {
                channels: vec!["jobs".into(), "crons".into(), "system".into()],
            },
            "core subscriptions",
        );
    }

    fn request_sidebar_snapshots(&mut self) {
        for (input, mode) in [(":jobs", Mode::Job), (":crons", Mode::Cron)] {
            if !self.enqueue_silent_request(
                RequestPayload::Eval {
                    input: input.to_string(),
                    mode,
                },
                input,
            ) {
                break;
            }
        }
    }

    fn request_cron_snapshot(&mut self) {
        let _ = self.enqueue_silent_request(
            RequestPayload::Eval {
                input: ":crons".to_string(),
                mode: Mode::Cron,
            },
            ":crons",
        );
    }

    fn track_pending_submission(&mut self, request_id: u32, pending: PendingSubmission) {
        self.pending_submissions.insert(request_id, pending);
    }

    fn take_pending_submission(&mut self, request_id: u32) -> Option<PendingSubmission> {
        self.pending_submissions.remove(&request_id)
    }

    fn start_fg_session(&mut self, id: String, card_index: Option<usize>) {
        if id.starts_with('A') {
            return;
        }

        let (cols, rows) = self.fg_terminal_size();
        self.fg_session = Some(FgSession {
            id,
            kind: FgSessionKind::Job {
                card_index,
                parser: Box::new(vt100::Parser::new(rows, cols, 0)),
            },
        });
    }

    fn append_fg_output(&mut self, data: &[u8]) {
        let Some(FgSession {
            kind: FgSessionKind::Job { parser, .. },
            ..
        }) = self.fg_session.as_mut()
        else {
            return;
        };
        parser.process(data);
    }

    fn finish_fg_session(&mut self, id: &str, reason: &str) {
        let Some(session) = self.fg_session.take() else {
            return;
        };
        if session.id != id {
            self.fg_session = Some(session);
            return;
        }

        let FgSessionKind::Job { card_index, parser } = session.kind;
        if let Some(card_index) = card_index {
            let status = if reason == "done" || reason == "detached" {
                CardStatus::Success
            } else {
                CardStatus::Error
            };
            self.main_view.set_card_status(card_index, status);
            let rendered =
                String::from_utf8_lossy(&parser.screen().contents_formatted()).into_owned();
            if !rendered.is_empty() {
                self.main_view.set_card_output(card_index, rendered);
            }
        }
    }

    fn detach_fg_session(&mut self) {
        let is_job = self
            .fg_session
            .as_ref()
            .is_some_and(|session| matches!(session.kind, FgSessionKind::Job { .. }));
        if is_job {
            self.send_fg_detach();
        } else {
            self.fg_session = None;
            self.sync_mode_views();
        }
    }

    fn send_fg_detach(&self) {
        if let Some(writer) = &self.writer {
            writer.send(RequestPayload::FgDetach {});
        }
    }

    fn send_fg_input(&self, data: Vec<u8>) {
        if let Some(writer) = &self.writer {
            writer.send(RequestPayload::FgInput { data });
        }
    }

    fn send_fg_resize(&self, cols: u16, rows: u16) {
        if let Some(writer) = &self.writer {
            writer.send(RequestPayload::FgResize { cols, rows });
        }
    }

    fn copy_focus(&self) {
        let Some(target) = self.copy_target() else {
            return;
        };
        if let Err(error) = copy_to_clipboard(&target.content) {
            tracing::warn!(%error, target = %target.label, "failed to copy content");
        }
    }

    fn fail_pending_submissions(&mut self, message: &str) {
        let pending = std::mem::take(&mut self.pending_submissions);
        for (_, pending) in pending {
            if pending.silent {
                continue;
            }
            self.show_submission_result(&pending, message.to_string(), CardStatus::Error, None);
        }
        self.refresh_clear_action();
    }

    fn upsert_job(
        &mut self,
        id: String,
        label: String,
        status: JobStatus,
        start_scope: Option<String>,
        open_hint: JobOpenHint,
    ) {
        if let Some(job) = self.jobs.iter_mut().find(|job| job.id == id) {
            if !label.is_empty() {
                job.label = label;
            }
            job.status = status;
            if start_scope.is_some() {
                job.start_scope = start_scope;
            }
            job.open_hint = open_hint;
            return;
        }

        self.jobs.push(JobRow {
            id,
            label,
            status,
            start_scope,
            end_scope: None,
            open_hint,
        });
    }

    fn update_job_status(&mut self, id: &str, status: JobStatus, end_scope: Option<String>) {
        if let Some(index) = self.jobs.iter().position(|job| job.id == id) {
            self.jobs[index].status = status;
            if end_scope.is_some() {
                self.jobs[index].end_scope = end_scope;
            }
            if let Some(card_index) = self.job_cards.get(id).copied() {
                let job = self.jobs[index].clone();
                self.refresh_job_card(card_index, &job);
            }
            self.refresh_cron_trigger_card(id);
        } else {
            self.jobs.push(JobRow {
                id: id.to_string(),
                label: id.to_string(),
                status,
                start_scope: None,
                end_scope,
                open_hint: JobOpenHint::Stream,
            });
            self.refresh_cron_trigger_card(id);
        }
    }

    fn ensure_job_card(&mut self, job_id: &str, input: String) -> usize {
        if let Some(index) = self.job_cards.get(job_id).copied() {
            if self
                .main_view
                .cards
                .get(index)
                .is_some_and(|card| card.mode == Mode::Job && card.label.as_deref() == Some(job_id))
            {
                return index;
            }
            self.job_cards.remove(job_id);
        }

        let index = self.main_view.push_card(input, Mode::Job);
        self.main_view.set_card_label(index, job_id.to_string());
        self.job_cards.insert(job_id.to_string(), index);
        index
    }

    fn refresh_job_card(&mut self, card_index: usize, job: &JobRow) {
        self.main_view.set_card_output(
            card_index,
            format_job_record(
                &job.id,
                &job.status,
                job.start_scope.as_deref(),
                job.end_scope.as_deref(),
            ),
        );
        self.main_view
            .set_card_status(card_index, card_status_for_job(&job.status));
    }

    fn upsert_cron(&mut self, id: String, label: String, status: CronStatus) {
        if let Some(cron) = self.crons.iter_mut().find(|cron| cron.id == id) {
            let cron_id = cron.id.clone();
            if !label.is_empty() {
                cron.label = label;
            }
            cron.status = status;
            self.refresh_cron_cards_for_cron(&cron_id);
            return;
        }

        let cron_id = id.clone();
        self.crons.push(CronRow { id, label, status });
        self.refresh_cron_cards_for_cron(&cron_id);
    }

    fn replace_jobs(&mut self, list: Vec<JobInfo>) {
        self.jobs = list
            .into_iter()
            .map(|job| JobRow {
                id: job.id,
                label: job.pipeline,
                status: job.status,
                start_scope: job.start_scope,
                end_scope: job.end_scope,
                open_hint: job.open_hint,
            })
            .collect();
    }

    fn replace_crons(&mut self, list: Vec<CronInfo>) {
        self.crons = list
            .into_iter()
            .map(|cron| CronRow {
                id: cron.id,
                label: format!("{} {}", cron.schedule, cron.command),
                status: cron.status,
            })
            .collect();
        self.refresh_all_cron_trigger_cards();
    }

    fn ensure_cron_trigger_card(&mut self, cron_id: &str, job_id: &str) -> usize {
        if let Some((existing_cron_id, card_index)) = self.cron_job_cards.get(job_id).cloned()
            && existing_cron_id == cron_id
            && self
                .main_view
                .cards
                .get(card_index)
                .is_some_and(|card| card.mode == Mode::Cron)
        {
            return card_index;
        }

        let card_index = self
            .main_view
            .push_card(format!("cron trigger {cron_id}"), Mode::Cron);
        self.main_view
            .set_card_label(card_index, cron_id.to_string());
        self.cron_job_cards
            .insert(job_id.to_string(), (cron_id.to_string(), card_index));
        card_index
    }

    fn refresh_cron_trigger_card(&mut self, job_id: &str) {
        let Some((cron_id, card_index)) = self.cron_job_cards.get(job_id).cloned() else {
            return;
        };

        let cron = self.crons.iter().find(|cron| cron.id == cron_id).cloned();
        let cron_label = cron
            .as_ref()
            .map(|cron| cron.label.clone())
            .unwrap_or_else(|| cron_id.clone());
        let cron_status = cron
            .as_ref()
            .map(|cron| cron.status)
            .unwrap_or(CronStatus::Scheduled);
        let job = self.jobs.iter().find(|job| job.id == job_id).cloned();

        self.main_view.set_card_output(
            card_index,
            format_cron_trigger_record(&cron_id, &cron_label, cron_status, job.as_ref()),
        );
        self.main_view.set_card_status(
            card_index,
            job.as_ref()
                .map(|job| card_status_for_job(&job.status))
                .unwrap_or(CardStatus::Pending),
        );
    }

    fn refresh_cron_cards_for_cron(&mut self, cron_id: &str) {
        let job_ids: Vec<String> = self
            .cron_job_cards
            .iter()
            .filter_map(|(job_id, (mapped_cron_id, _))| {
                (mapped_cron_id == cron_id).then_some(job_id.clone())
            })
            .collect();
        for job_id in job_ids {
            self.refresh_cron_trigger_card(&job_id);
        }
    }

    fn refresh_all_cron_trigger_cards(&mut self) {
        let job_ids: Vec<String> = self.cron_job_cards.keys().cloned().collect();
        for job_id in job_ids {
            self.refresh_cron_trigger_card(&job_id);
        }
    }

    fn open_cron_row(&mut self, row: usize) {
        let Some(cron) = self.crons.get(row).cloned() else {
            return;
        };
        self.open_preview_display(
            format!("cron:{}", cron.id),
            format!("cron {}", cron.id),
            format_cron_preview(&cron),
        );
    }

    /// Convert a sidebar display row (newest-first) to the underlying vec index (oldest-first).
    fn sidebar_row_to_index(&self, row: usize, len: usize) -> Option<usize> {
        len.checked_sub(1)?.checked_sub(row)
    }

    fn activate_sidebar_row(&mut self, row: usize) {
        match self.mode {
            Mode::Job => {
                let Some(idx) = self.sidebar_row_to_index(row, self.jobs.len()) else {
                    return;
                };
                let Some(job) = self.jobs.get(idx) else {
                    return;
                };
                let command = if matches!(job.status, JobStatus::Running)
                    && matches!(job.open_hint, JobOpenHint::Fg)
                {
                    format!(":fg {}", job.id)
                } else if matches!(job.status, JobStatus::Running) {
                    format!(":tail {}", job.id)
                } else {
                    format!(":out {}", job.id)
                };
                self.update(AppMsg::Submit(command));
            }
            Mode::Cron => {
                if let Some(idx) = self.sidebar_row_to_index(row, self.crons.len()) {
                    self.open_cron_row(idx);
                }
            }
        }
    }

    fn selected_sidebar_kill_command(&self) -> Option<String> {
        let row = self.sidebar.selected?;
        match self.mode {
            Mode::Job => {
                let idx = self.sidebar_row_to_index(row, self.jobs.len())?;
                let job = self.jobs.get(idx)?;
                matches!(job.status, JobStatus::Running).then(|| format!(":kill {}", job.id))
            }
            Mode::Cron => {
                let idx = self.sidebar_row_to_index(row, self.crons.len())?;
                let cron = self.crons.get(idx)?;
                Some(format!(":kill {}", cron.id))
            }
        }
    }

    fn complete_input(&mut self) {
        let range = self.input.current_word_range();
        let cursor = self.input.cursor.min(self.input.content.len());
        let candidates = self.completion_candidates(cursor);
        if candidates.is_empty() {
            return;
        }

        let word = self.input.content[range.clone()].to_string();
        let replacement = if candidates.len() == 1 {
            if candidates[0].ends_with('/') {
                candidates[0].clone()
            } else {
                format!("{} ", candidates[0])
            }
        } else {
            let shared = shared_prefix(&candidates);
            if shared.len() <= word.len() {
                return;
            }
            shared
        };
        self.input.replace_range(range, &replacement);
    }

    fn completion_candidates(&self, cursor: usize) -> Vec<String> {
        let content = &self.input.content;
        let line_start = content[..cursor].rfind('\n').map_or(0, |idx| idx + 1);
        let line_prefix = &content[line_start..cursor];
        let trimmed = line_prefix.trim_start();
        let range = self.input.current_word_range();
        let word = &content[range];
        if !trimmed.starts_with(':') {
            return bare_completion_candidates(self.mode, line_prefix, word);
        }

        let tokens: Vec<&str> = trimmed.split_whitespace().collect();
        if tokens.is_empty() {
            return builtin_command_candidates(word);
        }

        if word.starts_with(':') && tokens.len() <= 1 {
            return builtin_command_candidates(word);
        }

        let command = tokens[0];
        let ids = match command {
            ":out" | ":err" | ":tail" | ":retry" => self
                .jobs
                .iter()
                .map(|job| job.id.clone())
                .collect::<Vec<_>>(),
            ":fg" | ":wait" => self
                .jobs
                .iter()
                .map(|job| job.id.clone())
                .collect::<Vec<_>>(),
            ":send" => self
                .jobs
                .iter()
                .map(|job| job.id.clone())
                .collect::<Vec<_>>(),
            ":kill" | ":cancel" | ":pause" | ":resume" | ":log" => {
                let mut ids = self
                    .jobs
                    .iter()
                    .map(|job| job.id.clone())
                    .collect::<Vec<_>>();
                ids.extend(self.crons.iter().map(|cron| cron.id.clone()));
                ids
            }
            _ => Vec::new(),
        };
        ids.into_iter()
            .filter(|candidate| candidate.starts_with(word))
            .collect()
    }

    /// TEA update: apply a message to the state.
    pub fn update(&mut self, msg: AppMsg) {
        match msg {
            AppMsg::Quit => {
                self.should_quit = true;
            }

            AppMsg::Resize(w, h) => {
                self.terminal_width = w;
                self.terminal_height = h;
                if self.fg_active() {
                    let (cols, rows) = self.fg_terminal_size();
                    self.resize_fg_session(cols, rows);
                    self.send_fg_resize(cols, rows);
                }
            }

            AppMsg::Tick => {
                // Status bar re-renders on every draw, clock updates automatically.
            }

            AppMsg::ModeSwitch => {
                if self.fg_active() {
                    return;
                }
                self.mode = self.mode.next();
                self.sync_mode_views();
            }

            AppMsg::ToggleSidebar => {
                let currently_visible = self.sidebar_visible();
                self.show_sidebar = Some(!currently_visible);
            }

            AppMsg::ToggleMouseMode => {
                self.mouse_mode = self.mouse_mode.toggle();
                self.status_bar
                    .update(StatusBarMsg::SetMouseMode(self.mouse_mode));
            }

            AppMsg::CopyFocus => {
                self.copy_focus();
            }

            AppMsg::ClearDisplay => {
                if self.pending_submissions.is_empty() {
                    self.main_view.clear_all();
                    self.clear_display_pane();
                    self.job_cards.clear();
                    self.chain_cards.clear();
                    self.refresh_clear_action();
                }
            }

            AppMsg::OpenJobPicker => {
                self.open_job_picker();
            }

            AppMsg::KillSelection => {
                if let Some(command) = self.selected_sidebar_kill_command() {
                    self.update(AppMsg::Submit(command));
                }
            }

            AppMsg::OpenTargetSettings => {
                self.open_target_settings();
            }

            AppMsg::Paste(text) => {
                if self.fg_active() {
                    self.send_fg_input(fg_paste_bytes(&text, self.fg_bracketed_paste()));
                    return;
                }
                if self.job_picker_open() {
                    return;
                }
                self.set_focus(FocusArea::Input);
                self.input.insert_text(&text);
            }

            AppMsg::Submit(text) => {
                if let Some(local) = parse_local_command(&text) {
                    self.input.update(InputMsg::Clear);
                    match local {
                        LocalCommand::Clear => self.update(AppMsg::ClearDisplay),
                        LocalCommand::Quit => self.update(AppMsg::Quit),
                        LocalCommand::Restart => self.restart_daemon(),
                    }
                    return;
                }

                let warnings = operator_spacing_warnings(&text);
                let card_index = submission_precreates_card(&text, self.mode, &warnings)
                    .then(|| self.main_view.push_card(text.clone(), self.mode));
                let pending =
                    PendingSubmission::user(card_index, text.clone(), self.mode, warnings);
                self.input.update(InputMsg::Clear);

                if let Some(ref writer) = self.writer {
                    let payload = RequestPayload::Eval {
                        input: text.clone(),
                        mode: self.mode,
                    };
                    match writer.try_send(payload) {
                        Ok(request_id) => {
                            self.track_pending_submission(request_id, pending);
                            self.refresh_clear_action();
                        }
                        Err(e) => {
                            tracing::warn!("failed to send command: {e}");
                            self.show_submission_result(
                                &pending,
                                format!("Error [transport]: failed to send command: {e}"),
                                CardStatus::Error,
                                None,
                            );
                        }
                    }
                } else {
                    self.show_submission_result(
                        &pending,
                        "Error [offline]: cued is not connected".to_string(),
                        CardStatus::Error,
                        None,
                    );
                }
            }

            AppMsg::Connected => {
                self.connected = true;
                self.status_bar.update(StatusBarMsg::SetConnected(true));
                self.sync_mode_views();
                self.subscribe_core_channels();
                self.request_sidebar_snapshots();
                self.restore_display_subscriptions();
                self.refresh_clear_action();
            }

            AppMsg::Disconnected => {
                self.fail_pending_submissions("Error [transport]: cued disconnected");
                self.fg_session = None;
                self.connected = false;
                self.writer = None;
                self.close_job_picker();
                self.status_bar.update(StatusBarMsg::SetConnected(false));
            }

            AppMsg::Reconnected { writer } => {
                self.writer = Some(writer);
                self.connected = true;
                self.status_bar.update(StatusBarMsg::SetConnected(true));
                self.sync_mode_views();
                self.subscribe_core_channels();
                self.request_sidebar_snapshots();
                self.restore_display_subscriptions();
                self.refresh_clear_action();
                // If this reconnect was triggered by a live target switch, apply
                // the new profile name and show confirmation.
                if let Some(profile) = self.pending_reconnect_profile_name.take() {
                    self.session_profile_name = Some(profile.clone());
                    if let Some(state) = self.target_settings.as_mut() {
                        state.notice = Some(format!("connected to `{profile}`"));
                    }
                }
            }

            AppMsg::Response { id, payload } => {
                let pending = self.take_pending_submission(id);
                self.refresh_clear_action();

                match payload {
                    ResponsePayload::Ok(ok) => match ok {
                        OkPayload::Ack {} => {
                            if let Some(pending) = pending.as_ref()
                                && !pending.silent
                            {
                                self.show_submission_result(
                                    pending,
                                    format_ack_message(&pending.input),
                                    CardStatus::Success,
                                    None,
                                );
                            }
                        }
                        OkPayload::ScriptCreated {
                            script_id,
                            items,
                            submit_error,
                        } => {
                            let mut sidebar_dirty = false;
                            for item in &items {
                                match &item.result {
                                    ScriptItemResult::Job {
                                        job_id,
                                        start_scope,
                                        open_hint,
                                    } => {
                                        self.upsert_job(
                                            job_id.clone(),
                                            summarize_script_source(&item.source),
                                            JobStatus::Running,
                                            start_scope.clone(),
                                            *open_hint,
                                        );
                                        sidebar_dirty = true;
                                    }
                                    ScriptItemResult::Chain { chain, .. } => {
                                        for job in &chain.jobs {
                                            if let (Some(job_id), Some(open_hint)) =
                                                (&job.job_id, &job.open_hint)
                                            {
                                                self.upsert_job(
                                                    job_id.clone(),
                                                    summarize_script_source(&job.pipeline),
                                                    job.status.clone(),
                                                    job.start_scope.clone(),
                                                    *open_hint,
                                                );
                                                sidebar_dirty = true;
                                            }
                                        }
                                    }
                                    ScriptItemResult::Cron { cron_id } => {
                                        self.upsert_cron(
                                            cron_id.clone(),
                                            summarize_script_source(&item.source),
                                            CronStatus::Scheduled,
                                        );
                                        sidebar_dirty = true;
                                    }
                                    ScriptItemResult::Message { .. } => {}
                                }
                            }
                            if sidebar_dirty {
                                self.sync_sidebar_items();
                            }
                            if let Some(pending) = pending.as_ref()
                                && !pending.silent
                            {
                                self.show_submission_result(
                                    pending,
                                    format_script_submission(&items, submit_error.as_ref()),
                                    if submit_error.is_some() {
                                        CardStatus::Error
                                    } else {
                                        CardStatus::Success
                                    },
                                    Some(script_id),
                                );
                            }
                        }
                        OkPayload::JobCreated {
                            job_id,
                            start_scope,
                            open_hint,
                            ..
                        } => {
                            let label = pending
                                .as_ref()
                                .map(|pending| normalize_command_label(&pending.input))
                                .unwrap_or_else(|| job_id.clone());
                            self.upsert_job(
                                job_id.clone(),
                                label,
                                JobStatus::Running,
                                start_scope,
                                open_hint,
                            );
                            self.sync_sidebar_items();
                            if let Some(pending) = pending.as_ref()
                                && !pending.silent
                            {
                                let card_index = if let Some(card_index) = pending.card_index {
                                    self.main_view.set_card_label(card_index, job_id.clone());
                                    self.job_cards.insert(job_id.clone(), card_index);
                                    card_index
                                } else {
                                    self.ensure_job_card(&job_id, pending.input.clone())
                                };
                                if let Some(job) =
                                    self.jobs.iter().find(|job| job.id == job_id).cloned()
                                {
                                    self.refresh_job_card(card_index, &job);
                                }
                            }
                        }
                        OkPayload::ChainCreated {
                            chain_id,
                            job_ids,
                            chain,
                        } => {
                            if let Some(pending) = pending.as_ref()
                                && !pending.silent
                            {
                                let card_index = self.show_submission_result(
                                    pending,
                                    format!("{}: {}", chain_id, job_ids.join(", ")),
                                    CardStatus::Success,
                                    Some(chain_id.clone()),
                                );
                                // Register chain → card mapping and annotate the card.
                                self.chain_cards.insert(chain_id.clone(), card_index);
                                if chain.total_jobs > 1 {
                                    self.main_view.update(MainViewMsg::SetCardChainLabel {
                                        index: card_index,
                                        label: format!("chain:{}", chain_id),
                                    });
                                }
                            }
                        }
                        OkPayload::FgAttached { id } => {
                            let card_index = if let Some(pending) = pending.as_ref()
                                && !pending.silent
                            {
                                Some(self.show_submission_result(
                                    pending,
                                    id.clone(),
                                    CardStatus::Streaming,
                                    Some(id.clone()),
                                ))
                            } else {
                                None
                            };
                            self.start_fg_session(id.clone(), card_index);
                            let (cols, rows) = self.fg_terminal_size();
                            self.send_fg_resize(cols, rows);
                        }
                        OkPayload::CronAdded { cron_id } => {
                            let label = pending
                                .as_ref()
                                .map(|pending| normalize_command_label(&pending.input))
                                .unwrap_or_else(|| cron_id.clone());
                            self.upsert_cron(cron_id.clone(), label, CronStatus::Scheduled);
                            self.sync_sidebar_items();
                            if let Some(pending) = pending.as_ref()
                                && !pending.silent
                            {
                                self.show_submission_result(
                                    pending,
                                    cron_id.clone(),
                                    CardStatus::Success,
                                    Some(cron_id),
                                );
                            }
                        }
                        OkPayload::ScopeCreated { hash, summary, .. } => {
                            if let Some(pending) = pending.as_ref()
                                && !pending.silent
                            {
                                self.show_submission_result(
                                    pending,
                                    summary,
                                    CardStatus::Success,
                                    Some(hash),
                                );
                            }
                        }
                        OkPayload::JobList(list) => {
                            let count = list.len();
                            self.replace_jobs(list);
                            self.sync_sidebar_items();
                            if let Some(pending) = pending.as_ref()
                                && !pending.silent
                            {
                                self.show_submission_result(
                                    pending,
                                    format!("loaded {count} job(s) into sidebar"),
                                    CardStatus::Success,
                                    None,
                                );
                            }
                        }
                        OkPayload::CronList(list) => {
                            let count = list.len();
                            self.replace_crons(list);
                            self.sync_sidebar_items();
                            if let Some(pending) = pending.as_ref()
                                && !pending.silent
                            {
                                self.show_submission_result(
                                    pending,
                                    format!("loaded {count} cron(s) into sidebar"),
                                    CardStatus::Success,
                                    None,
                                );
                            }
                        }
                        OkPayload::EvalText { text } => {
                            if let Some(pending) = pending.as_ref()
                                && !pending.silent
                            {
                                self.show_submission_result(
                                    pending,
                                    text,
                                    CardStatus::Success,
                                    None,
                                );
                            }
                        }
                        OkPayload::Pong {} => {
                            tracing::debug!("pong received");
                        }
                        OkPayload::Output {
                            id,
                            data,
                            truncated,
                        } => {
                            if let Some(pending) = pending.as_ref()
                                && !pending.silent
                            {
                                let request =
                                    display_request_from_submission(&pending.input, pending.mode)
                                        .unwrap_or(DisplayRequest {
                                            stream: DisplayStream::Stdout,
                                            follow: false,
                                        });
                                self.show_output_display(
                                    id.clone(),
                                    request.stream,
                                    data,
                                    truncated,
                                    request.follow,
                                );
                                self.show_submission_result(
                                    pending,
                                    format!(
                                        "{} {} for {id}",
                                        if request.follow {
                                            "following"
                                        } else {
                                            "opened"
                                        },
                                        request.stream.label()
                                    ),
                                    CardStatus::Success,
                                    None,
                                );
                            }
                        }
                        _ => {
                            if let Some(pending) = pending.as_ref()
                                && !pending.silent
                            {
                                let text = format!("{ok:?}");
                                self.show_submission_result(
                                    pending,
                                    text,
                                    CardStatus::Success,
                                    None,
                                );
                            }
                        }
                    },
                    ResponsePayload::Err { code, message } => {
                        if let Some(pending) = pending.as_ref()
                            && !pending.silent
                        {
                            self.show_submission_result(
                                pending,
                                format!("Error [{code}]: {message}"),
                                CardStatus::Error,
                                None,
                            );
                        }
                    }
                }
            }

            AppMsg::ServerEvent(event) => match event {
                EventPayload::OutputChunk { id, stream, data } => {
                    self.append_display_output(&id, stream, &data);
                }
                EventPayload::OutputEof { .. } => {}
                EventPayload::JobCreated {
                    job_id,
                    pipeline,
                    start_scope,
                    open_hint,
                    chain_id,
                    chain_index,
                    chain_total,
                } => {
                    self.upsert_job(
                        job_id.clone(),
                        pipeline,
                        JobStatus::Running,
                        start_scope,
                        open_hint,
                    );
                    // Annotate the chain card with a step label when a new chain job starts.
                    if let (Some(cid), Some(idx), Some(total)) =
                        (chain_id, chain_index, chain_total)
                        && let Some(&card_index) = self.chain_cards.get(&cid)
                        && total > 1
                    {
                        self.main_view.update(MainViewMsg::SetCardChainLabel {
                            index: card_index,
                            label: chain_step_label(&cid, idx, total),
                        });
                    }
                    self.refresh_cron_trigger_card(&job_id);
                    self.sync_sidebar_items();
                }
                EventPayload::JobStateChanged {
                    job_id,
                    old_state: _,
                    new_state,
                    end_scope,
                    ..
                } => {
                    self.update_job_status(&job_id, new_state, end_scope);
                    self.sync_sidebar_items();
                }
                EventPayload::JobRemoved { job_id } => {
                    self.jobs.retain(|job| job.id != job_id);
                    self.job_cards.remove(&job_id);
                    self.cron_job_cards.remove(&job_id);
                    self.sync_sidebar_items();
                }
                EventPayload::ChainStarted { chain } => {
                    if let Some(&card_index) = self.chain_cards.get(&chain.id) {
                        let running_step = chain
                            .jobs
                            .iter()
                            .position(|j| j.status == cue_core::job::JobStatus::Running)
                            .unwrap_or(0);
                        if chain.total_jobs > 1 {
                            self.main_view.update(MainViewMsg::SetCardChainLabel {
                                index: card_index,
                                label: chain_step_label(&chain.id, running_step, chain.total_jobs),
                            });
                        }
                    }
                }
                EventPayload::ChainProgress { chain } => {
                    if let Some(&card_index) = self.chain_cards.get(&chain.id) {
                        let running_step = chain
                            .jobs
                            .iter()
                            .position(|j| j.status == cue_core::job::JobStatus::Running)
                            .or_else(|| {
                                chain
                                    .jobs
                                    .iter()
                                    .rposition(|j| j.status == cue_core::job::JobStatus::Done)
                                    .map(|i| i + 1)
                            })
                            .unwrap_or(0)
                            .min(chain.total_jobs.saturating_sub(1));
                        if chain.total_jobs > 1 {
                            self.main_view.update(MainViewMsg::SetCardChainLabel {
                                index: card_index,
                                label: chain_step_label(&chain.id, running_step, chain.total_jobs),
                            });
                        }
                    }
                }
                EventPayload::ChainFinished { chain_id, success } => {
                    if let Some(&card_index) = self.chain_cards.get(&chain_id) {
                        let status = if success {
                            CardStatus::Success
                        } else {
                            CardStatus::Error
                        };
                        self.main_view.set_card_status(card_index, status);
                    }
                }
                EventPayload::FgOutput { data } => {
                    self.append_fg_output(&data);
                }
                EventPayload::FgExited { id, reason } => {
                    self.finish_fg_session(&id, &reason);
                }
                EventPayload::CronTriggered { cron_id, job_id } => {
                    self.ensure_cron_trigger_card(&cron_id, &job_id);
                    self.refresh_cron_trigger_card(&job_id);
                    self.request_cron_snapshot();
                }
                EventPayload::CronRemoved { cron_id } => {
                    self.crons.retain(|cron| cron.id != cron_id);
                    self.refresh_cron_cards_for_cron(&cron_id);
                    self.sync_sidebar_items();
                }
                EventPayload::ShuttingDown { reason } => {
                    self.main_view.update(MainViewMsg::AppendOutput {
                        data: format!("⚠ Daemon shutting down: {reason}"),
                    });
                    self.connected = false;
                    self.status_bar.update(StatusBarMsg::SetConnected(false));
                }
                _ => {
                    tracing::debug!(?event, "unhandled server event");
                }
            },

            AppMsg::KeyEvent(key) => {
                if self.fg_active() && key.kind == KeyEventKind::Press {
                    if key.modifiers.contains(KeyModifiers::CONTROL)
                        && matches!(key.code, KeyCode::Char('z'))
                    {
                        self.detach_fg_session();
                        return;
                    }
                    if key.modifiers.contains(KeyModifiers::CONTROL)
                        && matches!(key.code, KeyCode::Char('y'))
                    {
                        self.copy_focus();
                        return;
                    }
                    if let Some(bytes) = fg_key_bytes(key, self.fg_application_cursor()) {
                        self.send_fg_input(bytes);
                    }
                    return;
                }

                if self.job_picker_open() && key.kind == KeyEventKind::Press {
                    match key.code {
                        KeyCode::Esc => {
                            self.close_job_picker();
                        }
                        KeyCode::Up => {
                            self.move_job_picker(-1);
                        }
                        KeyCode::Down => {
                            self.move_job_picker(1);
                        }
                        KeyCode::Enter => {
                            self.kill_selected_job_from_picker();
                        }
                        KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                            self.close_job_picker();
                        }
                        _ => {}
                    }
                    return;
                }

                if key.kind == KeyEventKind::Press {
                    if is_mode_switch_key(key) {
                        self.update(AppMsg::ModeSwitch);
                        return;
                    }
                    if key.modifiers.contains(KeyModifiers::CONTROL)
                        && matches!(key.code, KeyCode::Char('l'))
                    {
                        self.update(AppMsg::ClearDisplay);
                        return;
                    }
                    if key.modifiers.contains(KeyModifiers::CONTROL)
                        && matches!(key.code, KeyCode::Char('d'))
                    {
                        self.update(AppMsg::Quit);
                        return;
                    }
                    if key.code == KeyCode::Char('c')
                        && key.modifiers.contains(KeyModifiers::CONTROL)
                    {
                        self.open_job_picker();
                        return;
                    }
                    if key.code == KeyCode::Char('y')
                        && key.modifiers.contains(KeyModifiers::CONTROL)
                    {
                        self.update(AppMsg::CopyFocus);
                        return;
                    }
                    if key.code == KeyCode::Char('r')
                        && key.modifiers.contains(KeyModifiers::CONTROL)
                        && self.focus == FocusArea::MainView
                        && self.target_settings_open()
                    {
                        self.reload_target_settings();
                        return;
                    }
                    if key.code == KeyCode::Char('t')
                        && key.modifiers.contains(KeyModifiers::CONTROL)
                    {
                        self.update(AppMsg::OpenTargetSettings);
                        return;
                    }
                    if key.code == KeyCode::Tab
                        && !key.modifiers.contains(KeyModifiers::SHIFT)
                        && self.focus == FocusArea::Input
                    {
                        self.complete_input();
                        return;
                    }
                }

                if self.target_settings_open() && key.kind == KeyEventKind::Press {
                    match key.code {
                        KeyCode::Esc => {
                            self.close_target_settings();
                            return;
                        }
                        KeyCode::Up => {
                            self.move_target_selection(-1);
                            return;
                        }
                        KeyCode::Down => {
                            self.move_target_selection(1);
                            return;
                        }
                        KeyCode::Home => {
                            self.move_target_selection_to_edge(false);
                            return;
                        }
                        KeyCode::End => {
                            self.move_target_selection_to_edge(true);
                            return;
                        }
                        KeyCode::Enter => {
                            self.save_selected_target_profile();
                            return;
                        }
                        // Live reconnect: [R] when a pending reconnect profile exists.
                        KeyCode::Char('r') | KeyCode::Char('R')
                            if !key.modifiers.contains(KeyModifiers::CONTROL)
                                && self.target_settings.as_ref().is_some_and(|settings| {
                                    settings.pending_reconnect_profile.is_some()
                                }) =>
                        {
                            self.trigger_reconnect_now();
                            return;
                        }
                        _ => {}
                    }
                }

                if self.focus != FocusArea::Input
                    && self.sidebar_visible()
                    && self.sidebar.selected.is_some()
                    && matches!(key.code, KeyCode::Backspace | KeyCode::Delete)
                {
                    self.update(AppMsg::KillSelection);
                    return;
                }

                let child_msg = match self.focus {
                    FocusArea::Input => self.input.handle_key(key),
                    FocusArea::MainView => self.main_view.handle_key(key),
                    FocusArea::Sidebar => self.sidebar.handle_key(key),
                };
                if let Some(msg) = child_msg {
                    self.update(msg);
                }
            }

            AppMsg::MouseEvent(mouse) => {
                if self.fg_active() {
                    return;
                }
                let regions = self.layout_regions();
                let point = Rect::new(mouse.column, mouse.row, 1, 1);

                if self.job_picker_open() {
                    if matches!(mouse.kind, MouseEventKind::Down(MouseButton::Left)) {
                        let popup = centered_rect(
                            Rect::new(0, 0, self.terminal_width, self.terminal_height),
                            70,
                            60,
                        );
                        if !contains(popup, point) {
                            self.close_job_picker();
                            return;
                        }
                        let inner = Rect::new(
                            popup.x + 1,
                            popup.y + 1,
                            popup.width.saturating_sub(2),
                            popup.height.saturating_sub(2),
                        );
                        let row = point.y.saturating_sub(inner.y) as usize;
                        if row < self.job_picker_items().len() {
                            if let Some(picker) = self.job_picker.as_mut() {
                                picker.selected = Some(row);
                            }
                            self.kill_selected_job_from_picker();
                        }
                    }
                    return;
                }

                if self.target_settings_open() {
                    if matches!(mouse.kind, MouseEventKind::Down(MouseButton::Left)) {
                        let popup = self.target_settings_popup_rect();
                        if !contains(popup, point) {
                            self.close_target_settings();
                            return;
                        }
                        if let Some(index) = self.target_settings_profile_hit(point) {
                            self.set_focus(FocusArea::MainView);
                            let already_selected = self
                                .target_settings
                                .as_ref()
                                .is_some_and(|state| state.selected == index);
                            if already_selected {
                                self.save_selected_target_profile();
                            } else {
                                self.select_target_profile(index);
                            }
                        }
                    }
                    return;
                }

                if matches!(mouse.kind, MouseEventKind::Down(MouseButton::Left)) {
                    if contains(regions.header, point) {
                        if let Some(msg) = self.status_bar.action_at(regions.header, point.x) {
                            self.update(msg);
                        }
                        return;
                    }
                    if contains(regions.input, point) {
                        self.set_focus(FocusArea::Input);
                        self.input
                            .set_cursor_from_point(regions.input, point.x, point.y);
                        return;
                    }
                    if let Some(sidebar) = regions.sidebar
                        && contains(sidebar, point)
                    {
                        self.set_focus(FocusArea::Sidebar);
                        if let Some(list) = regions.sidebar_list
                            && contains(list, point)
                        {
                            let row = point.y.saturating_sub(list.y) as usize;
                            if let Some(index) = self.sidebar.select_visible_row(row) {
                                self.activate_sidebar_row(index);
                            }
                        }
                        return;
                    }
                    if let Some(hit) = self.display_tab_hit(regions.display, point) {
                        self.set_focus(FocusArea::MainView);
                        match hit {
                            DisplayTabHit::Activate(index) => self.activate_display_tab(index),
                            DisplayTabHit::Close(index) => self.close_display_tab(index),
                        }
                        return;
                    }
                    if contains(regions.results_inner, point) {
                        self.set_focus(FocusArea::MainView);
                        if let Some(index) =
                            self.main_view.card_at_point(regions.results_inner, point)
                        {
                            self.inspect_card(index);
                        }
                        return;
                    }
                    if contains(regions.main, point) {
                        self.set_focus(FocusArea::MainView);
                        return;
                    }
                }

                match mouse.kind {
                    MouseEventKind::ScrollUp => {
                        if let Some(sidebar) = regions.sidebar
                            && contains(sidebar, point)
                        {
                            self.set_focus(FocusArea::Sidebar);
                            self.sidebar.move_selection(-1);
                        } else if contains(regions.results, point) {
                            self.set_focus(FocusArea::MainView);
                            if let Some(msg) = self.main_view.handle_mouse(mouse) {
                                self.update(msg);
                            }
                        }
                    }
                    MouseEventKind::ScrollDown => {
                        if let Some(sidebar) = regions.sidebar
                            && contains(sidebar, point)
                        {
                            self.set_focus(FocusArea::Sidebar);
                            self.sidebar.move_selection(1);
                        } else if contains(regions.results, point) {
                            self.set_focus(FocusArea::MainView);
                            if let Some(msg) = self.main_view.handle_mouse(mouse) {
                                self.update(msg);
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    /// Propagate overview counts to the sidebar.
    pub fn set_overview(&mut self, counts: OverviewCounts) {
        self.status_bar
            .update(StatusBarMsg::SetOverview(counts.clone()));
        self.sidebar.update(SidebarMsg::SetOverview(counts));
    }
}

impl Default for AppState {
    fn default() -> Self {
        Self::new()
    }
}

fn normalize_command_label(input: &str) -> String {
    let trimmed = input.trim();
    for prefix in [":run", ":cron"] {
        if let Some(rest) = trimmed.strip_prefix(prefix) {
            let rest = rest.trim();
            if !rest.is_empty() {
                return rest.to_string();
            }
        }
    }
    trimmed.to_string()
}

fn summarize_script_source(source: &str) -> String {
    let compact = source.split_whitespace().collect::<Vec<_>>().join(" ");
    truncate_display_text(&compact, 96)
}

fn truncate_display_text(text: &str, max_chars: usize) -> String {
    let mut chars = text.chars();
    let truncated: String = chars.by_ref().take(max_chars).collect();
    if chars.next().is_some() {
        format!("{truncated}...")
    } else {
        truncated
    }
}

fn format_script_submission(
    items: &[ScriptItemInfo],
    submit_error: Option<&ScriptSubmitError>,
) -> String {
    let mut lines = vec![format!("submitted {} item(s)", items.len())];
    for item in items {
        lines.push(format!(
            "{}. {} -> {}",
            item.index + 1,
            summarize_script_source(&item.source),
            format_script_item_result(&item.result),
        ));
    }
    if let Some(error) = submit_error {
        lines.push(String::new());
        lines.push(format!(
            "submit stopped at {}. {} [{}]: {}",
            error.index + 1,
            summarize_script_source(&error.source),
            error.code,
            error.message,
        ));
    }
    lines.join("\n")
}

fn format_script_item_result(result: &ScriptItemResult) -> String {
    match result {
        ScriptItemResult::Job { job_id, .. } => job_id.clone(),
        ScriptItemResult::Chain {
            chain_id, job_ids, ..
        } => {
            if job_ids.is_empty() {
                chain_id.clone()
            } else {
                format!("{chain_id} [{}]", job_ids.join(", "))
            }
        }
        ScriptItemResult::Cron { cron_id } => cron_id.clone(),
        ScriptItemResult::Message { text } => summarize_script_source(text),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LocalCommand {
    Clear,
    Quit,
    Restart,
}

fn parse_local_command(input: &str) -> Option<LocalCommand> {
    let trimmed = input.trim();
    match trimmed {
        ":clear" => Some(LocalCommand::Clear),
        ":quit" | ":exit" => Some(LocalCommand::Quit),
        ":restart" => Some(LocalCommand::Restart),
        _ => None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DisplayRequest {
    stream: DisplayStream,
    follow: bool,
}

fn display_request_from_submission(input: &str, mode: Mode) -> Option<DisplayRequest> {
    let trimmed = input.trim();
    let command = if mode == Mode::Job && !trimmed.starts_with(':') {
        return None;
    } else {
        trimmed.strip_prefix(':')?.split_whitespace().next()?
    };

    match command {
        "out" => Some(DisplayRequest {
            stream: DisplayStream::Stdout,
            follow: false,
        }),
        "tail" => Some(DisplayRequest {
            stream: DisplayStream::Stdout,
            follow: true,
        }),
        "err" => Some(DisplayRequest {
            stream: DisplayStream::Stderr,
            follow: false,
        }),
        _ => None,
    }
}

fn decorate_submission_output(warnings: &[String], body: String) -> String {
    if warnings.is_empty() {
        return body;
    }
    if body.is_empty() {
        return warnings.join("\n");
    }
    format!("{}\n\n{}", warnings.join("\n"), body)
}

fn operator_spacing_warnings(input: &str) -> Vec<String> {
    const OPERATORS: [&str; 4] = ["|||", "|?|", "->", "~>"];

    let mut warnings = Vec::new();
    let mut pos = 0;
    let mut in_quotes = false;

    while pos < input.len() {
        let rest = &input[pos..];
        let Some(ch) = rest.chars().next() else {
            break;
        };

        if ch == '\\' && in_quotes {
            pos += ch.len_utf8();
            if let Some(next) = input[pos..].chars().next() {
                pos += next.len_utf8();
            }
            continue;
        }

        if ch == '"' {
            in_quotes = !in_quotes;
            pos += ch.len_utf8();
            continue;
        }

        if !in_quotes && let Some(op) = OPERATORS.iter().find(|op| rest.starts_with(**op)) {
            let before_ok = input[..pos]
                .chars()
                .next_back()
                .is_none_or(char::is_whitespace);
            let after_pos = pos + op.len();
            let after_ok = input[after_pos..]
                .chars()
                .next()
                .is_none_or(char::is_whitespace);
            if !before_ok || !after_ok {
                warnings.push(format!(
                    "Warning: missing spaces around `{}`; did you mean `{}`?",
                    op,
                    spaced_operator_suggestion(input, pos, op),
                ));
            }
            pos = after_pos;
            continue;
        }

        pos += ch.len_utf8();
    }

    warnings
}

fn spaced_operator_suggestion(input: &str, pos: usize, op: &str) -> String {
    let before = input[..pos].trim_end_matches([' ', '\t']);
    let after = input[pos + op.len()..].trim_start_matches([' ', '\t']);
    format!("{before} {op} {after}")
}

fn submission_precreates_card(input: &str, mode: Mode, warnings: &[String]) -> bool {
    let _ = (input, mode, warnings);
    false
}

fn is_mode_switch_key(key: KeyEvent) -> bool {
    matches!(key.code, KeyCode::BackTab)
        || (key.code == KeyCode::Tab && key.modifiers.contains(KeyModifiers::SHIFT))
}

fn fg_key_bytes(key: KeyEvent, application_cursor: bool) -> Option<Vec<u8>> {
    match key.code {
        KeyCode::Char(ch) => {
            if key.modifiers.contains(KeyModifiers::CONTROL) {
                if ch.is_ascii_alphabetic() {
                    Some(vec![(ch.to_ascii_lowercase() as u8) & 0x1f])
                } else {
                    None
                }
            } else {
                Some(ch.to_string().into_bytes())
            }
        }
        KeyCode::Enter => Some(vec![b'\r']),
        KeyCode::Tab => Some(vec![b'\t']),
        KeyCode::Backspace => Some(vec![0x7f]),
        KeyCode::Esc => Some(vec![0x1b]),
        KeyCode::Left => Some(if application_cursor {
            b"\x1bOD".to_vec()
        } else {
            b"\x1b[D".to_vec()
        }),
        KeyCode::Right => Some(if application_cursor {
            b"\x1bOC".to_vec()
        } else {
            b"\x1b[C".to_vec()
        }),
        KeyCode::Up => Some(if application_cursor {
            b"\x1bOA".to_vec()
        } else {
            b"\x1b[A".to_vec()
        }),
        KeyCode::Down => Some(if application_cursor {
            b"\x1bOB".to_vec()
        } else {
            b"\x1b[B".to_vec()
        }),
        KeyCode::Home => Some(if application_cursor {
            b"\x1bOH".to_vec()
        } else {
            b"\x1b[H".to_vec()
        }),
        KeyCode::End => Some(if application_cursor {
            b"\x1bOF".to_vec()
        } else {
            b"\x1b[F".to_vec()
        }),
        KeyCode::Delete => Some(b"\x1b[3~".to_vec()),
        KeyCode::BackTab => Some(b"\x1b[Z".to_vec()),
        _ => None,
    }
}

fn fg_paste_bytes(text: &str, bracketed: bool) -> Vec<u8> {
    if bracketed {
        let mut wrapped = b"\x1b[200~".to_vec();
        wrapped.extend_from_slice(text.as_bytes());
        wrapped.extend_from_slice(b"\x1b[201~");
        wrapped
    } else {
        text.as_bytes().to_vec()
    }
}

pub(crate) fn contains(area: Rect, point: Rect) -> bool {
    point.x >= area.x
        && point.x < area.x + area.width
        && point.y >= area.y
        && point.y < area.y + area.height
}

fn inner_rect(area: Rect) -> Rect {
    Rect::new(
        area.x.saturating_add(1),
        area.y.saturating_add(1),
        area.width.saturating_sub(2),
        area.height.saturating_sub(2),
    )
}

fn centered_rect(area: Rect, width_pct: u16, height_pct: u16) -> Rect {
    let vertical = Layout::vertical([
        Constraint::Percentage((100 - height_pct) / 2),
        Constraint::Percentage(height_pct),
        Constraint::Percentage((100 - height_pct) / 2),
    ])
    .split(area);
    let horizontal = Layout::horizontal([
        Constraint::Percentage((100 - width_pct) / 2),
        Constraint::Percentage(width_pct),
        Constraint::Percentage((100 - width_pct) / 2),
    ])
    .split(vertical[1]);
    horizontal[1]
}

fn format_ack_message(input: &str) -> String {
    let trimmed = input.trim();
    for (prefix, verb) in [
        (":kill", "kill requested for"),
        (":cancel", "cancel requested for"),
        (":pause", "paused"),
        (":resume", "resumed"),
        (":send", "sent"),
    ] {
        if let Some(rest) = trimmed.strip_prefix(prefix) {
            let rest = rest.trim();
            if !rest.is_empty() {
                return format!("{verb} {rest}");
            }
        }
    }
    "ok".to_string()
}

fn format_card_preview(card: &Card) -> String {
    let mode = match card.mode {
        Mode::Job => "JOB",
        Mode::Cron => "CRON",
    };
    let status = match card.status {
        CardStatus::Success => "success",
        CardStatus::Error => "error",
        CardStatus::Pending => "pending",
        CardStatus::Streaming => "streaming",
    };
    let mut lines = vec![
        format!("mode: {mode}"),
        format!("input: {}", card.input),
        format!("status: {status}"),
    ];
    if let Some(label) = &card.label {
        lines.push(format!("label: {label}"));
    }
    if !card.output.is_empty() {
        lines.push(String::new());
        lines.push(card.output.clone());
    }
    lines.join("\n")
}

fn format_cron_preview(cron: &CronRow) -> String {
    format!(
        "id: {}\nstatus: {}\n{}",
        cron.id,
        format_cron_status(cron.status),
        cron.label
    )
}

fn format_cron_trigger_record(
    cron_id: &str,
    cron_label: &str,
    cron_status: CronStatus,
    job: Option<&JobRow>,
) -> String {
    let mut lines = vec![
        format!("cron: {cron_id}"),
        format!("cron status: {}", format_cron_status(cron_status)),
        format!("definition: {cron_label}"),
    ];

    match job {
        Some(job) => {
            lines.push(String::new());
            lines.push(format_job_record(
                &job.id,
                &job.status,
                job.start_scope.as_deref(),
                job.end_scope.as_deref(),
            ));
        }
        None => {
            lines.push(String::new());
            lines.push("job: awaiting snapshot".to_string());
        }
    }

    lines.join("\n")
}

fn format_target_settings_view(
    state: &TargetSettingsState,
    session_profile_name: Option<&str>,
) -> TargetSettingsView {
    let mut lines = vec![
        format!(
            "source: {}{}",
            display_path(&state.snapshot.source_path),
            if state.snapshot.using_legacy_config {
                " (legacy config fallback)"
            } else {
                ""
            }
        ),
        format!(
            "current session target: {}",
            session_profile_name.unwrap_or("n/a")
        ),
        format!("default on next launch: {}", state.snapshot.default_profile),
        format!(
            "ssh auto-detection: {}",
            if state.snapshot.auto_detect_ssh {
                "enabled (~/.ssh/config)"
            } else {
                "disabled"
            }
        ),
        match state.selected_profile_name() {
            Some(selected) if selected == state.snapshot.default_profile => {
                format!("selection: {selected} (already the saved default)")
            }
            Some(selected) => {
                format!("selection: {selected} (press Enter to save for next launch)")
            }
            None => "selection: none".into(),
        },
        "Ctrl+R reloads target profiles from disk; Esc or Ctrl+T closes this dialog.".into(),
        String::new(),
        "profiles:".into(),
    ];
    let mut profile_line_rows = Vec::new();

    for (index, profile) in state.snapshot.profiles.iter().enumerate() {
        profile_line_rows.push(lines.len());
        let marker = if index == state.selected { ">" } else { " " };
        let mut tags = Vec::new();
        if session_profile_name == Some(profile.name.as_str()) {
            tags.push("session");
        }
        if state.snapshot.default_profile == profile.name {
            tags.push("default");
        }
        if Some(profile.name.as_str()) == state.selected_profile_name() {
            tags.push("selected");
        }
        if let Some(source_tag) = target_profile_source_tag(profile) {
            tags.push(source_tag);
        }
        if let Some(alert) = target_profile_alert(profile) {
            tags.push(alert);
        }
        let tags = if tags.is_empty() {
            String::new()
        } else {
            format!(" [{}]", tags.join(", "))
        };
        lines.push(format!(
            "{marker} {} ({}){}",
            profile.name, profile.transport, tags
        ));
        lines.push(format!("    {}", profile.detail));
    }

    if let Some(notice) = &state.notice {
        lines.push(String::new());
        lines.push(format!("note: {notice}"));
    }

    TargetSettingsView {
        content: lines.join("\n"),
        profile_line_rows,
    }
}

fn target_profile_alert(
    profile: &crate::target_config::TargetProfileSummary,
) -> Option<&'static str> {
    match profile.transport.as_str() {
        "missing" => Some("missing"),
        "invalid" => Some("invalid"),
        "ssh" => Some("restart-only"),
        _ if profile.detail == "unrecognized transport kind" => Some("invalid"),
        _ => None,
    }
}

fn target_profile_source_tag(
    profile: &crate::target_config::TargetProfileSummary,
) -> Option<&'static str> {
    match profile.source {
        TargetProfileSource::Local => Some("permanent"),
        TargetProfileSource::Configured => Some("configured"),
        TargetProfileSource::AutoDetectedSsh => Some("auto"),
        TargetProfileSource::Missing => None,
    }
}

fn target_profile_is_ssh(profile: &crate::target_config::TargetProfileSummary) -> bool {
    profile.transport == "ssh"
}

fn target_profile_supports_live_reconnect(
    profile: &crate::target_config::TargetProfileSummary,
) -> bool {
    profile.transport == "unix"
}

fn format_job_record(
    job_id: &str,
    status: &JobStatus,
    start_scope: Option<&str>,
    end_scope: Option<&str>,
) -> String {
    let mut lines = vec![
        job_id.to_string(),
        format!("status: {}", format_job_status(status)),
    ];
    if let Some(start_scope) = start_scope {
        lines.push(format!("start scope: {start_scope}"));
    }
    if status.is_terminal()
        && let Some(end_scope) = end_scope
    {
        if start_scope == Some(end_scope) {
            lines.push(format!("end scope: no side effect ({end_scope})"));
        } else {
            lines.push(format!("end scope: {end_scope}"));
        }
    }
    lines.join("\n")
}

fn format_job_status(status: &JobStatus) -> String {
    match status {
        JobStatus::Pending => "pending".to_string(),
        JobStatus::Running => "running".to_string(),
        JobStatus::Done => "done".to_string(),
        JobStatus::Failed => "failed".to_string(),
        JobStatus::Killed => "killed".to_string(),
        JobStatus::Cancelled(reason) => format!("cancelled({reason:?})").to_lowercase(),
    }
}

fn card_status_for_job(status: &JobStatus) -> CardStatus {
    match status {
        JobStatus::Pending => CardStatus::Pending,
        JobStatus::Running => CardStatus::Streaming,
        JobStatus::Done => CardStatus::Success,
        JobStatus::Failed | JobStatus::Killed | JobStatus::Cancelled(_) => CardStatus::Error,
    }
}

fn builtin_command_candidates(word: &str) -> Vec<String> {
    let prefix = word.strip_prefix(':').unwrap_or(word);
    command_names()
        .chain(["restart"])
        .filter(|command| command.starts_with(prefix))
        .map(|command| format!(":{command}"))
        .collect()
}

fn bare_completion_candidates(mode: Mode, line_prefix: &str, word: &str) -> Vec<String> {
    match mode {
        Mode::Job => shell_segment_completion_candidates(line_prefix, word),
        Mode::Cron => cron_completion_candidates(line_prefix, word),
    }
}

fn cron_completion_candidates(line_prefix: &str, word: &str) -> Vec<String> {
    const KEYWORDS: &[&str] = &[
        "every", "in", "at", "on", "daily", "hourly", "weekly", "monthly", "cron",
    ];

    if let Some(command_start) = cron_command_start(line_prefix) {
        return shell_segment_completion_candidates(&line_prefix[command_start..], word);
    }

    KEYWORDS
        .iter()
        .filter(|keyword| keyword.starts_with(word))
        .map(|keyword| keyword.to_string())
        .collect()
}

fn cron_command_start(line_prefix: &str) -> Option<usize> {
    let trimmed = line_prefix.trim_start();
    let leading = line_prefix.len().saturating_sub(trimmed.len());
    let tokens = token_spans(trimmed);
    let first = tokens.first()?.0;

    let start_after = match first {
        "daily" | "hourly" | "weekly" | "monthly" => 1,
        "every" | "in" => 2,
        "cron" => 6,
        "at" => {
            if tokens.len() >= 4 && tokens.get(2).is_some_and(|token| token.0 == "on") {
                4
            } else {
                2
            }
        }
        "on" => {
            if tokens.len() >= 4 && tokens.get(2).is_some_and(|token| token.0 == "at") {
                4
            } else {
                2
            }
        }
        _ => return None,
    };

    if tokens.len() < start_after {
        return None;
    }
    let (_, _, end) = tokens[start_after - 1];
    Some(leading + end + 1)
}

fn shell_segment_completion_candidates(line_prefix: &str, word: &str) -> Vec<String> {
    let tokens = line_prefix.split_whitespace().collect::<Vec<_>>();
    let segment_start = tokens
        .iter()
        .rposition(|token| is_chain_operator(token))
        .map_or(0, |index| index + 1);
    let segment_token_count = tokens.len().saturating_sub(segment_start);
    let ends_with_whitespace = line_prefix.chars().last().is_some_and(char::is_whitespace);
    let completing_command = if ends_with_whitespace {
        line_prefix.trim().is_empty()
            || tokens.last().is_some_and(|token| is_chain_operator(token))
            || segment_token_count == 0
    } else {
        line_prefix.trim().is_empty() || segment_token_count <= 1
    };

    let mut candidates = path_completion_candidates(word);
    if completing_command {
        candidates.extend(command_completion_candidates(word));
    }
    candidates.sort();
    candidates.dedup();
    candidates
}

fn is_chain_operator(token: &str) -> bool {
    matches!(token, "->" | "~>" | "|||" | "|?|")
}

fn command_completion_candidates(prefix: &str) -> Vec<String> {
    let mut candidates = Vec::new();
    if prefix.contains('/') || prefix.starts_with('~') {
        return candidates;
    }

    if let Some(paths) = std::env::var_os("PATH") {
        for dir in std::env::split_paths(&paths) {
            if let Ok(entries) = fs::read_dir(dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                        continue;
                    };
                    if !name.starts_with(prefix) {
                        continue;
                    }
                    let Ok(metadata) = entry.metadata() else {
                        continue;
                    };
                    if metadata.is_file() && metadata.permissions().mode() & 0o111 != 0 {
                        candidates.push(name.to_string());
                    }
                }
            }
        }
    }
    candidates
}

fn path_completion_candidates(prefix: &str) -> Vec<String> {
    let (base_dir, partial, display_prefix) = path_completion_context(prefix);
    let Ok(entries) = fs::read_dir(base_dir) else {
        return Vec::new();
    };

    let mut candidates = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if !name.starts_with(&partial) {
            continue;
        }
        let suffix = if path.is_dir() { "/" } else { "" };
        candidates.push(format!("{display_prefix}{name}{suffix}"));
    }
    candidates
}

fn path_completion_context(prefix: &str) -> (PathBuf, String, String) {
    let expanded = expand_completion_prefix(prefix);
    let path = Path::new(&expanded);

    if prefix.ends_with('/') {
        let base = if expanded.is_empty() {
            std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
        } else {
            PathBuf::from(&expanded)
        };
        return (base, String::new(), prefix.to_string());
    }

    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty());
    let base_dir = parent
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")));
    let partial = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default()
        .to_string();
    let display_prefix = prefix
        .rfind('/')
        .map(|index| prefix[..=index].to_string())
        .unwrap_or_default();

    (base_dir, partial, display_prefix)
}

fn expand_completion_prefix(prefix: &str) -> String {
    if prefix == "~" || prefix.starts_with("~/") {
        let home = std::env::var("HOME").unwrap_or_default();
        if prefix == "~" {
            home
        } else {
            format!("{home}/{}", &prefix[2..])
        }
    } else {
        prefix.to_string()
    }
}

fn token_spans(input: &str) -> Vec<(&str, usize, usize)> {
    let mut tokens = Vec::new();
    let mut start = None;
    for (index, ch) in input.char_indices() {
        if ch.is_whitespace() {
            if let Some(token_start) = start.take() {
                tokens.push((&input[token_start..index], token_start, index));
            }
        } else if start.is_none() {
            start = Some(index);
        }
    }
    if let Some(token_start) = start {
        tokens.push((&input[token_start..], token_start, input.len()));
    }
    tokens
}

fn shared_prefix(items: &[String]) -> String {
    let Some(first) = items.first() else {
        return String::new();
    };
    let mut prefix = first.clone();
    for item in &items[1..] {
        let shared_len = prefix
            .chars()
            .zip(item.chars())
            .take_while(|(a, b)| a == b)
            .map(|(ch, _)| ch.len_utf8())
            .sum();
        prefix.truncate(shared_len);
        if prefix.is_empty() {
            break;
        }
    }
    prefix
}

fn job_sidebar_item(job: &JobRow) -> SidebarItem {
    SidebarItem {
        id: job.id.clone(),
        label: job.label.clone(),
        status_icon: job_status_icon(&job.status),
    }
}

fn job_status_icon(status: &JobStatus) -> &'static str {
    match status {
        JobStatus::Pending => "⏳",
        JobStatus::Running => "🔄",
        JobStatus::Done => "✅",
        JobStatus::Failed => "❌",
        JobStatus::Killed => "🛑",
        JobStatus::Cancelled(_) => "⏹",
    }
}

fn cron_status_icon(status: CronStatus) -> &'static str {
    match status {
        CronStatus::Scheduled => "⏰",
        CronStatus::Paused => "⏸",
        CronStatus::Completed => "✅",
        CronStatus::Expired => "⌛",
    }
}

fn cron_sidebar_item(cron: &CronRow) -> SidebarItem {
    SidebarItem {
        id: cron.id.clone(),
        label: cron.label.clone(),
        status_icon: cron_status_icon(cron.status),
    }
}

fn format_cron_status(status: CronStatus) -> &'static str {
    match status {
        CronStatus::Scheduled => "scheduled",
        CronStatus::Paused => "paused",
        CronStatus::Completed => "completed",
        CronStatus::Expired => "expired",
    }
}

fn copy_to_clipboard(text: &str) -> std::io::Result<()> {
    let encoded = BASE64_STANDARD.encode(text.as_bytes());
    let mut stdout = std::io::stdout();
    stdout.write_all(b"\x1b]52;c;")?;
    stdout.write_all(encoded.as_bytes())?;
    stdout.write_all(b"\x07")?;
    stdout.flush()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn queue_pending(state: &mut AppState, id: u32, pending: PendingSubmission) {
        state.track_pending_submission(id, pending);
    }

    fn test_target_profile(
        name: &str,
        transport: &str,
        detail: &str,
        source: TargetProfileSource,
    ) -> crate::target_config::TargetProfileSummary {
        crate::target_config::TargetProfileSummary {
            name: name.into(),
            transport: transport.into(),
            detail: detail.into(),
            source,
        }
    }

    fn test_target_snapshot(
        path: impl Into<PathBuf>,
        default_profile: &str,
        profiles: Vec<crate::target_config::TargetProfileSummary>,
    ) -> TargetSettingsSnapshot {
        TargetSettingsSnapshot {
            source_path: path.into(),
            using_legacy_config: false,
            auto_detect_ssh: true,
            default_profile: default_profile.into(),
            profiles,
        }
    }

    #[test]
    fn job_created_response_shows_job_id_not_stdout() {
        let mut state = AppState::new();
        let card_index = state.main_view.push_card("sleep 4".into(), Mode::Job);
        queue_pending(
            &mut state,
            1,
            PendingSubmission::user(Some(card_index), "sleep 4".into(), Mode::Job, Vec::new()),
        );
        state.update(AppMsg::Response {
            id: 1,
            payload: ResponsePayload::Ok(OkPayload::JobCreated {
                job_id: "J1".into(),
                start_scope: Some("S@abc12345".into()),
                open_hint: JobOpenHint::Stream,
                chain_id: None,
                chain_index: None,
                chain_total: None,
            }),
        });

        let card = state.main_view.cards.last().unwrap();
        assert_eq!(card.label.as_deref(), Some("J1"));
        assert_eq!(card.output, "J1\nstatus: running\nstart scope: S@abc12345");
        assert_eq!(card.status, CardStatus::Streaming);
        assert_eq!(state.jobs.len(), 1);
        assert_eq!(state.jobs[0].label, "sleep 4");
    }

    #[test]
    fn job_created_without_precreated_card_opens_command_log_record() {
        let mut state = AppState::new();
        queue_pending(
            &mut state,
            1,
            PendingSubmission::user(None, "sleep 4".into(), Mode::Job, Vec::new()),
        );
        state.update(AppMsg::Response {
            id: 1,
            payload: ResponsePayload::Ok(OkPayload::JobCreated {
                job_id: "J1".into(),
                start_scope: Some("S@abc12345".into()),
                open_hint: JobOpenHint::Stream,
                chain_id: None,
                chain_index: None,
                chain_total: None,
            }),
        });

        assert_eq!(state.main_view.cards.len(), 1);
        assert_eq!(
            state.main_view.cards[0].output,
            "J1\nstatus: running\nstart scope: S@abc12345"
        );
        assert_eq!(state.jobs.len(), 1);
        assert_eq!(state.jobs[0].label, "sleep 4");
    }

    #[test]
    fn output_events_do_not_overwrite_run_card() {
        let mut state = AppState::new();
        let card_index = state.main_view.push_card("ls".into(), Mode::Job);
        queue_pending(
            &mut state,
            1,
            PendingSubmission::user(Some(card_index), "ls".into(), Mode::Job, Vec::new()),
        );
        state.update(AppMsg::Response {
            id: 1,
            payload: ResponsePayload::Ok(OkPayload::JobCreated {
                job_id: "J1".into(),
                start_scope: Some("S@abc12345".into()),
                open_hint: JobOpenHint::Stream,
                chain_id: None,
                chain_index: None,
                chain_total: None,
            }),
        });
        state.update(AppMsg::ServerEvent(EventPayload::OutputChunk {
            id: "J1".into(),
            stream: cue_core::ipc::Stream::Stdout,
            data: "file.txt\n".into(),
        }));

        let card = state.main_view.cards.last().unwrap();
        assert_eq!(card.output, "J1\nstatus: running\nstart scope: S@abc12345");
    }

    #[test]
    fn running_stream_job_sidebar_open_prefers_tail() {
        let mut state = AppState::new();
        state.jobs.push(JobRow {
            id: "J1".into(),
            label: "sleep 5".into(),
            status: JobStatus::Running,
            start_scope: None,
            end_scope: None,
            open_hint: JobOpenHint::Stream,
        });

        state.activate_sidebar_row(0);

        let card = state.main_view.cards.last().unwrap();
        assert_eq!(card.input, ":tail J1");
    }

    #[test]
    fn running_fg_job_sidebar_open_prefers_fg() {
        let mut state = AppState::new();
        state.jobs.push(JobRow {
            id: "J1".into(),
            label: "vim notes.txt".into(),
            status: JobStatus::Running,
            start_scope: None,
            end_scope: None,
            open_hint: JobOpenHint::Fg,
        });

        state.activate_sidebar_row(0);

        let card = state.main_view.cards.last().unwrap();
        assert_eq!(card.input, ":fg J1");
    }

    #[test]
    fn output_response_without_precreated_card_opens_display_pane() {
        let mut state = AppState::new();
        queue_pending(
            &mut state,
            1,
            PendingSubmission::user(None, ":out J1".into(), Mode::Job, Vec::new()),
        );

        state.update(AppMsg::Response {
            id: 1,
            payload: ResponsePayload::Ok(OkPayload::Output {
                id: "J1".into(),
                data: "hello\n".into(),
                truncated: false,
            }),
        });

        assert_eq!(state.main_view.cards.len(), 1);
        let card = state.main_view.cards.last().unwrap();
        assert_eq!(card.input, ":out J1");
        assert_eq!(card.output, "opened stdout for J1");
        assert_eq!(card.status, CardStatus::Success);
        assert_eq!(state.display_pane_title(), " Display ".to_string());
        assert_eq!(
            state.display_tab_labels(),
            vec![" stdout J1  × ".to_string()]
        );
        assert_eq!(state.display_pane_content(), "hello\n");
    }

    #[test]
    fn tail_response_opens_following_stdout_tab() {
        let mut state = AppState::new();
        queue_pending(
            &mut state,
            1,
            PendingSubmission::user(None, ":tail J1".into(), Mode::Job, Vec::new()),
        );

        state.update(AppMsg::Response {
            id: 1,
            payload: ResponsePayload::Ok(OkPayload::Output {
                id: "J1".into(),
                data: "hello\n".into(),
                truncated: false,
            }),
        });
        state.update(AppMsg::ServerEvent(EventPayload::OutputChunk {
            id: "J1".into(),
            stream: cue_core::ipc::Stream::Stdout,
            data: "world\n".into(),
        }));

        let card = state.main_view.cards.last().unwrap();
        assert_eq!(card.output, "following stdout for J1");
        assert_eq!(
            state.display_tab_labels(),
            vec![" follow stdout J1  × ".to_string()]
        );
        assert_eq!(state.display_pane_content(), "hello\nworld\n");
    }

    #[test]
    fn err_response_opens_stderr_tab() {
        let mut state = AppState::new();
        queue_pending(
            &mut state,
            1,
            PendingSubmission::user(None, ":err J1".into(), Mode::Job, Vec::new()),
        );

        state.update(AppMsg::Response {
            id: 1,
            payload: ResponsePayload::Ok(OkPayload::Output {
                id: "J1".into(),
                data: "boom\n".into(),
                truncated: false,
            }),
        });

        assert_eq!(
            state.display_tab_labels(),
            vec![" stderr J1  × ".to_string()]
        );
        assert_eq!(state.display_pane_content(), "boom\n");
    }

    #[test]
    fn scope_created_response_shows_summary_not_only_hash() {
        let mut state = AppState::new();
        let card_index = state.main_view.push_card(":cd /tmp".into(), Mode::Job);
        queue_pending(
            &mut state,
            1,
            PendingSubmission::user(Some(card_index), ":cd /tmp".into(), Mode::Job, Vec::new()),
        );

        state.update(AppMsg::Response {
            id: 1,
            payload: ResponsePayload::Ok(OkPayload::ScopeCreated {
                hash: "S@abc12345".into(),
                label: Some("cd /tmp".into()),
                summary: "S@abc12345\ncwd: /old -> /tmp".into(),
            }),
        });

        let card = state.main_view.cards.last().unwrap();
        assert_eq!(card.label.as_deref(), Some("S@abc12345"));
        assert_eq!(card.output, "S@abc12345\ncwd: /old -> /tmp");
        assert_eq!(card.status, CardStatus::Success);
    }

    #[test]
    fn streaming_output_appends_only_to_active_display_pane() {
        let mut state = AppState::new();
        state.show_output_display(
            "J1".into(),
            DisplayStream::Stdout,
            "hello\n".into(),
            false,
            true,
        );

        state.update(AppMsg::ServerEvent(EventPayload::OutputChunk {
            id: "J1".into(),
            stream: cue_core::ipc::Stream::Stdout,
            data: "world\n".into(),
        }));
        state.update(AppMsg::ServerEvent(EventPayload::OutputChunk {
            id: "J2".into(),
            stream: cue_core::ipc::Stream::Stdout,
            data: "ignored\n".into(),
        }));

        assert_eq!(state.display_pane_content(), "hello\nworld\n");
    }

    #[test]
    fn clear_display_resets_upper_pane() {
        let mut state = AppState::new();
        state.show_output_display(
            "J1".into(),
            DisplayStream::Stdout,
            "hello\n".into(),
            false,
            false,
        );

        state.update(AppMsg::ClearDisplay);

        assert_eq!(state.display_pane_title(), " Display ".to_string());
        assert!(state.display_pane_content().contains("Use `:out J1`"));
    }

    #[test]
    fn ctrl_c_opens_running_job_picker() {
        let mut state = AppState::new();
        state.jobs = vec![
            JobRow {
                id: "J1".into(),
                label: "sleep 1".into(),
                status: JobStatus::Done,
                start_scope: None,
                end_scope: None,
                open_hint: JobOpenHint::Stream,
            },
            JobRow {
                id: "J2".into(),
                label: "sleep 2".into(),
                status: JobStatus::Running,
                start_scope: None,
                end_scope: None,
                open_hint: JobOpenHint::Stream,
            },
        ];

        state.update(AppMsg::KeyEvent(KeyEvent::new(
            KeyCode::Char('c'),
            KeyModifiers::CONTROL,
        )));

        assert!(state.job_picker_open());
        assert_eq!(state.job_picker_selected(), Some(0));
        assert_eq!(state.job_picker_items().len(), 1);
    }

    #[test]
    fn ctrl_c_in_cron_mode_opens_remove_picker() {
        let mut state = AppState::new();
        state.mode = Mode::Cron;
        state.sync_mode_views();
        state.crons = vec![CronRow {
            id: "C1".into(),
            label: "every 5m cargo test".into(),
            status: CronStatus::Scheduled,
        }];

        state.update(AppMsg::KeyEvent(KeyEvent::new(
            KeyCode::Char('c'),
            KeyModifiers::CONTROL,
        )));

        assert!(state.job_picker_open());
        assert_eq!(state.job_picker_selected(), Some(0));
        assert_eq!(state.job_picker_title(), "Crons");
        assert_eq!(state.job_picker_submit_label(), "remove");
        assert_eq!(state.job_picker_items()[0].0, "C1");
    }

    #[test]
    fn ctrl_d_quits_tui() {
        let mut state = AppState::new();

        state.update(AppMsg::KeyEvent(KeyEvent::new(
            KeyCode::Char('d'),
            KeyModifiers::CONTROL,
        )));

        assert!(state.should_quit);
    }

    #[test]
    fn targets_footer_shows_target_controls() {
        let mut state = AppState::new();
        state.session_profile_name = Some("local".into());
        state.target_settings = Some(TargetSettingsState::new(test_target_snapshot(
            "/tmp/client.toml",
            "remote",
            vec![
                test_target_profile(
                    "local",
                    "unix",
                    "socket: /tmp/cue.sock",
                    TargetProfileSource::Local,
                ),
                test_target_profile(
                    "remote",
                    "ssh",
                    "devbox | cued gateway --stdio",
                    TargetProfileSource::Configured,
                ),
            ],
        )));
        state.focus = FocusArea::MainView;

        assert!(state.footer_text().contains("Enter save default"));
        assert!(state.footer_text().contains("Ctrl+R reload"));
        assert!(state.target_settings_content().is_some_and(|content| {
            content.contains("current session target: local")
                && content.contains("ssh auto-detection: enabled")
        }));
    }

    #[test]
    fn targets_preview_keys_move_selection() {
        let mut state = AppState::new();
        state.target_settings = Some(TargetSettingsState::new(test_target_snapshot(
            "/tmp/client.toml",
            "local",
            vec![
                test_target_profile(
                    "local",
                    "unix",
                    "socket: /tmp/cue.sock",
                    TargetProfileSource::Local,
                ),
                test_target_profile(
                    "remote",
                    "ssh",
                    "devbox | cued gateway --stdio",
                    TargetProfileSource::Configured,
                ),
            ],
        )));
        state.focus = FocusArea::MainView;

        state.update(AppMsg::KeyEvent(KeyEvent::new(
            KeyCode::Down,
            KeyModifiers::NONE,
        )));

        assert_eq!(
            state
                .target_settings
                .as_ref()
                .and_then(TargetSettingsState::selected_profile_name),
            Some("remote")
        );
    }

    #[test]
    fn home_and_end_jump_target_selection() {
        let mut state = AppState::new();
        state.target_settings = Some(TargetSettingsState::new(test_target_snapshot(
            "/tmp/client.toml",
            "local",
            vec![
                test_target_profile(
                    "local",
                    "unix",
                    "socket: /tmp/cue.sock",
                    TargetProfileSource::Local,
                ),
                test_target_profile(
                    "remote",
                    "ssh",
                    "devbox | cued gateway --stdio",
                    TargetProfileSource::Configured,
                ),
                test_target_profile(
                    "staging",
                    "ssh",
                    "stagebox | cued gateway --stdio",
                    TargetProfileSource::AutoDetectedSsh,
                ),
            ],
        )));
        state.focus = FocusArea::MainView;

        state.update(AppMsg::KeyEvent(KeyEvent::new(
            KeyCode::End,
            KeyModifiers::NONE,
        )));
        assert_eq!(
            state
                .target_settings
                .as_ref()
                .and_then(TargetSettingsState::selected_profile_name),
            Some("staging")
        );

        state.update(AppMsg::KeyEvent(KeyEvent::new(
            KeyCode::Home,
            KeyModifiers::NONE,
        )));
        assert_eq!(
            state
                .target_settings
                .as_ref()
                .and_then(TargetSettingsState::selected_profile_name),
            Some("local")
        );
    }

    #[test]
    fn enter_on_targets_preview_saves_default_profile() {
        let unique = format!(
            "cue-shell-targets-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let dir = std::env::temp_dir().join(unique);
        std::fs::create_dir_all(&dir).unwrap();
        let config_path = dir.join("client.toml");
        std::fs::write(
            &config_path,
            r#"
[transport]
default_profile = "local"

[transport.profiles.local]
transport = "unix"

[transport.profiles.remote]
transport = "ssh"
destination = "devbox"
"#,
        )
        .unwrap();

        let mut state = AppState::new();
        state.target_settings = Some(TargetSettingsState::new(test_target_snapshot(
            config_path.clone(),
            "local",
            vec![
                test_target_profile(
                    "local",
                    "unix",
                    "socket: /tmp/cue.sock",
                    TargetProfileSource::Local,
                ),
                test_target_profile(
                    "remote",
                    "ssh",
                    "devbox | cued gateway --stdio",
                    TargetProfileSource::Configured,
                ),
            ],
        )));
        state.focus = FocusArea::MainView;
        state.move_target_selection(1);

        state.update(AppMsg::KeyEvent(KeyEvent::new(
            KeyCode::Enter,
            KeyModifiers::NONE,
        )));

        let saved = std::fs::read_to_string(&config_path).unwrap();
        assert!(saved.contains("default_profile = \"remote\""));
        assert_eq!(
            state
                .target_settings
                .as_ref()
                .map(|state| state.snapshot.default_profile.as_str()),
            Some("remote")
        );

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn enter_on_existing_default_profile_shows_noop_notice() {
        let mut state = AppState::new();
        state.target_settings = Some(TargetSettingsState::new(test_target_snapshot(
            "/tmp/client.toml",
            "remote",
            vec![
                test_target_profile(
                    "local",
                    "unix",
                    "socket: /tmp/cue.sock",
                    TargetProfileSource::Local,
                ),
                test_target_profile(
                    "remote",
                    "ssh",
                    "devbox | cued gateway --stdio",
                    TargetProfileSource::Configured,
                ),
            ],
        )));
        state.focus = FocusArea::MainView;
        state.move_target_selection(1);

        state.update(AppMsg::KeyEvent(KeyEvent::new(
            KeyCode::Enter,
            KeyModifiers::NONE,
        )));

        assert!(
            state
                .target_settings
                .as_ref()
                .and_then(|state| state.notice.as_deref())
                .is_some_and(|notice| notice.contains("already the default target"))
        );
    }

    #[test]
    fn saving_unix_profile_offers_live_reconnect() {
        let unique = format!(
            "cue-shell-targets-unix-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let dir = std::env::temp_dir().join(unique);
        std::fs::create_dir_all(&dir).unwrap();
        let config_path = dir.join("client.toml");
        std::fs::write(
            &config_path,
            r#"
[transport]
default_profile = "local"

[transport.profiles.local]
transport = "unix"
socket = "/tmp/cue.sock"

[transport.profiles.alt]
transport = "unix"
socket = "/tmp/alt.sock"
"#,
        )
        .unwrap();

        let mut state = AppState::new();
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        state.set_reconnect_tx(tx);
        state.session_profile_name = Some("local".into());
        state.target_settings = Some(TargetSettingsState::new(test_target_snapshot(
            config_path.clone(),
            "local",
            vec![
                test_target_profile(
                    "local",
                    "unix",
                    "socket: /tmp/cue.sock",
                    TargetProfileSource::Local,
                ),
                test_target_profile(
                    "alt",
                    "unix",
                    "socket: /tmp/alt.sock",
                    TargetProfileSource::Configured,
                ),
            ],
        )));
        state.focus = FocusArea::MainView;
        state.move_target_selection(1);

        state.update(AppMsg::KeyEvent(KeyEvent::new(
            KeyCode::Enter,
            KeyModifiers::NONE,
        )));

        assert!(
            state
                .target_settings
                .as_ref()
                .and_then(|state| state.notice.as_deref())
                .is_some_and(|notice| notice.contains("Press R to reconnect now"))
        );
        assert_eq!(
            state
                .target_settings
                .as_ref()
                .and_then(|state| state.pending_reconnect_profile.as_deref()),
            Some("alt")
        );

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn saving_ssh_profile_reports_restart_only() {
        let unique = format!(
            "cue-shell-targets-ssh-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let dir = std::env::temp_dir().join(unique);
        std::fs::create_dir_all(&dir).unwrap();
        let config_path = dir.join("client.toml");
        std::fs::write(
            &config_path,
            r#"
[transport]
default_profile = "local"

[transport.profiles.local]
transport = "unix"

[transport.profiles.remote]
transport = "ssh"
destination = "devbox"
"#,
        )
        .unwrap();

        let mut state = AppState::new();
        state.session_profile_name = Some("local".into());
        state.target_settings = Some(TargetSettingsState::new(test_target_snapshot(
            config_path.clone(),
            "local",
            vec![
                test_target_profile(
                    "local",
                    "unix",
                    "socket: /tmp/cue.sock",
                    TargetProfileSource::Local,
                ),
                test_target_profile(
                    "remote",
                    "ssh",
                    "devbox | cued gateway --stdio",
                    TargetProfileSource::Configured,
                ),
            ],
        )));
        state.focus = FocusArea::MainView;
        state.move_target_selection(1);

        state.update(AppMsg::KeyEvent(KeyEvent::new(
            KeyCode::Enter,
            KeyModifiers::NONE,
        )));

        assert!(
            state
                .target_settings
                .as_ref()
                .and_then(|state| state.notice.as_deref())
                .is_some_and(|notice| notice.contains("live reconnect is unsupported"))
        );
        assert_eq!(
            state
                .target_settings
                .as_ref()
                .and_then(|state| state.pending_reconnect_profile.as_deref()),
            None
        );

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn target_settings_mouse_click_selects_profile() {
        let mut state = AppState::new();
        state.target_settings = Some(TargetSettingsState::new(test_target_snapshot(
            "/tmp/client.toml",
            "local",
            vec![
                test_target_profile(
                    "local",
                    "unix",
                    "socket: /tmp/cue.sock",
                    TargetProfileSource::Local,
                ),
                test_target_profile(
                    "remote",
                    "ssh",
                    "devbox | cued gateway --stdio",
                    TargetProfileSource::Configured,
                ),
            ],
        )));
        state.focus = FocusArea::MainView;

        let content_area = state.target_settings_content_rect().unwrap();
        let view = state.render_target_settings_view().unwrap();
        let remote_line = view.profile_line_rows[1] as u16;

        state.update(AppMsg::MouseEvent(MouseEvent {
            kind: MouseEventKind::Down(MouseButton::Left),
            column: content_area.x.saturating_add(1),
            row: content_area.y.saturating_add(remote_line),
            modifiers: KeyModifiers::NONE,
        }));

        assert_eq!(
            state
                .target_settings
                .as_ref()
                .and_then(TargetSettingsState::selected_profile_name),
            Some("remote")
        );
    }

    #[test]
    fn target_settings_click_outside_modal_closes() {
        let mut state = AppState::new();
        state.target_settings = Some(TargetSettingsState::new(test_target_snapshot(
            "/tmp/client.toml",
            "local",
            vec![test_target_profile(
                "local",
                "unix",
                "socket: /tmp/cue.sock",
                TargetProfileSource::Local,
            )],
        )));

        state.update(AppMsg::MouseEvent(MouseEvent {
            kind: MouseEventKind::Down(MouseButton::Left),
            column: 0,
            row: 0,
            modifiers: KeyModifiers::NONE,
        }));

        assert!(!state.target_settings_open());
    }

    #[test]
    fn target_settings_marks_missing_profiles() {
        let state = TargetSettingsState::new(test_target_snapshot(
            "/tmp/client.toml",
            "remote",
            vec![test_target_profile(
                "remote",
                "missing",
                "profile is referenced by default_profile but not defined",
                TargetProfileSource::Missing,
            )],
        ));

        let view = format_target_settings_view(&state, Some("local"));

        assert!(view.content.contains("[default, selected, missing]"));
    }

    #[test]
    fn ctrl_t_toggles_targets_page_closed() {
        let mut state = AppState::new();
        state.target_settings = Some(TargetSettingsState::new(test_target_snapshot(
            "/tmp/client.toml",
            "local",
            vec![test_target_profile(
                "local",
                "unix",
                "socket: /tmp/cue.sock",
                TargetProfileSource::Local,
            )],
        )));
        state.focus = FocusArea::MainView;

        state.update(AppMsg::KeyEvent(KeyEvent::new(
            KeyCode::Char('t'),
            KeyModifiers::CONTROL,
        )));

        assert!(!state.target_settings_open());
        assert!(state.target_settings.is_none());
    }

    #[test]
    fn display_tabs_switch_and_close() {
        let mut state = AppState::new();
        state.show_output_display(
            "J1".into(),
            DisplayStream::Stdout,
            "one\n".into(),
            false,
            false,
        );
        state.show_output_display(
            "J2".into(),
            DisplayStream::Stdout,
            "two\n".into(),
            false,
            false,
        );

        assert_eq!(state.active_display_tab(), Some(1));
        assert_eq!(state.display_tab_labels().len(), 2);

        state.activate_display_tab(0);
        assert_eq!(state.display_pane_content(), "one\n");

        state.close_display_tab(0);
        assert_eq!(
            state.display_tab_labels(),
            vec![" stdout J2  × ".to_string()]
        );
        assert_eq!(state.display_pane_content(), "two\n");
    }

    #[test]
    fn copy_target_prefers_active_display_tab() {
        let mut state = AppState::new();
        state.show_output_display(
            "J1".into(),
            DisplayStream::Stdout,
            "hello\n".into(),
            false,
            false,
        );
        state.main_view.push_card("sleep 1".into(), Mode::Job);

        assert_eq!(
            state.copy_target(),
            Some(CopyTarget {
                label: "stdout J1".into(),
                content: "hello\n".into(),
            })
        );
    }

    #[test]
    fn cron_sidebar_open_shows_preview_tab() {
        let mut state = AppState::new();
        state.mode = Mode::Cron;
        state.sync_mode_views();
        state.crons.push(CronRow {
            id: "C1".into(),
            label: "every 5m cargo test".into(),
            status: CronStatus::Scheduled,
        });

        state.activate_sidebar_row(0);

        assert_eq!(state.display_tab_labels(), vec![" cron C1  × ".to_string()]);
        assert!(state.display_pane_content().contains("every 5m cargo test"));
    }

    #[test]
    fn sidebar_delete_removes_selected_cron() {
        let mut state = AppState::new();
        state.mode = Mode::Cron;
        state.sync_mode_views();
        state.crons.push(CronRow {
            id: "C1".into(),
            label: "every 5m cargo test".into(),
            status: CronStatus::Scheduled,
        });
        state.sync_sidebar_items();
        state.set_focus(FocusArea::Sidebar);
        state.sidebar.selected = Some(0);

        state.update(AppMsg::KeyEvent(KeyEvent::new(
            KeyCode::Backspace,
            KeyModifiers::NONE,
        )));

        let card = state.main_view.cards.last().expect("kill card");
        assert_eq!(card.input, ":kill C1");
        assert_eq!(card.mode, Mode::Cron);
    }

    #[test]
    fn opening_cron_from_sidebar_keeps_sidebar_focus() {
        let mut state = AppState::new();
        state.mode = Mode::Cron;
        state.sync_mode_views();
        state.crons.push(CronRow {
            id: "C1".into(),
            label: "every 5m cargo test".into(),
            status: CronStatus::Scheduled,
        });
        state.set_focus(FocusArea::Sidebar);

        state.activate_sidebar_row(0);

        assert_eq!(state.focus, FocusArea::Sidebar);
        assert_eq!(state.display_tab_labels(), vec![" cron C1  × ".to_string()]);
    }

    #[test]
    fn delete_from_main_view_removes_selected_cron() {
        let mut state = AppState::new();
        state.mode = Mode::Cron;
        state.sync_mode_views();
        state.show_sidebar = Some(true);
        state.crons.push(CronRow {
            id: "C1".into(),
            label: "every 5m cargo test".into(),
            status: CronStatus::Scheduled,
        });
        state.sync_sidebar_items();
        state.sidebar.selected = Some(0);
        state.focus = FocusArea::MainView;

        state.update(AppMsg::KeyEvent(KeyEvent::new(
            KeyCode::Delete,
            KeyModifiers::NONE,
        )));

        let card = state.main_view.cards.last().expect("kill card");
        assert_eq!(card.input, ":kill C1");
        assert_eq!(card.mode, Mode::Cron);
    }

    #[test]
    fn cron_trigger_event_creates_cron_record() {
        let mut state = AppState::new();
        state.crons.push(CronRow {
            id: "C1".into(),
            label: "every 5m cargo test".into(),
            status: CronStatus::Scheduled,
        });

        state.update(AppMsg::ServerEvent(EventPayload::CronTriggered {
            cron_id: "C1".into(),
            job_id: "J42".into(),
        }));

        let card = state.main_view.cards.last().expect("cron trigger card");
        assert_eq!(card.mode, Mode::Cron);
        assert_eq!(card.label.as_deref(), Some("C1"));
        assert!(card.output.contains("definition: every 5m cargo test"));
        assert!(card.output.contains("job: awaiting snapshot"));
    }

    #[test]
    fn cron_trigger_card_tracks_job_status_updates() {
        let mut state = AppState::new();
        state.crons.push(CronRow {
            id: "C1".into(),
            label: "every 5m cargo test".into(),
            status: CronStatus::Scheduled,
        });
        state.update(AppMsg::ServerEvent(EventPayload::CronTriggered {
            cron_id: "C1".into(),
            job_id: "J42".into(),
        }));

        state.update(AppMsg::ServerEvent(EventPayload::JobCreated {
            job_id: "J42".into(),
            pipeline: "cargo test".into(),
            start_scope: Some("S@abc".into()),
            open_hint: JobOpenHint::Stream,
            chain_id: None,
            chain_index: None,
            chain_total: None,
        }));
        state.update(AppMsg::ServerEvent(EventPayload::JobStateChanged {
            job_id: "J42".into(),
            old_state: JobStatus::Running,
            new_state: JobStatus::Done,
            end_scope: Some("S@def".into()),
            chain_id: None,
            chain_index: None,
        }));

        let card = state.main_view.cards.last().expect("cron trigger card");
        assert_eq!(card.status, CardStatus::Success);
        assert!(card.output.contains("J42"));
        assert!(card.output.contains("status: done"));
        assert!(card.output.contains("start scope: S@abc"));
        assert!(card.output.contains("end scope: S@def"));
    }

    #[test]
    fn card_inspection_opens_preview_tab() {
        let mut state = AppState::new();
        let card_index = state.main_view.push_card("cargo test".into(), Mode::Job);
        state.main_view.set_card_output(card_index, "done".into());

        state.inspect_card(card_index);

        assert_eq!(state.display_tab_labels(), vec![" record  × ".to_string()]);
        assert!(state.display_pane_content().contains("input: cargo test"));
    }

    #[test]
    fn tab_completes_builtin_command() {
        let mut state = AppState::new();
        state.input.insert_text(":ki");

        state.update(AppMsg::KeyEvent(KeyEvent::new(
            KeyCode::Tab,
            KeyModifiers::NONE,
        )));

        assert_eq!(state.input.content, ":kill ");
    }

    #[test]
    fn tab_completes_bare_job_path() {
        let original_dir = std::env::current_dir().unwrap();
        std::env::set_current_dir(env!("CARGO_MANIFEST_DIR")).unwrap();
        let mut state = AppState::new();
        state.input.insert_text("src/app.r");

        state.update(AppMsg::KeyEvent(KeyEvent::new(
            KeyCode::Tab,
            KeyModifiers::NONE,
        )));

        assert_eq!(state.input.content, "src/app.rs ");
        std::env::set_current_dir(original_dir).unwrap();
    }

    #[test]
    fn tab_completes_bare_cron_command_path() {
        let original_dir = std::env::current_dir().unwrap();
        std::env::set_current_dir(env!("CARGO_MANIFEST_DIR")).unwrap();
        let mut state = AppState::new();
        state.mode = Mode::Cron;
        state.sync_mode_views();
        state.input.insert_text("daily src/app.r");

        state.update(AppMsg::KeyEvent(KeyEvent::new(
            KeyCode::Tab,
            KeyModifiers::NONE,
        )));

        assert_eq!(state.input.content, "daily src/app.rs ");
        std::env::set_current_dir(original_dir).unwrap();
    }

    #[test]
    fn shift_tab_switches_mode_from_sidebar_focus() {
        let mut state = AppState::new();
        state.set_focus(FocusArea::Sidebar);

        state.update(AppMsg::KeyEvent(KeyEvent::new(
            KeyCode::BackTab,
            KeyModifiers::SHIFT,
        )));

        assert_eq!(state.mode, Mode::Cron);
        assert_eq!(state.focus, FocusArea::Sidebar);
    }

    #[test]
    fn shift_tab_switches_mode_from_main_view_focus() {
        let mut state = AppState::new();
        state.set_focus(FocusArea::MainView);

        state.update(AppMsg::KeyEvent(KeyEvent::new(
            KeyCode::BackTab,
            KeyModifiers::SHIFT,
        )));

        assert_eq!(state.mode, Mode::Cron);
        assert_eq!(state.focus, FocusArea::MainView);
    }

    #[test]
    fn fg_attach_without_precreated_card_opens_display_and_session() {
        let mut state = AppState::new();
        queue_pending(
            &mut state,
            1,
            PendingSubmission::user(None, ":fg J1".into(), Mode::Job, Vec::new()),
        );

        state.update(AppMsg::Response {
            id: 1,
            payload: ResponsePayload::Ok(OkPayload::FgAttached { id: "J1".into() }),
        });

        assert!(state.fg_active());
        assert_eq!(state.main_view.cards.len(), 1);
        let card = state.main_view.cards.last().unwrap();
        assert_eq!(card.input, ":fg J1");
        assert_eq!(card.label.as_deref(), Some("J1"));
        assert_eq!(card.status, CardStatus::Streaming);
    }

    #[test]
    fn switching_modes_filters_card_history() {
        let mut state = AppState::new();
        state.main_view.push_card("cargo test".into(), Mode::Job);
        state
            .main_view
            .push_card("every 5m cargo test".into(), Mode::Cron);

        assert_eq!(state.main_view.mode, Mode::Job);
        assert_eq!(
            state
                .main_view
                .cards
                .iter()
                .filter(|card| card.mode == state.main_view.mode)
                .count(),
            1
        );

        state.update(AppMsg::ModeSwitch);
        assert_eq!(state.main_view.mode, Mode::Cron);
        assert_eq!(
            state
                .main_view
                .cards
                .iter()
                .filter(|card| card.mode == state.main_view.mode)
                .count(),
            1
        );
    }

    #[test]
    fn offline_submit_does_not_stay_waiting() {
        let mut state = AppState::new();
        state.update(AppMsg::Submit("ls".into()));

        let card = state.main_view.cards.last().unwrap();
        assert_eq!(card.status, CardStatus::Error);
        assert!(card.output.contains("offline"));
    }

    #[test]
    fn restart_local_command_uses_restart_handle() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let mut state = AppState::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let shared = Arc::clone(&calls);
        state.set_restart_handle(Some(RestartHandle::new(move || {
            shared.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })));

        state.update(AppMsg::Submit(":restart".into()));

        let card = state.main_view.cards.last().expect("restart card");
        assert_eq!(card.input, ":restart");
        assert_eq!(card.status, CardStatus::Pending);
        assert!(card.output.contains("waiting for reconnect"));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn chain_created_does_not_overwrite_leaf_label() {
        let mut state = AppState::new();
        let card_index = state.main_view.push_card("sleep 4 -> ls".into(), Mode::Job);
        queue_pending(
            &mut state,
            1,
            PendingSubmission::user(
                Some(card_index),
                "sleep 4 -> ls".into(),
                Mode::Job,
                Vec::new(),
            ),
        );

        state.update(AppMsg::ServerEvent(EventPayload::JobCreated {
            job_id: "J1".into(),
            pipeline: "sleep 4".into(),
            start_scope: Some("S@abc12345".into()),
            open_hint: JobOpenHint::Stream,
            chain_id: None,
            chain_index: None,
            chain_total: None,
        }));
        state.update(AppMsg::Response {
            id: 1,
            payload: ResponsePayload::Ok(OkPayload::ChainCreated {
                chain_id: "C1".into(),
                job_ids: vec!["J1".into()],
                chain: cue_core::ipc::ChainInfo {
                    id: "C1".into(),
                    pipeline: "sleep 4 -> ls".into(),
                    total_jobs: 1,
                    jobs: vec![],
                },
            }),
        });

        assert_eq!(state.jobs[0].label, "sleep 4");
        assert_eq!(state.jobs[0].start_scope.as_deref(), Some("S@abc12345"));
        assert_eq!(state.main_view.cards.last().unwrap().output, "C1: J1");
    }

    #[test]
    fn missing_operator_spacing_warns_on_submit() {
        let mut state = AppState::new();
        state.update(AppMsg::Submit("sleep 4->ls".into()));

        let card = state.main_view.cards.last().unwrap();
        assert!(card.output.contains("missing spaces around `->`"));
        assert!(card.output.contains("sleep 4 -> ls"));
    }

    #[test]
    fn clear_display_clears_all_logs_when_idle() {
        let mut state = AppState::new();
        state.main_view.push_card("job".into(), Mode::Job);
        state.main_view.push_card("cron".into(), Mode::Cron);

        state.update(AppMsg::ClearDisplay);

        assert!(state.main_view.cards.is_empty());
    }

    #[test]
    fn clear_display_is_blocked_while_submission_is_pending() {
        let mut state = AppState::new();
        let card_index = state.main_view.push_card("sleep 4".into(), Mode::Job);
        queue_pending(
            &mut state,
            1,
            PendingSubmission::user(Some(card_index), "sleep 4".into(), Mode::Job, Vec::new()),
        );

        state.update(AppMsg::ClearDisplay);

        assert_eq!(
            state
                .main_view
                .cards
                .iter()
                .filter(|card| card.mode == Mode::Job)
                .count(),
            1
        );
    }

    #[test]
    fn silent_snapshot_responses_do_not_consume_user_cards() {
        let mut state = AppState::new();
        let card_index = state.main_view.push_card("sleep 4".into(), Mode::Job);
        queue_pending(&mut state, 1, PendingSubmission::silent());
        queue_pending(
            &mut state,
            2,
            PendingSubmission::user(Some(card_index), "sleep 4".into(), Mode::Job, Vec::new()),
        );

        state.update(AppMsg::Response {
            id: 1,
            payload: ResponsePayload::Ok(OkPayload::JobList(vec![])),
        });
        state.update(AppMsg::Response {
            id: 2,
            payload: ResponsePayload::Ok(OkPayload::JobCreated {
                job_id: "J1".into(),
                start_scope: None,
                open_hint: JobOpenHint::Stream,
                chain_id: None,
                chain_index: None,
                chain_total: None,
            }),
        });

        let card = &state.main_view.cards[card_index];
        assert_eq!(card.label.as_deref(), Some("J1"));
        assert_eq!(card.output, "J1\nstatus: running");
    }

    #[test]
    fn responses_match_pending_submissions_by_request_id() {
        let mut state = AppState::new();
        let card_index = state.main_view.push_card("sleep 4".into(), Mode::Job);
        queue_pending(
            &mut state,
            10,
            PendingSubmission::user(Some(card_index), "sleep 4".into(), Mode::Job, Vec::new()),
        );
        queue_pending(&mut state, 11, PendingSubmission::silent());

        state.update(AppMsg::Response {
            id: 11,
            payload: ResponsePayload::Ok(OkPayload::JobList(vec![])),
        });
        state.update(AppMsg::Response {
            id: 10,
            payload: ResponsePayload::Ok(OkPayload::JobCreated {
                job_id: "J1".into(),
                start_scope: None,
                open_hint: JobOpenHint::Stream,
                chain_id: None,
                chain_index: None,
                chain_total: None,
            }),
        });

        let card = &state.main_view.cards[card_index];
        assert_eq!(card.label.as_deref(), Some("J1"));
        assert_eq!(card.output, "J1\nstatus: running");
    }

    #[test]
    fn job_created_event_carries_start_scope_into_sidebar() {
        let mut state = AppState::new();

        state.update(AppMsg::ServerEvent(EventPayload::JobCreated {
            job_id: "J1".into(),
            pipeline: "sleep 4".into(),
            start_scope: Some("S@abc12345".into()),
            open_hint: JobOpenHint::Stream,
            chain_id: None,
            chain_index: None,
            chain_total: None,
        }));

        assert_eq!(state.jobs.len(), 1);
        assert_eq!(state.jobs[0].label, "sleep 4");
        assert_eq!(state.jobs[0].start_scope.as_deref(), Some("S@abc12345"));
    }

    #[test]
    fn job_list_snapshot_preserves_start_scope() {
        let mut state = AppState::new();

        state.update(AppMsg::Response {
            id: 1,
            payload: ResponsePayload::Ok(OkPayload::JobList(vec![JobInfo {
                id: "J1".into(),
                status: JobStatus::Running,
                pipeline: "sleep 4".into(),
                exit_code: None,
                start_scope: Some("S@abc12345".into()),
                end_scope: None,
                open_hint: JobOpenHint::Stream,
                chain_id: None,
                chain_index: None,
                chain_total: None,
            }])),
        });

        assert_eq!(state.jobs.len(), 1);
        assert_eq!(state.jobs[0].start_scope.as_deref(), Some("S@abc12345"));
    }

    #[test]
    fn fg_key_bytes_use_application_cursor_sequences_when_enabled() {
        let key = KeyEvent::new(KeyCode::Up, KeyModifiers::NONE);
        assert_eq!(fg_key_bytes(key, false), Some(b"\x1b[A".to_vec()));
        assert_eq!(fg_key_bytes(key, true), Some(b"\x1bOA".to_vec()));
    }

    #[test]
    fn fg_paste_bytes_wrap_when_bracketed_paste_is_enabled() {
        assert_eq!(fg_paste_bytes("echo hi", false), b"echo hi".to_vec());
        assert_eq!(
            fg_paste_bytes("echo hi", true),
            b"\x1b[200~echo hi\x1b[201~".to_vec()
        );
    }

    #[test]
    fn fg_output_updates_terminal_modes_and_preserves_formatted_contents() {
        let mut state = AppState::new();
        queue_pending(
            &mut state,
            1,
            PendingSubmission::user(None, ":fg J1".into(), Mode::Job, Vec::new()),
        );
        state.update(AppMsg::Response {
            id: 1,
            payload: ResponsePayload::Ok(OkPayload::FgAttached { id: "J1".into() }),
        });
        state.update(AppMsg::ServerEvent(EventPayload::FgOutput {
            data: b"\x1b[?1049h\x1b[?1h\x1b[?2004h\x1b[31mhello\x1b[0m".to_vec(),
        }));

        let screen = state.fg_screen().unwrap();
        assert!(screen.alternate_screen());
        assert!(screen.application_cursor());
        assert!(screen.bracketed_paste());
        assert!(screen.contents().contains("hello"));

        state.update(AppMsg::ServerEvent(EventPayload::FgExited {
            id: "J1".into(),
            reason: "detached".into(),
        }));

        let card = state.main_view.cards.last().unwrap();
        assert_eq!(card.status, CardStatus::Success);
        assert!(card.output.contains("hello"));
        assert!(card.output.contains("\u{1b}[31m"));
    }
}
