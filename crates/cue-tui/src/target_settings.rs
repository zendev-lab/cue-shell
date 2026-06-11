use crate::target_config::{
    TargetProfileKind, TargetProfileSource, TargetProfileSummary, TargetSettingsSnapshot,
    display_path,
};

#[derive(Debug, Clone)]
pub(crate) struct TargetSettingsState {
    pub(crate) snapshot: TargetSettingsSnapshot,
    pub(crate) selected: usize,
    pub(crate) notice: Option<String>,
    /// Profile name waiting for an R-key reconnect trigger.
    /// `None` when no pending live-reconnect is available.
    pub(crate) pending_reconnect_profile: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct TargetSettingsView {
    pub(crate) content: String,
    pub(crate) profile_line_rows: Vec<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TargetProfileSaveFeedback {
    pub(crate) notice: String,
    pub(crate) pending_reconnect: bool,
}

impl TargetSettingsState {
    pub(crate) fn new(snapshot: TargetSettingsSnapshot) -> Self {
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

    pub(crate) fn with_notice(snapshot: TargetSettingsSnapshot, notice: String) -> Self {
        let mut state = Self::new(snapshot);
        state.notice = Some(notice);
        state
    }

    pub(crate) fn move_selection(&mut self, delta: isize) {
        if self.snapshot.profiles.is_empty() {
            self.selected = 0;
            return;
        }
        let max = self.snapshot.profiles.len().saturating_sub(1) as isize;
        let next = (self.selected as isize + delta).clamp(0, max);
        self.selected = next as usize;
    }

    pub(crate) fn selected_profile_name(&self) -> Option<&str> {
        self.snapshot
            .profiles
            .get(self.selected)
            .map(|profile| profile.name.as_str())
    }

    pub(crate) fn selected_profile(&self) -> Option<&TargetProfileSummary> {
        self.snapshot.profiles.get(self.selected)
    }

    pub(crate) fn select_first(&mut self) {
        self.selected = 0;
    }

    pub(crate) fn select_last(&mut self) {
        self.selected = self.snapshot.profiles.len().saturating_sub(1);
    }

    pub(crate) fn select_profile_name(&mut self, profile_name: &str) {
        if let Some(index) = self
            .snapshot
            .profiles
            .iter()
            .position(|profile| profile.name == profile_name)
        {
            self.selected = index;
        }
    }

    pub(crate) fn select_index(&mut self, index: usize) {
        if index < self.snapshot.profiles.len() {
            self.selected = index;
        }
    }
}

pub(crate) fn format_target_settings_view(
    state: &TargetSettingsState,
    session_profile_name: Option<&str>,
) -> TargetSettingsView {
    let mut lines = vec![
        format!("source: {}", display_path(&state.snapshot.source_path)),
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

fn target_profile_alert(profile: &TargetProfileSummary) -> Option<&'static str> {
    match &profile.transport {
        TargetProfileKind::Missing => Some("missing"),
        TargetProfileKind::Invalid | TargetProfileKind::Unsupported(_) => Some("invalid"),
        TargetProfileKind::Unix | TargetProfileKind::Ssh => None,
    }
}

fn target_profile_source_tag(profile: &TargetProfileSummary) -> Option<&'static str> {
    match profile.source {
        TargetProfileSource::Local => Some("permanent"),
        TargetProfileSource::Configured => Some("configured"),
        TargetProfileSource::AutoDetectedSsh => Some("auto"),
        TargetProfileSource::Missing => None,
    }
}

pub(crate) fn target_profile_supports_live_reconnect(profile: &TargetProfileSummary) -> bool {
    profile.is_usable_target()
}

pub(crate) fn target_profile_can_be_saved(profile: &TargetProfileSummary) -> bool {
    profile.is_usable_target()
}

pub(crate) fn saved_target_profile_feedback(
    snapshot: &TargetSettingsSnapshot,
    profile_name: &str,
    session_profile_name: Option<&str>,
    live_reconnect_available: bool,
) -> TargetProfileSaveFeedback {
    let source = display_path(&snapshot.source_path);
    if session_profile_name == Some(profile_name) {
        return TargetProfileSaveFeedback {
            notice: format!(
                "saved default profile `{profile_name}` to {source}; current session already uses it"
            ),
            pending_reconnect: false,
        };
    }

    match session_profile_name {
        Some(current_session) if live_reconnect_available => TargetProfileSaveFeedback {
            notice: format!(
                "saved default profile `{profile_name}` to {source}; current session still uses `{current_session}`. Press R to reconnect now"
            ),
            pending_reconnect: true,
        },
        Some(current_session) => TargetProfileSaveFeedback {
            notice: format!(
                "saved default profile `{profile_name}` to {source}; current session still uses `{current_session}` until reconnect/restart"
            ),
            pending_reconnect: false,
        },
        None => TargetProfileSaveFeedback {
            notice: format!(
                "saved default profile `{profile_name}` to {source}; reconnect/restart cue to apply"
            ),
            pending_reconnect: false,
        },
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    fn test_profile(
        name: &str,
        transport: TargetProfileKind,
        detail: &str,
        source: TargetProfileSource,
    ) -> TargetProfileSummary {
        TargetProfileSummary {
            name: name.into(),
            transport,
            detail: detail.into(),
            source,
        }
    }

    fn test_snapshot(
        path: impl Into<PathBuf>,
        default_profile: &str,
        profiles: Vec<TargetProfileSummary>,
    ) -> TargetSettingsSnapshot {
        TargetSettingsSnapshot {
            source_path: path.into(),
            auto_detect_ssh: true,
            default_profile: default_profile.into(),
            profiles,
        }
    }

    #[test]
    fn ssh_target_profile_supports_live_reconnect_without_alerts() {
        let profile = test_profile(
            "remote",
            TargetProfileKind::Ssh,
            "devbox | cued gateway --stdio",
            TargetProfileSource::Configured,
        );

        assert!(target_profile_supports_live_reconnect(&profile));
        let state =
            TargetSettingsState::new(test_snapshot("/tmp/client.toml", "remote", vec![profile]));
        let view = format_target_settings_view(&state, Some("local"));

        assert!(view.content.contains("> remote (ssh)"));
        assert!(!view.content.contains("invalid"));
        assert!(!view.content.contains("missing"));
    }

    #[test]
    fn missing_target_profiles_are_marked_in_profile_tags() {
        let state = TargetSettingsState::new(test_snapshot(
            "/tmp/client.toml",
            "remote",
            vec![test_profile(
                "remote",
                TargetProfileKind::Missing,
                "profile is referenced by default_profile but not defined",
                TargetProfileSource::Missing,
            )],
        ));

        let view = format_target_settings_view(&state, Some("local"));

        assert!(view.content.contains("[default, selected, missing]"));
    }

    #[test]
    fn saved_target_profile_feedback_offers_live_reconnect_when_session_differs() {
        let snapshot = test_snapshot("/tmp/client.toml", "remote", Vec::new());

        let feedback = saved_target_profile_feedback(&snapshot, "remote", Some("local"), true);

        assert!(
            feedback
                .notice
                .contains("current session still uses `local`")
        );
        assert!(feedback.notice.contains("Press R to reconnect now"));
        assert!(feedback.pending_reconnect);
    }

    #[test]
    fn saved_target_profile_feedback_does_not_reconnect_current_session_target() {
        let snapshot = test_snapshot("/tmp/client.toml", "remote", Vec::new());

        let feedback = saved_target_profile_feedback(&snapshot, "remote", Some("remote"), true);

        assert!(feedback.notice.contains("current session already uses it"));
        assert!(!feedback.pending_reconnect);
    }

    #[test]
    fn saved_target_profile_feedback_handles_sessions_without_live_reconnect() {
        let snapshot = test_snapshot("/tmp/client.toml", "remote", Vec::new());

        let feedback = saved_target_profile_feedback(&snapshot, "remote", Some("local"), false);

        assert!(feedback.notice.contains("until reconnect/restart"));
        assert!(!feedback.pending_reconnect);
    }
}
