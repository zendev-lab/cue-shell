use ansi_to_tui::IntoText as _;
use ratatui::text::Text;

pub(crate) fn to_text(output: &str) -> Text<'static> {
    output
        .as_bytes()
        .into_text()
        .unwrap_or_else(|_| Text::from(output.to_string()))
}
