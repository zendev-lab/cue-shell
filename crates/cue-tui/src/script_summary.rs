use cue_core::ipc::{
    ScriptItemInfo, ScriptItemResult, ScriptRunStatus, ScriptSource, ScriptSubmitError,
};

pub(crate) fn summarize_source(source: &str) -> String {
    let compact = source.split_whitespace().collect::<Vec<_>>().join(" ");
    truncate_text(&compact, 96)
}

fn truncate_text(text: &str, max_chars: usize) -> String {
    let mut chars = text.chars();
    let truncated: String = chars.by_ref().take(max_chars).collect();
    if chars.next().is_some() {
        format!("{truncated}...")
    } else {
        truncated
    }
}

pub(crate) fn format_submission(
    source: &ScriptSource,
    items: &[ScriptItemInfo],
    submit_error: Option<&ScriptSubmitError>,
) -> String {
    let mut lines = vec![format!("submitted {} item(s)", items.len())];
    if let ScriptSource::File { path } = source {
        lines.push(format!("source: {path}"));
    }
    for item in items {
        lines.push(format!(
            "{}. {} -> {}",
            item.index + 1,
            summarize_source(&item.source),
            format_item_result(&item.result),
        ));
    }
    if let Some(error) = submit_error {
        lines.push(String::new());
        lines.push(format!(
            "submit stopped at {}. {} [{}]: {}",
            error.index + 1,
            summarize_source(&error.source),
            error.code,
            error.message,
        ));
    }
    lines.join("\n")
}

pub(crate) fn format_finished(
    status: ScriptRunStatus,
    exit_code: i32,
    failed_item_index: Option<usize>,
) -> String {
    let mut text = format!("\nscript finished: {status:?}, exit={exit_code}");
    if let Some(index) = failed_item_index {
        text.push_str(&format!(" (failed at item {})", index + 1));
    }
    text
}

pub(crate) fn format_item_created(item: &ScriptItemInfo) -> String {
    format!(
        "\n{}. {} -> {}",
        item.index + 1,
        summarize_source(&item.source),
        format_item_result(&item.result),
    )
}

fn format_item_result(result: &ScriptItemResult) -> String {
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
        ScriptItemResult::Message { text } => summarize_source(text),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn submission_format_includes_file_source() {
        let text = format_submission(
            &ScriptSource::File {
                path: "scripts/build.cue".into(),
            },
            &[],
            None,
        );
        assert!(text.contains("submitted 0 item(s)"));
        assert!(text.contains("source: scripts/build.cue"));
    }

    #[test]
    fn finished_format_includes_exit_and_failed_item() {
        let text = format_finished(ScriptRunStatus::Failed, 2, Some(1));
        assert!(text.contains("exit=2"));
        assert!(text.contains("failed at item 2"));
    }

    #[test]
    fn source_summary_compacts_and_truncates_whitespace() {
        let source = "echo    one\n  two";
        assert_eq!(summarize_source(source), "echo one two");

        let long = "x".repeat(100);
        assert_eq!(summarize_source(&long), format!("{}...", "x".repeat(96)));
    }
}
