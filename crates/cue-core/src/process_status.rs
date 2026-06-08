use std::process::ExitStatus;

/// Convert an OS child status into the numeric exit code cue-shell reports.
///
/// Normal exit statuses keep their process-provided code. On Unix, signal
/// termination follows the shell convention `128 + signal`. Rare platform
/// statuses that expose neither form use the caller-provided fallback because
/// jobs and CLI extensions have different unavailable-code semantics.
pub fn exit_code_from_status(status: ExitStatus, unavailable_exit_code: i32) -> i32 {
    if let Some(code) = status.code() {
        return code;
    }

    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;

        if let Some(signal) = status.signal() {
            return 128 + signal;
        }
    }

    unavailable_exit_code
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn preserves_normal_exit_code() {
        use std::os::unix::process::ExitStatusExt;

        let status = ExitStatus::from_raw(7 << 8);

        assert_eq!(exit_code_from_status(status, -1), 7);
    }

    #[cfg(unix)]
    #[test]
    fn maps_signal_to_shell_exit_code() {
        use std::os::unix::process::ExitStatusExt;

        const SIGTERM: i32 = 15;
        let status = ExitStatus::from_raw(SIGTERM);

        assert_eq!(exit_code_from_status(status, -1), 128 + SIGTERM);
    }
}
