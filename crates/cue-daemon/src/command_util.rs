use std::ffi::{OsStr, OsString};
use std::process::{Command, Output};

use anyhow::{Context, Result};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CommandSpec {
    program: OsString,
    args: Vec<OsString>,
}

impl CommandSpec {
    pub(crate) fn new(program: impl AsRef<OsStr>) -> Self {
        Self {
            program: program.as_ref().to_os_string(),
            args: Vec::new(),
        }
    }

    pub(crate) fn arg(mut self, arg: impl AsRef<OsStr>) -> Self {
        self.args.push(arg.as_ref().to_os_string());
        self
    }

    pub(crate) fn args<I, S>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        self.args
            .extend(args.into_iter().map(|arg| arg.as_ref().to_os_string()));
        self
    }

    pub(crate) fn output(&self) -> Result<Output> {
        Command::new(&self.program)
            .args(&self.args)
            .output()
            .with_context(|| format!("run `{}`", self.display()))
    }

    pub(crate) fn display(&self) -> String {
        std::iter::once(&self.program)
            .chain(self.args.iter())
            .map(|value| shell_quote(&value.to_string_lossy()))
            .collect::<Vec<_>>()
            .join(" ")
    }

    pub(crate) fn failure_summary(&self, output: &Output) -> String {
        let mut summary = format!("`{}` exited with status {}", self.display(), output.status);
        append_command_output(&mut summary, "stderr", &output.stderr);
        append_command_output(&mut summary, "stdout", &output.stdout);
        summary
    }
}

fn shell_quote(value: &str) -> String {
    if !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_./:=@+".contains(&byte))
    {
        value.to_string()
    } else {
        format!("'{}'", value.replace('\'', "'\\''"))
    }
}

fn append_command_output(summary: &mut String, label: &str, data: &[u8]) {
    let text = String::from_utf8_lossy(data);
    let text = text.trim();
    if !text.is_empty() {
        summary.push_str(&format!("\n{label}: {text}"));
    }
}

#[cfg(test)]
mod tests {
    use std::process::Output;

    use super::*;

    #[test]
    fn display_quotes_shell_sensitive_args() {
        assert_eq!(
            CommandSpec::new("launchctl")
                .args(["bootstrap", "gui/501", "/tmp/path with spaces/cued.plist"])
                .display(),
            "launchctl bootstrap gui/501 '/tmp/path with spaces/cued.plist'"
        );
        assert_eq!(
            CommandSpec::new("cmd").args(["it's", "plain"]).display(),
            "cmd 'it'\\''s' plain"
        );
    }

    #[cfg(unix)]
    #[test]
    fn failure_summary_includes_status_and_output() {
        use std::os::unix::process::ExitStatusExt;

        let output = Output {
            status: std::process::ExitStatus::from_raw(7 << 8),
            stdout: b"stdout note\n".to_vec(),
            stderr: b"stderr reason\n".to_vec(),
        };

        let summary = CommandSpec::new("systemctl")
            .args(["--user", "restart", "cued"])
            .failure_summary(&output);

        assert!(
            summary.contains("systemctl --user restart cued"),
            "{summary}"
        );
        assert!(summary.contains("exit status: 7"), "{summary}");
        assert!(summary.contains("stderr: stderr reason"), "{summary}");
        assert!(summary.contains("stdout: stdout note"), "{summary}");
    }
}
