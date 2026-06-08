use anyhow::{Context, Result};
use crossterm::event::{DisableBracketedPaste, DisableMouseCapture, PopKeyboardEnhancementFlags};
use std::sync::{Arc, Mutex, MutexGuard};

type PanicHook = Box<dyn Fn(&std::panic::PanicHookInfo<'_>) + Sync + Send + 'static>;

pub(crate) struct PanicHookGuard {
    original_hook: Arc<Mutex<Option<PanicHook>>>,
}

impl PanicHookGuard {
    pub(crate) fn install(keyboard_enhancements_enabled: bool) -> Self {
        let original_hook = Arc::new(Mutex::new(Some(std::panic::take_hook())));
        let hook_original = Arc::clone(&original_hook);
        std::panic::set_hook(Box::new(move |info| {
            restore_terminal_input_modes_lossy(keyboard_enhancements_enabled);
            let guard = original_hook_guard(&hook_original);
            if let Some(original_hook) = guard.as_ref() {
                original_hook(info);
            }
        }));

        Self { original_hook }
    }
}

impl Drop for PanicHookGuard {
    fn drop(&mut self) {
        if std::thread::panicking() {
            return;
        }

        let mut guard = original_hook_guard(&self.original_hook);
        if let Some(original_hook) = guard.take() {
            std::panic::set_hook(original_hook);
        }
    }
}

pub(crate) struct TerminalRestoreGuard {
    keyboard_enhancements_enabled: bool,
    restored: bool,
}

impl TerminalRestoreGuard {
    pub(crate) fn new() -> Self {
        Self {
            keyboard_enhancements_enabled: false,
            restored: false,
        }
    }

    pub(crate) fn set_keyboard_enhancements_enabled(&mut self, enabled: bool) {
        self.keyboard_enhancements_enabled = enabled;
    }

    pub(crate) fn restore(&mut self) -> Result<()> {
        if self.restored {
            return Ok(());
        }

        let result = restore_terminal_input_modes(self.keyboard_enhancements_enabled);
        ratatui::restore();
        self.restored = true;
        result
    }
}

impl Drop for TerminalRestoreGuard {
    fn drop(&mut self) {
        if self.restored {
            return;
        }

        restore_terminal_input_modes_lossy(self.keyboard_enhancements_enabled);
        ratatui::restore();
        self.restored = true;
    }
}

pub(crate) fn initial_terminal_size(
    read_size: impl FnOnce() -> std::io::Result<(u16, u16)>,
) -> Result<(u16, u16)> {
    read_size().context("read terminal size")
}

fn restore_terminal_input_modes(keyboard_enhancements_enabled: bool) -> Result<()> {
    crossterm::execute!(
        std::io::stdout(),
        DisableMouseCapture,
        DisableBracketedPaste
    )
    .context("restore terminal input modes")?;
    if keyboard_enhancements_enabled {
        crossterm::execute!(std::io::stdout(), PopKeyboardEnhancementFlags)
            .context("disable keyboard enhancements")?;
    }
    Ok(())
}

fn restore_terminal_input_modes_lossy(keyboard_enhancements_enabled: bool) {
    let _ = restore_terminal_input_modes(keyboard_enhancements_enabled);
}

fn original_hook_guard(slot: &Mutex<Option<PanicHook>>) -> MutexGuard<'_, Option<PanicHook>> {
    slot.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    #[test]
    fn initial_terminal_size_uses_reported_size() {
        let size = initial_terminal_size(|| Ok((100, 40))).expect("terminal size should load");

        assert_eq!(size, (100, 40));
    }

    #[test]
    fn initial_terminal_size_reports_read_failure() {
        let error = initial_terminal_size(|| {
            Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "terminal unavailable",
            ))
        })
        .expect_err("terminal size failure should not fall back to a fake size");

        let message = format!("{error:#}");
        assert!(message.contains("read terminal size"));
        assert!(message.contains("terminal unavailable"));
    }
}
