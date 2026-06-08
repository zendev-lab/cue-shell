//! PTY allocation helpers (placeholder for future pty-based process spawning).
//!
//! V1 of the process manager uses `tokio::process::Command` with piped
//! stdout/stderr.  This module provides the low-level pty primitives for
//! a future V2 that will allocate a pseudo-terminal for each job.

use std::ffi::CStr;
use std::os::fd::{FromRawFd, OwnedFd};
use std::sync::Mutex;

use anyhow::{Result, bail};

/// Guard concurrent access to `ptsname()` which returns a pointer to a
/// process-global static buffer (not thread-safe).  On Linux we could
/// use `ptsname_r` instead, but macOS libc does not expose it.
static PTY_LOCK: Mutex<()> = Mutex::new(());

/// A master/slave PTY pair.
pub struct PtyPair {
    /// The master side — parent reads/writes here.
    pub master: OwnedFd,
    /// The slave side — child attaches as controlling terminal.
    pub slave: OwnedFd,
}

/// Allocate a new PTY pair via POSIX `posix_openpt` / `grantpt` / `unlockpt`.
///
/// # Safety
///
/// Calls into libc; returned file descriptors are wrapped in `OwnedFd` so they
/// will be closed on drop.
pub fn open_pty() -> Result<PtyPair> {
    // SAFETY: posix_openpt is a well-defined POSIX API.  We immediately wrap
    // the returned fd in OwnedFd so it will be closed on drop.
    let master_raw = unsafe { libc::posix_openpt(libc::O_RDWR | libc::O_NOCTTY) };
    if master_raw < 0 {
        bail!("posix_openpt failed: {}", std::io::Error::last_os_error());
    }

    // SAFETY: master_raw is valid; we own it exclusively.
    let master = unsafe { OwnedFd::from_raw_fd(master_raw) };

    // Grant access to the slave side.
    // SAFETY: grantpt operates on a valid master fd.
    let rc = unsafe { libc::grantpt(master_raw) };
    if rc != 0 {
        bail!("grantpt failed: {}", std::io::Error::last_os_error());
    }

    // Unlock the slave side.
    // SAFETY: unlockpt operates on a valid master fd.
    let rc = unsafe { libc::unlockpt(master_raw) };
    if rc != 0 {
        bail!("unlockpt failed: {}", std::io::Error::last_os_error());
    }

    // Get the slave device path.
    // SAFETY: ptsname returns a pointer to a process-global static buffer.
    // We hold PTY_LOCK while reading it and copy into an owned CString.
    let slave_cstring = {
        let _guard = PTY_LOCK.lock().unwrap();
        let ptr = unsafe { libc::ptsname(master_raw) };
        if ptr.is_null() {
            bail!("ptsname failed: {}", std::io::Error::last_os_error());
        }
        // Copy while lock is held so the static buffer cannot be overwritten.
        unsafe { CStr::from_ptr(ptr) }.to_owned()
    };

    // Open the slave side.
    // SAFETY: open() with a valid NUL-terminated path.
    let slave_raw = unsafe { libc::open(slave_cstring.as_ptr(), libc::O_RDWR | libc::O_NOCTTY) };
    if slave_raw < 0 {
        bail!(
            "open slave pty {} failed: {}",
            slave_cstring.to_string_lossy(),
            std::io::Error::last_os_error()
        );
    }

    // SAFETY: slave_raw is valid; we own it exclusively.
    let slave = unsafe { OwnedFd::from_raw_fd(slave_raw) };

    Ok(PtyPair { master, slave })
}

/// Query the slave device path for diagnostic / logging purposes.
///
/// Returns `None` if the fd is not a valid master pty.
#[cfg(test)]
pub fn slave_name(master: &OwnedFd) -> Option<String> {
    use std::os::fd::AsRawFd;
    let _guard = PTY_LOCK.lock().unwrap();
    // SAFETY: ptsname operates on a valid fd; guarded by PTY_LOCK.
    let ptr = unsafe { libc::ptsname(master.as_raw_fd()) };
    if ptr.is_null() {
        return None;
    }
    // Copy while lock is held.
    Some(
        unsafe { CStr::from_ptr(ptr) }
            .to_string_lossy()
            .into_owned(),
    )
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::os::fd::AsRawFd;

    use super::*;

    #[test]
    fn open_pty_creates_valid_pair() {
        let pair = open_pty().expect("open_pty should succeed");
        assert!(pair.master.as_raw_fd() >= 0);
        assert!(pair.slave.as_raw_fd() >= 0);
        assert_ne!(pair.master.as_raw_fd(), pair.slave.as_raw_fd());
    }

    #[test]
    fn pty_write_master_read_slave() {
        let pair = open_pty().expect("open_pty should succeed");

        // Write to master.
        let mut master = std::fs::File::from(pair.master);
        let mut slave = std::fs::File::from(pair.slave);

        master.write_all(b"hello\n").expect("write to master");
        master.flush().expect("flush master");

        // Read from slave (pty echoes input, so we read what was "typed").
        let mut buf = [0u8; 64];
        let n = slave.read(&mut buf).expect("read from slave");
        assert!(n > 0, "should have read something from slave");
        // PTY line discipline may transform \n → \r\n; just check content
        let output = String::from_utf8_lossy(&buf[..n]);
        assert!(
            output.contains("hello"),
            "slave output should contain 'hello', got: {output:?}"
        );
    }

    #[test]
    fn slave_name_returns_path() {
        let pair = open_pty().expect("open_pty should succeed");
        let name = slave_name(&pair.master);
        assert!(name.is_some(), "should return slave device path");
        let name = name.unwrap();
        assert!(
            name.contains("pty") || name.contains("pts") || name.contains("tty"),
            "slave name should contain pty, pts, or tty, got: {name}"
        );
    }
}
