use std::io::Write as _;
use std::os::fd::AsRawFd as _;
use std::os::unix::fs::OpenOptionsExt as _;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum LifecycleState {
    Starting = 0,
    Running = 1,
    Draining = 2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RestartOwnership {
    Supervisor,
    Standalone,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RestartCompletion {
    Completed,
    AlreadyCompleted,
    CancelledOrReplaced,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RestartArm {
    Armed,
    StopWon,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RestartArmRollback {
    Restored,
    StopWon,
    Replaced,
}

/// A successor helper that has completed process/runtime initialization and is
/// waiting for the parent to publish its durable restart fence.
pub(crate) trait RestartWatchdogHandoff {
    /// Publish COMMIT and wait until the child has verified the exact Armed
    /// ticket and replied ACTIVE.
    fn activate(&mut self) -> Result<()>;

    /// Stop and reap the helper. Callers must complete this before rolling an
    /// Armed fence back, otherwise the helper could race the rollback.
    fn terminate_and_reap(&mut self) -> Result<()>;

    /// Transfer ownership to the detached helper after ACTIVE.
    fn detach(&mut self);
}

impl RestartWatchdogHandoff for () {
    fn activate(&mut self) -> Result<()> {
        Ok(())
    }

    fn terminate_and_reap(&mut self) -> Result<()> {
        Ok(())
    }

    fn detach(&mut self) {}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RestartTicket {
    pub restart_id: String,
    pub daemon_instance_id: String,
    pub target_generation: String,
    pub first_request: bool,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RestartPhase {
    #[default]
    Armed,
    Cancelled,
    Completed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct RestartRecord {
    pub restart_id: String,
    pub daemon_instance_id: String,
    pub protocol_version: u32,
    pub target_generation: String,
    #[serde(default)]
    pub phase: RestartPhase,
    #[serde(default)]
    pub supervisor_restart: bool,
}

pub(crate) struct DaemonLifecycle {
    state: AtomicU8,
    socket_path: PathBuf,
    restart_ownership: RestartOwnership,
    restart: Mutex<Option<RestartRecord>>,
    startup_restart: Mutex<Option<RestartRecord>>,
    execution_ready: AtomicBool,
    execution_ready_tx: watch::Sender<bool>,
    startup_activation_tx: watch::Sender<bool>,
    restart_tx: watch::Sender<bool>,
    stop_tx: watch::Sender<bool>,
    drained_tx: watch::Sender<bool>,
    response_tx: watch::Sender<bool>,
    pending_response: Mutex<Option<(u64, u32)>>,
}

impl DaemonLifecycle {
    #[cfg(test)]
    pub(crate) fn new(socket_path: PathBuf, restart_ownership: RestartOwnership) -> Self {
        Self::new_with_startup(socket_path, restart_ownership, None)
    }

    pub(crate) fn new_with_startup(
        socket_path: PathBuf,
        restart_ownership: RestartOwnership,
        startup_restart: Option<RestartRecord>,
    ) -> Self {
        let initially_ready = startup_restart.is_none();
        let (execution_ready_tx, _) = watch::channel(initially_ready);
        let (restart_tx, _) = watch::channel(false);
        let (startup_activation_tx, _) = watch::channel(false);
        let (stop_tx, _) = watch::channel(false);
        let (drained_tx, _) = watch::channel(false);
        let (response_tx, _) = watch::channel(false);
        let state = if startup_restart.is_some() {
            LifecycleState::Starting
        } else {
            LifecycleState::Running
        };
        Self {
            state: AtomicU8::new(state as u8),
            socket_path,
            restart_ownership,
            restart: Mutex::new(None),
            startup_restart: Mutex::new(startup_restart),
            execution_ready: AtomicBool::new(initially_ready),
            execution_ready_tx,
            startup_activation_tx,
            restart_tx,
            stop_tx,
            drained_tx,
            response_tx,
            pending_response: Mutex::new(None),
        }
    }

    pub(crate) fn state(&self) -> LifecycleState {
        match self.state.load(Ordering::Acquire) {
            value if value == LifecycleState::Starting as u8 => LifecycleState::Starting,
            value if value == LifecycleState::Draining as u8 => LifecycleState::Draining,
            _ => LifecycleState::Running,
        }
    }

    pub(crate) fn is_starting(&self) -> bool {
        self.state() == LifecycleState::Starting
    }

    pub(crate) fn is_draining(&self) -> bool {
        self.state() == LifecycleState::Draining
    }

    pub(crate) fn execution_admission_closed(&self) -> bool {
        self.state() != LifecycleState::Running || !self.execution_ready.load(Ordering::Acquire)
    }

    pub(crate) fn is_execution_ready(&self) -> bool {
        self.execution_ready.load(Ordering::Acquire)
    }

    /// Close admission, durably fence one successor, and start exactly one
    /// fallback watchdog. Repeated requests are read-only and return the
    /// original ticket without creating helper waiters.
    pub(crate) fn request_restart(&self, client_id: u64, request_id: u32) -> Result<RestartTicket> {
        self.request_restart_with(client_id, request_id, |socket_path, record| {
            crate::cli::spawn_restart_successor(socket_path, record)
        })
    }

    fn request_restart_with<F, H>(
        &self,
        client_id: u64,
        request_id: u32,
        spawn_watchdog: F,
    ) -> Result<RestartTicket>
    where
        F: FnOnce(&Path, &RestartRecord) -> Result<H>,
        H: RestartWatchdogHandoff,
    {
        anyhow::ensure!(
            self.restart_ownership != RestartOwnership::Unknown,
            "cannot safely restart: service-manager ownership is unknown; daemon remains running"
        );
        let mut restart = self
            .restart
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(intent) = restart.as_ref() {
            return Ok(RestartTicket {
                restart_id: intent.restart_id.clone(),
                daemon_instance_id: intent.daemon_instance_id.clone(),
                target_generation: intent.target_generation.clone(),
                first_request: false,
            });
        }
        anyhow::ensure!(
            self.state() == LifecycleState::Running && self.is_execution_ready(),
            "cannot restart while a successor is still completing startup"
        );

        let intent = RestartRecord {
            restart_id: uuid::Uuid::new_v4().to_string(),
            daemon_instance_id: crate::daemon_instance_id().to_string(),
            protocol_version: cue_core::ipc::IPC_PROTOCOL_VERSION,
            target_generation: uuid::Uuid::new_v4().to_string(),
            phase: RestartPhase::Armed,
            supervisor_restart: self.restart_ownership == RestartOwnership::Supervisor,
        };
        let expected = self
            .startup_restart
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();

        // Avoid spawning a helper for a stop decision that is already durable.
        // This read is only a fast preflight; the locked CAS below remains the
        // authoritative stop-vs-restart linearization point.
        if read_restart_record(&self.socket_path)?
            .is_some_and(|record| record.phase == RestartPhase::Cancelled)
        {
            anyhow::bail!("restart cancelled: an explicit stop already won");
        }

        // The child has initialized and replied WAITING, but it cannot take
        // over until the parent publishes this exact durable fence and sends
        // COMMIT. This closes both the post-exec runtime-failure gap and the
        // old fixed-delay window when the parent is suspended before the CAS.
        let mut watchdog = spawn_watchdog(&self.socket_path, &intent)?;
        let arm = match arm_restart_record(&self.socket_path, expected.as_ref(), &intent) {
            Ok(arm) => arm,
            Err(error) => {
                let reap = watchdog.terminate_and_reap();
                return match reap {
                    Ok(()) => Err(error),
                    Err(reap_error) => Err(anyhow::anyhow!(
                        "{error:#}; successor helper could not be reaped after arm failure: {reap_error:#}"
                    )),
                };
            }
        };
        if arm == RestartArm::StopWon {
            let reap = watchdog.terminate_and_reap();
            match reap {
                Ok(()) => anyhow::bail!("restart cancelled: an explicit stop already won"),
                Err(error) => anyhow::bail!(
                    "restart cancelled: an explicit stop already won; successor helper could not be reaped: {error:#}"
                ),
            }
        }

        if let Err(activation_error) = watchdog.activate() {
            // Do not restore the previous fence while a child might still act
            // on Armed. A confirmed kill/wait is the rollback prerequisite.
            if let Err(reap_error) = watchdog.terminate_and_reap() {
                // The helper may still possess the Armed authority. Never
                // return to Running admission in that state: close admission
                // first, revoke the exact ticket when possible, then wake the
                // coordinated fail-stop path. Even if the tombstone write
                // fails, old and possible successor cannot execute together.
                let cancellation = self.fail_stop_unreaped_handoff(&intent);
                return match cancellation {
                    Ok(()) => Err(anyhow::anyhow!(
                        "successor handoff failed after restart arm: {activation_error:#}; helper could not be reaped, so its ticket was cancelled and the daemon is fail-stopping: {reap_error:#}"
                    )),
                    Err(cancel_error) => Err(anyhow::anyhow!(
                        "successor handoff failed after restart arm: {activation_error:#}; helper could not be reaped and its ticket could not be cancelled, so the daemon is fail-stopping with admission closed: {reap_error:#}; cancellation error: {cancel_error:#}"
                    )),
                };
            }
            let rollback = rollback_armed_restart(&self.socket_path, &intent, expected.as_ref())?;
            return match rollback {
                RestartArmRollback::Restored => Err(activation_error.context(
                    "successor handoff failed; the Armed restart fence was rolled back",
                )),
                RestartArmRollback::StopWon => Err(activation_error.context(
                    "successor handoff failed after an explicit stop won; cancellation was preserved",
                )),
                RestartArmRollback::Replaced => Err(activation_error.context(
                    "successor handoff failed and the restart fence was concurrently replaced; no rollback was attempted",
                )),
            };
        }
        watchdog.detach();
        self.state
            .store(LifecycleState::Draining as u8, Ordering::Release);
        self.drained_tx.send_replace(false);
        self.response_tx.send_replace(false);
        *self
            .pending_response
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some((client_id, request_id));

        *restart = Some(intent.clone());
        self.restart_tx.send_replace(true);
        Ok(RestartTicket {
            restart_id: intent.restart_id,
            daemon_instance_id: intent.daemon_instance_id,
            target_generation: intent.target_generation,
            first_request: true,
        })
    }

    fn fail_stop_unreaped_handoff(&self, armed: &RestartRecord) -> Result<()> {
        self.state
            .store(LifecycleState::Draining as u8, Ordering::Release);
        self.execution_ready.store(false, Ordering::Release);
        self.execution_ready_tx.send_replace(false);
        let cancellation = cancel_matching_armed_restart(&self.socket_path, armed);
        self.stop_tx.send_replace(true);
        cancellation
    }

    pub(crate) fn cancel_restart_for_shutdown(&self) -> Result<()> {
        self.cancel_restart_fence()?;
        // Admission remains closed: an explicit stop must win without a late
        // execution window before coordinated teardown reaches the scheduler.
        self.state
            .store(LifecycleState::Draining as u8, Ordering::Release);
        Ok(())
    }

    pub(crate) fn fail_stop_restart(&self) -> Result<()> {
        self.cancel_restart_for_shutdown()?;
        self.stop_tx.send_replace(true);
        Ok(())
    }

    fn cancel_restart_fence(&self) -> Result<()> {
        let mut restart = self
            .restart
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut cancelled = false;
        if let Some(intent) = restart.as_mut() {
            intent.phase = RestartPhase::Cancelled;
            write_restart_record(&self.socket_path, intent)?;
            cancelled = true;
        } else {
            let mut startup_restart = self
                .startup_restart
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(intent) = startup_restart.as_mut() {
                intent.phase = RestartPhase::Cancelled;
                write_restart_record(&self.socket_path, intent)?;
                cancelled = true;
            }
        }
        if !cancelled {
            cancel_restart_intent_for_stop(&self.socket_path)?;
        }
        self.restart_tx.send_replace(false);
        Ok(())
    }

    pub(crate) fn mark_drained(&self) {
        if self.is_draining() {
            self.drained_tx.send_replace(true);
        }
    }

    pub(crate) fn mark_restart_response_complete(&self, client_id: u64, request_id: u32) {
        let mut pending = self
            .pending_response
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.is_draining() && *pending == Some((client_id, request_id)) {
            *pending = None;
            self.response_tx.send_replace(true);
        }
    }

    pub(crate) fn resolve_restart_response_disconnect(&self, client_id: u64) {
        let pending = self
            .pending_response
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .to_owned();
        if let Some((pending_client, request_id)) = pending
            && pending_client == client_id
        {
            self.mark_restart_response_complete(client_id, request_id);
        }
    }

    pub(crate) fn supervisor_restart_requested(&self) -> bool {
        self.restart
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .is_some_and(|record| record.phase == RestartPhase::Armed && record.supervisor_restart)
    }

    pub(crate) fn mark_startup_restart_completed(&self) {
        if let Some(record) = self
            .startup_restart
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_mut()
        {
            record.phase = RestartPhase::Completed;
        }
        self.state
            .store(LifecycleState::Running as u8, Ordering::Release);
        self.startup_activation_tx.send_replace(true);
    }

    pub(crate) fn mark_startup_execution_ready(&self) {
        if self.state() == LifecycleState::Running {
            self.execution_ready.store(true, Ordering::Release);
            self.execution_ready_tx.send_replace(true);
        }
    }

    pub(crate) async fn wait_for_startup_activation(&self) {
        let mut rx = self.startup_activation_tx.subscribe();
        while !*rx.borrow_and_update() {
            if rx.changed().await.is_err() {
                return;
            }
        }
    }

    pub(crate) async fn wait_for_execution_ready(&self) {
        let mut rx = self.execution_ready_tx.subscribe();
        while !*rx.borrow_and_update() {
            if rx.changed().await.is_err() {
                return;
            }
        }
    }

    pub(crate) async fn wait_for_restart(&self) {
        let mut rx = self.restart_tx.subscribe();
        while !*rx.borrow_and_update() {
            if rx.changed().await.is_err() {
                return;
            }
        }
    }

    pub(crate) async fn wait_for_stop(&self) {
        let mut rx = self.stop_tx.subscribe();
        while !*rx.borrow_and_update() {
            if rx.changed().await.is_err() {
                return;
            }
        }
    }

    pub(crate) async fn wait_for_handoff_ready(&self) {
        let mut drained = self.drained_tx.subscribe();
        let mut response = self.response_tx.subscribe();
        loop {
            if *drained.borrow_and_update() && *response.borrow_and_update() {
                return;
            }
            tokio::select! {
                changed = drained.changed() => {
                    if changed.is_err() {
                        return;
                    }
                }
                changed = response.changed() => {
                    if changed.is_err() {
                        return;
                    }
                }
            }
        }
    }
}

fn restart_intent_path(socket_path: &Path) -> PathBuf {
    let mut path = socket_path.as_os_str().to_os_string();
    path.push(".cued.restart.json");
    PathBuf::from(path)
}

fn restart_record_lock_path(socket_path: &Path) -> PathBuf {
    let mut path = socket_path.as_os_str().to_os_string();
    path.push(".cued.restart.state.lock");
    PathBuf::from(path)
}

fn acquire_restart_record_lock(socket_path: &Path) -> Result<std::fs::File> {
    let path = restart_record_lock_path(socket_path);
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&path)
        .with_context(|| format!("open restart record lock {}", path.display()))?;
    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };
    if result != 0 {
        return Err(std::io::Error::last_os_error())
            .with_context(|| format!("lock restart record {}", path.display()));
    }
    Ok(file)
}

pub(crate) fn write_restart_record(socket_path: &Path, intent: &RestartRecord) -> Result<()> {
    let _record_lock = acquire_restart_record_lock(socket_path)?;
    write_restart_record_unlocked(socket_path, intent)
}

fn write_restart_record_unlocked(socket_path: &Path, intent: &RestartRecord) -> Result<()> {
    let path = restart_intent_path(socket_path);
    let encoded = serde_json::to_vec(intent).context("encode daemon restart intent")?;
    let mut temp = path.as_os_str().to_os_string();
    temp.push(format!(
        ".tmp.{}.{}",
        std::process::id(),
        uuid::Uuid::new_v4()
    ));
    let temp = PathBuf::from(temp);
    let write_result = (|| -> Result<()> {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&temp)
            .with_context(|| format!("create restart intent temp file {}", temp.display()))?;
        file.write_all(&encoded)
            .with_context(|| format!("write restart intent temp file {}", temp.display()))?;
        file.sync_all()
            .with_context(|| format!("sync restart intent temp file {}", temp.display()))?;
        std::fs::rename(&temp, &path).with_context(|| {
            format!(
                "atomically install daemon restart intent {}",
                path.display()
            )
        })?;
        if let Some(parent) = path.parent()
            && let Ok(directory) = std::fs::File::open(parent)
        {
            let _ = directory.sync_all();
        }
        Ok(())
    })();
    if write_result.is_err() {
        let _ = std::fs::remove_file(&temp);
    }
    write_result
}

fn read_restart_record(socket_path: &Path) -> Result<Option<RestartRecord>> {
    let path = restart_intent_path(socket_path);
    let encoded = match std::fs::read(&path) {
        Ok(encoded) => encoded,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("read daemon restart intent {}", path.display()));
        }
    };
    serde_json::from_slice(&encoded)
        .with_context(|| format!("decode daemon restart intent {}", path.display()))
        .map(Some)
}

fn arm_restart_record(
    socket_path: &Path,
    expected: Option<&RestartRecord>,
    armed: &RestartRecord,
) -> Result<RestartArm> {
    let _record_lock = acquire_restart_record_lock(socket_path)?;
    let current = read_restart_record(socket_path)?;
    if current
        .as_ref()
        .is_some_and(|record| record.phase == RestartPhase::Cancelled)
    {
        return Ok(RestartArm::StopWon);
    }
    anyhow::ensure!(
        current.as_ref() == expected,
        "restart fence changed before arm; daemon remains running"
    );
    write_restart_record_unlocked(socket_path, armed)?;
    Ok(RestartArm::Armed)
}

fn rollback_armed_restart(
    socket_path: &Path,
    armed: &RestartRecord,
    previous: Option<&RestartRecord>,
) -> Result<RestartArmRollback> {
    let _record_lock = acquire_restart_record_lock(socket_path)?;
    let current = read_restart_record(socket_path)?;
    if current
        .as_ref()
        .is_some_and(|record| record.phase == RestartPhase::Cancelled)
    {
        return Ok(RestartArmRollback::StopWon);
    }
    if current.as_ref() != Some(armed) {
        return Ok(RestartArmRollback::Replaced);
    }
    if let Some(previous) = previous {
        write_restart_record_unlocked(socket_path, previous)?;
    } else {
        remove_restart_record_unlocked(socket_path)?;
    }
    Ok(RestartArmRollback::Restored)
}

fn cancel_matching_armed_restart(socket_path: &Path, armed: &RestartRecord) -> Result<()> {
    let _record_lock = acquire_restart_record_lock(socket_path)?;
    let Some(current) = read_restart_record(socket_path)? else {
        return Ok(());
    };
    if current.phase == RestartPhase::Cancelled || current != *armed {
        return Ok(());
    }
    let mut cancelled = current;
    cancelled.phase = RestartPhase::Cancelled;
    write_restart_record_unlocked(socket_path, &cancelled)
}

pub(crate) fn restart_intent_matches_ticket(
    socket_path: &Path,
    restart_id: &str,
    predecessor_instance_id: &str,
    target_generation: &str,
    protocol_version: u32,
) -> Result<bool> {
    Ok(restart_record_for_ticket(
        socket_path,
        restart_id,
        predecessor_instance_id,
        target_generation,
        protocol_version,
    )?
    .is_some())
}

pub(crate) fn restart_record_for_ticket(
    socket_path: &Path,
    restart_id: &str,
    predecessor_instance_id: &str,
    target_generation: &str,
    protocol_version: u32,
) -> Result<Option<RestartRecord>> {
    Ok(read_restart_record(socket_path)?.filter(|intent| {
        intent.phase != RestartPhase::Cancelled
            && intent.restart_id == restart_id
            && intent.daemon_instance_id == predecessor_instance_id
            && intent.target_generation == target_generation
            && intent.protocol_version == protocol_version
    }))
}

pub(crate) fn restart_record_for_startup(socket_path: &Path) -> Result<Option<RestartRecord>> {
    read_restart_record(socket_path)
}

pub(crate) fn restart_record_exists(socket_path: &Path) -> Result<bool> {
    Ok(read_restart_record(socket_path)?.is_some())
}

pub(crate) fn cancel_restart_intent_for_stop(socket_path: &Path) -> Result<bool> {
    let _record_lock = acquire_restart_record_lock(socket_path)?;
    let mut intent = read_restart_record(socket_path)?.unwrap_or_else(|| RestartRecord {
        restart_id: format!("stop-{}", uuid::Uuid::new_v4()),
        daemon_instance_id: String::new(),
        protocol_version: cue_core::ipc::IPC_PROTOCOL_VERSION,
        target_generation: String::new(),
        phase: RestartPhase::Cancelled,
        supervisor_restart: false,
    });
    intent.phase = RestartPhase::Cancelled;
    write_restart_record_unlocked(socket_path, &intent)?;
    Ok(true)
}

pub(crate) fn clear_cancelled_restart_record(socket_path: &Path) -> Result<bool> {
    let _record_lock = acquire_restart_record_lock(socket_path)?;
    let Some(record) = read_restart_record(socket_path)? else {
        return Ok(false);
    };
    if record.phase != RestartPhase::Cancelled {
        return Ok(false);
    }
    remove_restart_record_unlocked(socket_path)?;
    Ok(true)
}

pub(crate) fn complete_matching_armed_restart(
    socket_path: &Path,
    expected: &RestartRecord,
) -> Result<RestartCompletion> {
    let _record_lock = acquire_restart_record_lock(socket_path)?;
    let Some(mut record) = read_restart_record(socket_path)? else {
        return Ok(RestartCompletion::CancelledOrReplaced);
    };
    if record.restart_id != expected.restart_id
        || record.daemon_instance_id != expected.daemon_instance_id
        || record.target_generation != expected.target_generation
        || record.protocol_version != expected.protocol_version
    {
        return Ok(RestartCompletion::CancelledOrReplaced);
    }
    match record.phase {
        RestartPhase::Armed => {
            record.phase = RestartPhase::Completed;
            write_restart_record_unlocked(socket_path, &record)?;
            Ok(RestartCompletion::Completed)
        }
        RestartPhase::Completed => Ok(RestartCompletion::AlreadyCompleted),
        RestartPhase::Cancelled => Ok(RestartCompletion::CancelledOrReplaced),
    }
}

#[cfg(test)]
pub(crate) fn remove_matching_restart_record(socket_path: &Path, restart_id: &str) -> Result<()> {
    let _record_lock = acquire_restart_record_lock(socket_path)?;
    if read_restart_record(socket_path)?.is_none_or(|record| record.restart_id != restart_id) {
        return Ok(());
    }
    remove_restart_record_unlocked(socket_path)
}

fn remove_restart_record_unlocked(socket_path: &Path) -> Result<()> {
    let path = restart_intent_path(socket_path);
    match std::fs::remove_file(&path) {
        Ok(()) => {
            if let Some(parent) = path.parent()
                && let Ok(directory) = std::fs::File::open(parent)
            {
                let _ = directory.sync_all();
            }
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => {
            Err(error).with_context(|| format!("remove daemon restart intent {}", path.display()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_socket(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "cued-lifecycle-{name}-{}-{}.sock",
            std::process::id(),
            uuid::Uuid::new_v4()
        ))
    }

    fn armed_record(restart_id: &str) -> RestartRecord {
        RestartRecord {
            restart_id: restart_id.into(),
            daemon_instance_id: "instance-a".into(),
            protocol_version: cue_core::ipc::IPC_PROTOCOL_VERSION,
            target_generation: "generation-a".into(),
            phase: RestartPhase::Armed,
            supervisor_restart: false,
        }
    }

    #[test]
    fn restart_intent_removal_is_fenced_by_restart_id() {
        let socket = temp_socket("fence");
        let intent = armed_record("restart-a");
        write_restart_record(&socket, &intent).unwrap();

        remove_matching_restart_record(&socket, "restart-b").unwrap();
        assert!(restart_record_for_startup(&socket).unwrap().is_some());

        remove_matching_restart_record(&socket, "restart-a").unwrap();
        assert!(restart_record_for_startup(&socket).unwrap().is_none());
    }

    #[test]
    fn stop_cancels_pending_intent_without_a_socket() {
        let socket = temp_socket("stop-gap");
        let intent = armed_record("restart-stop");
        write_restart_record(&socket, &intent).unwrap();

        assert!(cancel_restart_intent_for_stop(&socket).unwrap());
        let cancelled = restart_record_for_startup(&socket)
            .unwrap()
            .expect("cancellation tombstone must remain durable");
        assert_eq!(cancelled.phase, RestartPhase::Cancelled);
        assert!(
            !restart_intent_matches_ticket(
                &socket,
                "restart-stop",
                "instance-a",
                "generation-a",
                cue_core::ipc::IPC_PROTOCOL_VERSION,
            )
            .unwrap()
        );
        assert!(cancel_restart_intent_for_stop(&socket).unwrap());
        assert!(clear_cancelled_restart_record(&socket).unwrap());
        assert!(restart_record_for_startup(&socket).unwrap().is_none());
    }

    #[test]
    fn exact_readiness_completion_cannot_remove_a_cancelled_tombstone() {
        let socket = temp_socket("completion-cancel-race");
        let intent = armed_record("restart-ready");
        write_restart_record(&socket, &intent).unwrap();
        cancel_restart_intent_for_stop(&socket).unwrap();

        assert_eq!(
            complete_matching_armed_restart(&socket, &intent).unwrap(),
            RestartCompletion::CancelledOrReplaced
        );
        assert_eq!(
            restart_record_for_startup(&socket)
                .unwrap()
                .expect("completion must retain cancellation")
                .phase,
            RestartPhase::Cancelled
        );
        remove_matching_restart_record(&socket, &intent.restart_id).unwrap();

        let completed_socket = temp_socket("completion-idempotent");
        write_restart_record(&completed_socket, &intent).unwrap();
        assert_eq!(
            complete_matching_armed_restart(&completed_socket, &intent).unwrap(),
            RestartCompletion::Completed
        );
        assert_eq!(
            complete_matching_armed_restart(&completed_socket, &intent).unwrap(),
            RestartCompletion::AlreadyCompleted
        );
        assert_eq!(
            restart_record_for_startup(&completed_socket)
                .unwrap()
                .expect("completed readiness fence remains durable")
                .phase,
            RestartPhase::Completed
        );
        remove_matching_restart_record(&completed_socket, &intent.restart_id).unwrap();

        let missing_socket = temp_socket("completion-missing");
        assert_eq!(
            complete_matching_armed_restart(&missing_socket, &intent).unwrap(),
            RestartCompletion::CancelledOrReplaced
        );
    }

    #[test]
    fn restart_response_gate_resolves_only_the_first_accepting_request() {
        let lifecycle =
            DaemonLifecycle::new(temp_socket("response-loss"), RestartOwnership::Standalone);
        lifecycle
            .state
            .store(LifecycleState::Draining as u8, Ordering::Release);
        *lifecycle.pending_response.lock().unwrap() = Some((7, 42));

        lifecycle.mark_restart_response_complete(8, 9);
        assert!(!*lifecycle.response_tx.borrow());
        lifecycle.mark_restart_response_complete(7, 43);
        assert!(!*lifecycle.response_tx.borrow());

        lifecycle.resolve_restart_response_disconnect(8);
        assert!(!*lifecycle.response_tx.borrow());

        lifecycle.resolve_restart_response_disconnect(7);
        assert!(*lifecycle.response_tx.borrow());
    }

    #[test]
    fn repeated_restart_requests_spawn_exactly_one_watchdog() {
        let socket = temp_socket("single-watchdog");
        let lifecycle = DaemonLifecycle::new(socket.clone(), RestartOwnership::Standalone);
        let launches = std::sync::atomic::AtomicUsize::new(0);

        let first = lifecycle
            .request_restart_with(1, 10, |_, _| {
                launches.fetch_add(1, Ordering::Relaxed);
                Ok(())
            })
            .unwrap();
        for request_id in 11..111 {
            let repeated = lifecycle
                .request_restart_with(2, request_id, |_, _| {
                    launches.fetch_add(1, Ordering::Relaxed);
                    Ok(())
                })
                .unwrap();
            assert_eq!(repeated.restart_id, first.restart_id);
            assert!(!repeated.first_request);
        }

        assert_eq!(launches.load(Ordering::Relaxed), 1);
        remove_matching_restart_record(&socket, &first.restart_id).unwrap();
    }

    #[tokio::test]
    async fn begin_drain_ack_failure_is_cancelled_and_fail_stops() {
        let socket = temp_socket("begin-drain-fail-stop");
        let lifecycle = DaemonLifecycle::new(socket.clone(), RestartOwnership::Standalone);
        let ticket = lifecycle
            .request_restart_with(1, 10, |_, _| Ok(()))
            .unwrap();

        lifecycle.fail_stop_restart().unwrap();
        tokio::time::timeout(
            std::time::Duration::from_millis(100),
            lifecycle.wait_for_stop(),
        )
        .await
        .expect("main stop gate must not hang after BeginDrain acknowledgement loss");
        assert_eq!(lifecycle.state(), LifecycleState::Draining);
        let record = restart_record_for_startup(&socket)
            .unwrap()
            .expect("fail-stop must retain a durable tombstone");
        assert_eq!(record.restart_id, ticket.restart_id);
        assert_eq!(record.phase, RestartPhase::Cancelled);
        remove_matching_restart_record(&socket, &ticket.restart_id).unwrap();
    }

    #[test]
    fn supervisor_ownership_is_persisted_in_restart_record() {
        let socket = temp_socket("supervisor-owner");
        let lifecycle = DaemonLifecycle::new(socket.clone(), RestartOwnership::Supervisor);
        let ticket = lifecycle
            .request_restart_with(1, 10, |_, _| Ok(()))
            .unwrap();

        let record = restart_record_for_startup(&socket)
            .unwrap()
            .expect("restart record");
        assert!(record.supervisor_restart);
        assert!(lifecycle.supervisor_restart_requested());
        assert_eq!(record.restart_id, ticket.restart_id);
        remove_matching_restart_record(&socket, &ticket.restart_id).unwrap();
    }

    #[test]
    fn unknown_ownership_rejects_restart_before_admission_or_durable_changes() {
        let socket = temp_socket("unknown-owner");
        let lifecycle = DaemonLifecycle::new(socket.clone(), RestartOwnership::Unknown);
        let launches = std::sync::atomic::AtomicUsize::new(0);

        let error = lifecycle
            .request_restart_with(1, 10, |_, _| {
                launches.fetch_add(1, Ordering::Relaxed);
                Ok(())
            })
            .expect_err("ambiguous ownership must reject planned restart");

        assert!(format!("{error:#}").contains("ownership is unknown"));
        assert_eq!(lifecycle.state(), LifecycleState::Running);
        assert_eq!(launches.load(Ordering::Relaxed), 0);
        assert!(restart_record_for_startup(&socket).unwrap().is_none());
    }

    #[test]
    fn watchdog_spawn_failure_leaves_running_state_without_an_armed_intent() {
        let socket = temp_socket("watchdog-spawn-failure");
        let lifecycle = DaemonLifecycle::new(socket.clone(), RestartOwnership::Standalone);
        let launches = std::sync::atomic::AtomicUsize::new(0);

        let error = lifecycle
            .request_restart_with::<_, ()>(1, 10, |_, _| {
                launches.fetch_add(1, Ordering::Relaxed);
                anyhow::bail!("watchdog spawn failed")
            })
            .expect_err("a failed watchdog spawn must reject the restart");

        assert!(format!("{error:#}").contains("watchdog spawn failed"));
        assert_eq!(lifecycle.state(), LifecycleState::Running);
        assert!(lifecycle.is_execution_ready());
        assert_eq!(launches.load(Ordering::Relaxed), 1);
        assert!(restart_record_for_startup(&socket).unwrap().is_none());
    }

    struct FailingHandoff {
        socket: PathBuf,
        cancel_before_failure: bool,
        reap_fails: bool,
        events: std::sync::Arc<Mutex<Vec<&'static str>>>,
    }

    impl RestartWatchdogHandoff for FailingHandoff {
        fn activate(&mut self) -> Result<()> {
            self.events.lock().unwrap().push("activate");
            if self.cancel_before_failure {
                cancel_restart_intent_for_stop(&self.socket)?;
            }
            anyhow::bail!("ACTIVE acknowledgement failed")
        }

        fn terminate_and_reap(&mut self) -> Result<()> {
            self.events.lock().unwrap().push("reap");
            if self.reap_fails {
                anyhow::bail!("helper could not be reaped")
            }
            Ok(())
        }

        fn detach(&mut self) {
            self.events.lock().unwrap().push("detach");
        }
    }

    #[test]
    fn active_failure_reaps_helper_before_exact_arm_rollback() {
        let socket = temp_socket("active-failure-rollback");
        let lifecycle = DaemonLifecycle::new(socket.clone(), RestartOwnership::Standalone);
        let events = std::sync::Arc::new(Mutex::new(Vec::new()));
        let handoff_events = events.clone();
        let handoff_socket = socket.clone();

        let error = lifecycle
            .request_restart_with(1, 10, |_, _| {
                Ok(FailingHandoff {
                    socket: handoff_socket,
                    cancel_before_failure: false,
                    reap_fails: false,
                    events: handoff_events,
                })
            })
            .expect_err("missing ACTIVE must reject restart");

        assert!(format!("{error:#}").contains("rolled back"));
        assert_eq!(*events.lock().unwrap(), vec!["activate", "reap"]);
        assert_eq!(lifecycle.state(), LifecycleState::Running);
        assert!(lifecycle.is_execution_ready());
        assert!(restart_record_for_startup(&socket).unwrap().is_none());
    }

    #[test]
    fn stop_tombstone_wins_active_failure_rollback_race() {
        let socket = temp_socket("active-failure-stop-wins");
        let lifecycle = DaemonLifecycle::new(socket.clone(), RestartOwnership::Standalone);
        let events = std::sync::Arc::new(Mutex::new(Vec::new()));
        let handoff_events = events.clone();
        let handoff_socket = socket.clone();

        let error = lifecycle
            .request_restart_with(1, 10, |_, _| {
                Ok(FailingHandoff {
                    socket: handoff_socket,
                    cancel_before_failure: true,
                    reap_fails: false,
                    events: handoff_events,
                })
            })
            .expect_err("stop must cancel a handoff without ACTIVE");

        assert!(format!("{error:#}").contains("explicit stop won"));
        assert_eq!(*events.lock().unwrap(), vec!["activate", "reap"]);
        let cancelled = restart_record_for_startup(&socket).unwrap().unwrap();
        assert_eq!(cancelled.phase, RestartPhase::Cancelled);
        remove_matching_restart_record(&socket, &cancelled.restart_id).unwrap();
    }

    #[test]
    fn unreaped_active_failure_cancels_authority_and_fail_stops_closed_admission() {
        let socket = temp_socket("active-failure-unreaped");
        let lifecycle = DaemonLifecycle::new(socket.clone(), RestartOwnership::Standalone);
        let events = std::sync::Arc::new(Mutex::new(Vec::new()));
        let handoff_events = events.clone();
        let handoff_socket = socket.clone();

        let error = lifecycle
            .request_restart_with(1, 10, |_, _| {
                Ok(FailingHandoff {
                    socket: handoff_socket,
                    cancel_before_failure: false,
                    reap_fails: true,
                    events: handoff_events,
                })
            })
            .expect_err("an unreaped helper must fail-stop the old daemon");

        assert!(format!("{error:#}").contains("fail-stopping"));
        assert_eq!(*events.lock().unwrap(), vec!["activate", "reap"]);
        assert_eq!(lifecycle.state(), LifecycleState::Draining);
        assert!(!lifecycle.is_execution_ready());
        assert!(*lifecycle.stop_tx.borrow());
        let cancelled = restart_record_for_startup(&socket).unwrap().unwrap();
        assert_eq!(cancelled.phase, RestartPhase::Cancelled);
        remove_matching_restart_record(&socket, &cancelled.restart_id).unwrap();
    }

    #[test]
    fn completed_successor_rejects_nested_restart_until_execution_is_ready() {
        let socket = temp_socket("completed-not-ready");
        let startup = armed_record("completed-not-ready");
        let lifecycle = DaemonLifecycle::new_with_startup(
            socket.clone(),
            RestartOwnership::Standalone,
            Some(startup),
        );
        lifecycle.mark_startup_restart_completed();
        assert_eq!(lifecycle.state(), LifecycleState::Running);
        assert!(!lifecycle.is_execution_ready());
        let launches = std::sync::atomic::AtomicUsize::new(0);

        let error = lifecycle
            .request_restart_with(1, 10, |_, _| {
                launches.fetch_add(1, Ordering::Relaxed);
                Ok(())
            })
            .expect_err("a successor must not restart before execution activation");

        assert!(format!("{error:#}").contains("still completing startup"));
        assert_eq!(launches.load(Ordering::Relaxed), 0);
        assert!(restart_record_for_startup(&socket).unwrap().is_none());
    }

    #[test]
    fn stopping_a_ready_successor_cancels_its_completed_fence() {
        let socket = temp_socket("completed-stop");
        let mut completed = armed_record("completed-restart");
        completed.phase = RestartPhase::Completed;
        write_restart_record(&socket, &completed).unwrap();
        let lifecycle = DaemonLifecycle::new_with_startup(
            socket.clone(),
            RestartOwnership::Standalone,
            Some(completed.clone()),
        );

        lifecycle.cancel_restart_for_shutdown().unwrap();

        assert_eq!(
            restart_record_for_startup(&socket)
                .unwrap()
                .expect("stop must preserve its cancellation fence")
                .phase,
            RestartPhase::Cancelled
        );
        remove_matching_restart_record(&socket, &completed.restart_id).unwrap();
    }

    #[test]
    fn stop_before_restart_arm_wins_without_admission_or_watchdog_side_effects() {
        let socket = temp_socket("stop-before-arm");
        let lifecycle = DaemonLifecycle::new(socket.clone(), RestartOwnership::Standalone);
        let launches = std::sync::atomic::AtomicUsize::new(0);

        cancel_restart_intent_for_stop(&socket).unwrap();
        let error = lifecycle
            .request_restart_with(1, 10, |_, _| {
                launches.fetch_add(1, Ordering::Relaxed);
                Ok(())
            })
            .expect_err("stop-first ordering must reject restart arm");

        assert!(format!("{error:#}").contains("explicit stop already won"));
        assert_eq!(lifecycle.state(), LifecycleState::Running);
        assert_eq!(launches.load(Ordering::Relaxed), 0);
        let record = restart_record_for_startup(&socket).unwrap().unwrap();
        assert!(record.restart_id.starts_with("stop-"));
        assert_eq!(record.phase, RestartPhase::Cancelled);
        remove_matching_restart_record(&socket, &record.restart_id).unwrap();
    }

    #[test]
    fn restart_arm_before_stop_is_cancelled_without_rearming() {
        let socket = temp_socket("arm-before-stop");
        let lifecycle = DaemonLifecycle::new(socket.clone(), RestartOwnership::Standalone);
        let launches = std::sync::atomic::AtomicUsize::new(0);

        let ticket = lifecycle
            .request_restart_with(1, 10, |_, _| {
                launches.fetch_add(1, Ordering::Relaxed);
                Ok(())
            })
            .unwrap();
        cancel_restart_intent_for_stop(&socket).unwrap();

        assert_eq!(lifecycle.state(), LifecycleState::Draining);
        assert_eq!(launches.load(Ordering::Relaxed), 1);
        let record = restart_record_for_startup(&socket).unwrap().unwrap();
        assert_eq!(record.restart_id, ticket.restart_id);
        assert_eq!(record.phase, RestartPhase::Cancelled);
        remove_matching_restart_record(&socket, &record.restart_id).unwrap();
    }
}
