//! Bounded daemon-lifetime idempotency ledger for side-effecting IPC requests.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use serde::Serialize;

use cue_core::ipc::{ResponsePayload, error_code};

pub(super) const MAX_OPERATION_ID_BYTES: usize = 128;
const DEFAULT_IDENTITY_CAPACITY: usize = 65_536;
const DEFAULT_RESPONSE_CAPACITY: usize = 1_024;
const DEFAULT_COMPLETED_TTL: Duration = Duration::from_secs(15 * 60);
const DEFAULT_MAX_WAITERS: usize = 4;
// Keep replay entries well below the 16 MiB framed-message ceiling so the
// response envelope and JSON escaping cannot push a cached payload over it.
const DEFAULT_MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;
const DEFAULT_MAX_COMPLETED_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct OperationWaiter {
    pub client_id: u64,
    pub request_id: u32,
}

#[derive(Debug)]
pub(super) enum BeginOutcome {
    /// This caller owns the first execution and must route the request.
    Route,
    /// An identical execution is already pending; its response will be fanned out.
    Wait,
    /// Return a cached response or a deterministic ledger rejection immediately.
    Respond(Box<ResponsePayload>),
}

impl BeginOutcome {
    pub(super) fn respond(response: ResponsePayload) -> Self {
        Self::Respond(Box::new(response))
    }
}

#[derive(Debug)]
pub(super) struct CompletedOperation {
    pub waiters: Vec<OperationWaiter>,
    pub response: ResponsePayload,
}

#[derive(Debug, Clone, Copy)]
struct LedgerLimits {
    identity_capacity: usize,
    response_capacity: usize,
    completed_ttl: Duration,
    max_waiters: usize,
    max_response_bytes: usize,
    max_completed_bytes: usize,
}

impl Default for LedgerLimits {
    fn default() -> Self {
        Self {
            identity_capacity: DEFAULT_IDENTITY_CAPACITY,
            response_capacity: DEFAULT_RESPONSE_CAPACITY,
            completed_ttl: DEFAULT_COMPLETED_TTL,
            max_waiters: DEFAULT_MAX_WAITERS,
            max_response_bytes: DEFAULT_MAX_RESPONSE_BYTES,
            max_completed_bytes: DEFAULT_MAX_COMPLETED_BYTES,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct OperationKey {
    session_namespace: [u8; 32],
    operation_id_hash: [u8; 32],
}

#[derive(Debug)]
struct OperationEntry {
    fingerprint: [u8; 32],
    state: OperationState,
}

#[derive(Debug)]
enum OperationState {
    Pending {
        waiters: Vec<OperationWaiter>,
    },
    Completed {
        response: Box<ResponsePayload>,
        completed_at: Instant,
        response_bytes: usize,
    },
    /// The response cache was released, but the daemon-lifetime identity is
    /// retained so the side effect can never be routed again.
    Tombstone,
}

/// One process-wide ledger is shared by every gateway connection.
pub(super) struct OperationLedger {
    entries: HashMap<OperationKey, OperationEntry>,
    /// Only the first routed request can complete a pending operation.
    routed_requests: HashMap<OperationWaiter, OperationKey>,
    /// Guards against a client reusing one request id for two pending operations.
    pending_requests: HashMap<OperationWaiter, OperationKey>,
    completed_responses: usize,
    completed_bytes: usize,
    limits: LedgerLimits,
}

impl Default for OperationLedger {
    fn default() -> Self {
        Self::new(LedgerLimits::default())
    }
}

impl OperationLedger {
    fn new(limits: LedgerLimits) -> Self {
        Self {
            entries: HashMap::new(),
            routed_requests: HashMap::new(),
            pending_requests: HashMap::new(),
            completed_responses: 0,
            completed_bytes: 0,
            limits,
        }
    }

    #[cfg(test)]
    pub fn session_namespace(session_id: &str) -> [u8; 32] {
        *blake3::hash(session_id.as_bytes()).as_bytes()
    }

    pub fn session_incarnation_namespace(session_id: &str, incarnation: u64) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(&(session_id.len() as u64).to_be_bytes());
        hasher.update(session_id.as_bytes());
        hasher.update(&incarnation.to_be_bytes());
        *hasher.finalize().as_bytes()
    }

    pub fn fingerprint<T: Serialize>(payload: &T) -> Result<[u8; 32], serde_json::Error> {
        let encoded = serde_json::to_vec(payload)?;
        Ok(*blake3::hash(&encoded).as_bytes())
    }

    pub fn begin(
        &mut self,
        session_namespace: [u8; 32],
        operation_id: &str,
        fingerprint: [u8; 32],
        waiter: OperationWaiter,
    ) -> BeginOutcome {
        self.prune_expired(Instant::now());

        if let Some(message) = invalid_operation_id(operation_id) {
            return BeginOutcome::respond(ResponsePayload::err(
                error_code::INVALID_REQUEST,
                message,
            ));
        }

        let key = OperationKey {
            session_namespace,
            operation_id_hash: *blake3::hash(operation_id.as_bytes()).as_bytes(),
        };
        // Fingerprint conflicts take precedence even when the caller reused the
        // same transport request id for the same logical operation key.
        if let Some(entry) = self.entries.get(&key)
            && entry.fingerprint != fingerprint
        {
            return BeginOutcome::respond(ResponsePayload::err(
                error_code::INVALID_REQUEST,
                format!("operation_id {operation_id:?} was reused with a different payload"),
            ));
        }

        if let Some(existing) = self.pending_requests.get(&waiter) {
            return if existing == &key {
                BeginOutcome::Wait
            } else {
                BeginOutcome::respond(ResponsePayload::err(
                    error_code::INVALID_REQUEST,
                    format!(
                        "request {} is already waiting on another operation",
                        waiter.request_id
                    ),
                ))
            };
        }

        if let Some(entry) = self.entries.get_mut(&key) {
            return match &mut entry.state {
                OperationState::Pending { waiters } => {
                    if waiters.len() >= self.limits.max_waiters {
                        BeginOutcome::respond(ResponsePayload::err(
                            error_code::INVALID_STATE,
                            format!(
                                "operation_id {operation_id:?} has too many pending retry waiters"
                            ),
                        ))
                    } else {
                        waiters.push(waiter);
                        self.pending_requests.insert(waiter, key);
                        BeginOutcome::Wait
                    }
                }
                OperationState::Completed { response, .. } => {
                    BeginOutcome::Respond(response.clone())
                }
                OperationState::Tombstone => BeginOutcome::respond(ResponsePayload::err(
                    error_code::INVALID_STATE,
                    format!(
                        "operation_id {operation_id:?} already completed, but its replay response expired"
                    ),
                )),
            };
        }

        if self.entries.len() >= self.limits.identity_capacity {
            return BeginOutcome::respond(ResponsePayload::err(
                error_code::INVALID_STATE,
                "operation identity ledger is saturated; refusing a new side effect",
            ));
        }

        self.entries.insert(
            key.clone(),
            OperationEntry {
                fingerprint,
                state: OperationState::Pending {
                    waiters: vec![waiter],
                },
            },
        );
        self.routed_requests.insert(waiter, key.clone());
        self.pending_requests.insert(waiter, key);
        BeginOutcome::Route
    }

    /// Complete the operation owned by `routed_request`, returning every live
    /// waiter that must receive the exact same response. Non-idempotent responses
    /// return `None` and continue through the ordinary gateway path.
    pub fn complete(
        &mut self,
        routed_request: OperationWaiter,
        response: ResponsePayload,
    ) -> Option<CompletedOperation> {
        let key = self.routed_requests.remove(&routed_request)?;
        let entry = self.entries.get_mut(&key)?;
        let OperationState::Pending { waiters } = &mut entry.state else {
            return None;
        };
        let waiters = std::mem::take(waiters);
        for waiter in &waiters {
            self.pending_requests.remove(waiter);
        }

        let response_bytes = serialized_response_size(&response);
        if response_bytes > self.limits.max_response_bytes {
            entry.state = OperationState::Tombstone;
            return Some(CompletedOperation { waiters, response });
        }
        entry.state = OperationState::Completed {
            response: Box::new(response.clone()),
            completed_at: Instant::now(),
            response_bytes,
        };
        self.completed_responses = self.completed_responses.saturating_add(1);
        self.completed_bytes = self.completed_bytes.saturating_add(response_bytes);
        while self.completed_responses > self.limits.response_capacity
            || self.completed_bytes > self.limits.max_completed_bytes
        {
            if !self.evict_oldest_completed(Some(&key)) {
                break;
            }
        }

        Some(CompletedOperation { waiters, response })
    }

    /// A disconnected transport must not consume pending waiter capacity. The
    /// operation and its routed request mapping remain so late completion and a
    /// reconnecting retry still cannot execute the side effect twice.
    pub fn remove_waiters_for_client(&mut self, client_id: u64) {
        for entry in self.entries.values_mut() {
            if let OperationState::Pending { waiters } = &mut entry.state {
                waiters.retain(|waiter| waiter.client_id != client_id);
            }
        }
        self.pending_requests
            .retain(|waiter, _| waiter.client_id != client_id);
    }

    fn prune_expired(&mut self, now: Instant) {
        let expired = self
            .entries
            .iter()
            .filter_map(|(key, entry)| match &entry.state {
                OperationState::Completed { completed_at, .. }
                    if now.saturating_duration_since(*completed_at)
                        >= self.limits.completed_ttl =>
                {
                    Some(key.clone())
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        for key in expired {
            self.expire_completed(&key);
        }
    }

    fn evict_oldest_completed(&mut self, protected: Option<&OperationKey>) -> bool {
        let oldest = self
            .entries
            .iter()
            .filter(|(key, _)| protected != Some(*key))
            .filter_map(|(key, entry)| match &entry.state {
                OperationState::Completed { completed_at, .. } => {
                    Some((key.clone(), *completed_at))
                }
                OperationState::Pending { .. } | OperationState::Tombstone => None,
            })
            .min_by_key(|(_, completed_at)| *completed_at)
            .map(|(key, _)| key);
        if let Some(key) = oldest {
            self.expire_completed(&key);
            true
        } else {
            false
        }
    }

    fn expire_completed(&mut self, key: &OperationKey) {
        let Some(entry) = self.entries.get_mut(key) else {
            return;
        };
        let OperationState::Completed { response_bytes, .. } = &entry.state else {
            return;
        };
        self.completed_bytes = self.completed_bytes.saturating_sub(*response_bytes);
        self.completed_responses = self.completed_responses.saturating_sub(1);
        entry.state = OperationState::Tombstone;
    }
}

fn invalid_operation_id(operation_id: &str) -> Option<String> {
    if operation_id.is_empty() {
        return Some("operation_id must not be empty".into());
    }
    if operation_id.len() > MAX_OPERATION_ID_BYTES {
        return Some(format!(
            "operation_id must be at most {MAX_OPERATION_ID_BYTES} UTF-8 bytes"
        ));
    }
    if operation_id.trim() != operation_id || operation_id.chars().any(char::is_control) {
        return Some(
            "operation_id must not contain surrounding whitespace or control characters".into(),
        );
    }
    None
}

fn serialized_response_size(response: &ResponsePayload) -> usize {
    serde_json::to_vec(response)
        .map(|encoded| encoded.len())
        .unwrap_or(usize::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use cue_core::ipc::OkPayload;

    fn waiter(client_id: u64, request_id: u32) -> OperationWaiter {
        OperationWaiter {
            client_id,
            request_id,
        }
    }

    fn fingerprint(text: &str) -> [u8; 32] {
        OperationLedger::fingerprint(&text).expect("fingerprint")
    }

    fn assert_ack(response: &ResponsePayload) {
        assert!(matches!(response, ResponsePayload::Ok(OkPayload::Ack {})));
    }

    fn unwrap_error(outcome: BeginOutcome) -> (String, String) {
        match outcome {
            BeginOutcome::Respond(response) => match *response {
                ResponsePayload::Err { code, message } => (code, message),
                other => panic!("expected error response, got {other:?}"),
            },
            other => panic!("expected response, got {other:?}"),
        }
    }

    #[test]
    fn pending_retries_join_and_completed_retries_replay() {
        let mut ledger = OperationLedger::default();
        let namespace = OperationLedger::session_namespace("session-a");
        let first = waiter(1, 1);
        let retry = waiter(2, 7);

        assert!(matches!(
            ledger.begin(namespace, "tool-call-1", fingerprint("payload"), first),
            BeginOutcome::Route
        ));
        assert!(matches!(
            ledger.begin(namespace, "tool-call-1", fingerprint("payload"), retry),
            BeginOutcome::Wait
        ));

        let completed = ledger
            .complete(first, ResponsePayload::ack())
            .expect("idempotent completion");
        assert_eq!(completed.waiters, vec![first, retry]);
        assert_ack(&completed.response);

        match ledger.begin(
            namespace,
            "tool-call-1",
            fingerprint("payload"),
            waiter(3, 9),
        ) {
            BeginOutcome::Respond(response) => assert_ack(&response),
            other => panic!("expected replay, got {other:?}"),
        }
    }

    #[test]
    fn operation_id_reuse_with_different_payload_is_rejected() {
        let mut ledger = OperationLedger::default();
        let namespace = OperationLedger::session_namespace("session-a");
        assert!(matches!(
            ledger.begin(namespace, "same", fingerprint("first"), waiter(1, 1)),
            BeginOutcome::Route
        ));

        let (code, message) =
            unwrap_error(ledger.begin(namespace, "same", fingerprint("second"), waiter(2, 1)));
        assert_eq!(code, error_code::INVALID_REQUEST);
        assert!(message.contains("different payload"));
    }

    #[test]
    fn same_waiter_cannot_hide_an_operation_payload_conflict() {
        let mut ledger = OperationLedger::default();
        let namespace = OperationLedger::session_namespace("session-a");
        let same_waiter = waiter(1, 1);
        assert!(matches!(
            ledger.begin(namespace, "same", fingerprint("first"), same_waiter),
            BeginOutcome::Route
        ));

        let (code, _) =
            unwrap_error(ledger.begin(namespace, "same", fingerprint("second"), same_waiter));
        assert_eq!(code, error_code::INVALID_REQUEST);
    }

    #[test]
    fn identical_ids_are_isolated_by_logical_session() {
        let mut ledger = OperationLedger::default();
        let first = OperationLedger::session_namespace("session-a");
        let second = OperationLedger::session_namespace("session-b");

        assert!(matches!(
            ledger.begin(first, "same", fingerprint("first"), waiter(1, 1)),
            BeginOutcome::Route
        ));
        assert!(matches!(
            ledger.begin(second, "same", fingerprint("second"), waiter(2, 1)),
            BeginOutcome::Route
        ));
    }

    #[test]
    fn disconnect_removes_waiter_but_preserves_pending_execution() {
        let mut ledger = OperationLedger::default();
        let namespace = OperationLedger::session_namespace("session-a");
        let original = waiter(1, 1);
        assert!(matches!(
            ledger.begin(namespace, "retry", fingerprint("payload"), original),
            BeginOutcome::Route
        ));
        ledger.remove_waiters_for_client(1);

        let retry = waiter(2, 1);
        assert!(matches!(
            ledger.begin(namespace, "retry", fingerprint("payload"), retry),
            BeginOutcome::Wait
        ));
        let completed = ledger
            .complete(original, ResponsePayload::ack())
            .expect("late completion");
        assert_eq!(completed.waiters, vec![retry]);
    }

    #[test]
    fn capacity_never_evicts_pending_operations() {
        let mut ledger = OperationLedger::new(LedgerLimits {
            identity_capacity: 1,
            ..LedgerLimits::default()
        });
        let namespace = OperationLedger::session_namespace("session-a");
        assert!(matches!(
            ledger.begin(namespace, "first", fingerprint("first"), waiter(1, 1)),
            BeginOutcome::Route
        ));

        let (code, message) =
            unwrap_error(ledger.begin(namespace, "second", fingerprint("second"), waiter(2, 1)));
        assert_eq!(code, error_code::INVALID_STATE);
        assert!(message.contains("identity ledger is saturated"));
    }

    #[test]
    fn operation_id_length_is_bounded() {
        let mut ledger = OperationLedger::default();
        let namespace = OperationLedger::session_namespace("session-a");
        let oversized = "x".repeat(MAX_OPERATION_ID_BYTES + 1);
        let (code, _) =
            unwrap_error(ledger.begin(namespace, &oversized, fingerprint("payload"), waiter(1, 1)));
        assert_eq!(code, error_code::INVALID_REQUEST);
    }

    #[test]
    fn pending_waiter_count_is_bounded() {
        let mut ledger = OperationLedger::new(LedgerLimits {
            max_waiters: 1,
            ..LedgerLimits::default()
        });
        let namespace = OperationLedger::session_namespace("session-a");
        assert!(matches!(
            ledger.begin(namespace, "busy", fingerprint("payload"), waiter(1, 1)),
            BeginOutcome::Route
        ));
        let (code, _) =
            unwrap_error(ledger.begin(namespace, "busy", fingerprint("payload"), waiter(2, 1)));
        assert_eq!(code, error_code::INVALID_STATE);
    }

    #[test]
    fn completed_ttl_keeps_a_tombstone_and_never_reroutes_the_side_effect() {
        let mut ledger = OperationLedger::new(LedgerLimits {
            identity_capacity: 2,
            completed_ttl: Duration::ZERO,
            ..LedgerLimits::default()
        });
        let namespace = OperationLedger::session_namespace("session-a");
        let first = waiter(1, 1);
        assert!(matches!(
            ledger.begin(namespace, "first", fingerprint("first"), first),
            BeginOutcome::Route
        ));
        ledger
            .complete(first, ResponsePayload::ack())
            .expect("complete first operation");

        let (code, _) =
            unwrap_error(ledger.begin(namespace, "first", fingerprint("first"), waiter(2, 1)));
        assert_eq!(code, error_code::INVALID_STATE);
        assert!(matches!(
            ledger.begin(namespace, "second", fingerprint("second"), waiter(2, 2)),
            BeginOutcome::Route
        ));
    }

    #[test]
    fn response_cache_pressure_degrades_to_tombstone_instead_of_rerouting() {
        let mut ledger = OperationLedger::new(LedgerLimits {
            response_capacity: 1,
            ..LedgerLimits::default()
        });
        let namespace = OperationLedger::session_namespace("session-a");
        for (index, operation_id) in ["first", "second"].into_iter().enumerate() {
            let owner = waiter(1, index as u32 + 1);
            assert!(matches!(
                ledger.begin(namespace, operation_id, fingerprint(operation_id), owner),
                BeginOutcome::Route
            ));
            ledger
                .complete(owner, ResponsePayload::ack())
                .expect("complete operation");
        }

        let (code, _) =
            unwrap_error(ledger.begin(namespace, "first", fingerprint("first"), waiter(2, 1)));
        assert_eq!(code, error_code::INVALID_STATE);
    }

    #[test]
    fn oversized_response_reaches_live_waiters_but_is_not_replayed() {
        const { assert!(DEFAULT_MAX_RESPONSE_BYTES <= cue_core::ipc::MAX_MESSAGE_SIZE / 2) };
        let mut ledger = OperationLedger::new(LedgerLimits {
            max_response_bytes: 1,
            ..LedgerLimits::default()
        });
        let namespace = OperationLedger::session_namespace("session-a");
        let first = waiter(1, 1);
        assert!(matches!(
            ledger.begin(namespace, "large", fingerprint("payload"), first),
            BeginOutcome::Route
        ));
        let completed = ledger
            .complete(first, ResponsePayload::ack())
            .expect("complete oversized response");
        assert_ack(&completed.response);

        let (code, _) =
            unwrap_error(ledger.begin(namespace, "large", fingerprint("payload"), waiter(2, 1)));
        assert_eq!(code, error_code::INVALID_STATE);
    }
}
