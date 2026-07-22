//! EventBus actor — fan-out event delivery to subscribed clients.

use std::collections::HashMap;

use tokio::sync::{mpsc, watch};
use tracing::{debug, warn};

use cue_core::{EventChannel, ipc::EventPayload};

use super::{ClientEvent, EventBusMsg};

#[derive(Default)]
struct EventSubscriptions {
    // channel_name -> (client_id -> lossless delivery sink)
    channels: HashMap<EventChannel, HashMap<u64, EventSubscriber>>,
    // Map absence is unhandshaken, an entry containing None is the legacy
    // anonymous view, and Some(id) is isolated to one durable named session.
    client_sessions: HashMap<u64, Option<String>>,
}

#[derive(Clone)]
struct EventSubscriber {
    sender: mpsc::Sender<ClientEvent>,
    disconnect: watch::Sender<bool>,
}

#[derive(Debug, Default)]
struct PublishStats {
    delivered: usize,
    filtered: usize,
    lagging: usize,
    closed: usize,
}

impl EventSubscriptions {
    fn set_client_session(&mut self, client_id: u64, named_session_id: Option<String>) {
        self.client_sessions.insert(client_id, named_session_id);
    }

    fn subscribe(
        &mut self,
        client_id: u64,
        channel: EventChannel,
        sender: mpsc::Sender<ClientEvent>,
        disconnect: watch::Sender<bool>,
    ) {
        self.channels
            .entry(channel)
            .or_default()
            .insert(client_id, EventSubscriber { sender, disconnect });
    }

    fn unsubscribe(&mut self, client_id: u64, channel: &EventChannel) {
        if let Some(clients) = self.channels.get_mut(channel) {
            clients.remove(&client_id);
            if clients.is_empty() {
                self.channels.remove(channel);
            }
        }
    }

    fn unsubscribe_all(&mut self, client_id: u64) {
        self.channels.retain(|_ch, clients| {
            clients.remove(&client_id);
            !clients.is_empty()
        });
        self.client_sessions.remove(&client_id);
    }

    fn publish(
        &mut self,
        channel: &EventChannel,
        payload: &EventPayload,
        excluded_client_id: Option<u64>,
        session_id: Option<&Option<String>>,
    ) -> PublishStats {
        let mut stats = PublishStats::default();
        let deliveries = self
            .channels
            .get(channel)
            .map(|clients| {
                clients
                    .iter()
                    .filter(|(client_id, _)| Some(**client_id) != excluded_client_id)
                    .map(|(client_id, subscriber)| (*client_id, subscriber.clone()))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        let mut evicted_clients = Vec::new();
        for (client_id, subscriber) in deliveries {
            if let Some(owner) = session_id {
                let allowed = match self.client_sessions.get(&client_id) {
                    // A transport must finish its handshake before resource
                    // events can be delivered.
                    None => false,
                    // Anonymous clients intentionally retain the legacy
                    // global event view.
                    Some(None) => true,
                    // A named client only receives its own resources.
                    Some(Some(attached)) => owner.as_deref() == Some(attached.as_str()),
                };
                if !allowed {
                    stats.filtered += 1;
                    continue;
                }
            }
            let event = match session_id {
                Some(owner) => ClientEvent::session(payload.clone(), owner.clone()),
                None => ClientEvent::global(payload.clone()),
            };
            match subscriber.sender.try_send(event) {
                Ok(()) => stats.delivered += 1,
                Err(mpsc::error::TrySendError::Full(_)) => {
                    stats.lagging += 1;
                    let _ = subscriber.disconnect.send(true);
                    evicted_clients.push(client_id);
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    stats.closed += 1;
                    let _ = subscriber.disconnect.send(true);
                    evicted_clients.push(client_id);
                }
            }
        }

        // Losing even one event invalidates the client's view of every stream.
        // Remove all subscriptions immediately; the gateway watch signal above
        // closes the connection so the client must reconnect and resubscribe.
        for client_id in evicted_clients {
            self.unsubscribe_all(client_id);
        }

        stats
    }

    #[cfg(test)]
    fn subscriber_count(&self, channel: &EventChannel) -> usize {
        self.channels.get(channel).map_or(0, HashMap::len)
    }
}

/// Spawn the EventBus actor task.
pub(super) fn spawn(mut rx: mpsc::Receiver<EventBusMsg>) {
    tokio::spawn(async move {
        let mut subs = EventSubscriptions::default();

        debug!("event_bus: started");

        while let Some(msg) = rx.recv().await {
            match msg {
                EventBusMsg::Subscribe {
                    client_id,
                    channel,
                    sender,
                    disconnect,
                } => {
                    debug!(%client_id, %channel, "event_bus: subscribe");
                    subs.subscribe(client_id, channel, sender, disconnect);
                }

                EventBusMsg::SetClientSession {
                    client_id,
                    named_session_id,
                } => {
                    debug!(%client_id, ?named_session_id, "event_bus: update client session");
                    subs.set_client_session(client_id, named_session_id);
                }

                EventBusMsg::Unsubscribe { client_id, channel } => {
                    debug!(%client_id, %channel, "event_bus: unsubscribe");
                    subs.unsubscribe(client_id, &channel);
                }

                EventBusMsg::UnsubscribeAll { client_id } => {
                    debug!(%client_id, "event_bus: unsubscribe_all");
                    subs.unsubscribe_all(client_id);
                }

                EventBusMsg::Publish { payload, channel } => {
                    let stats = subs.publish(&channel, &payload, None, None);
                    if stats.lagging > 0 || stats.closed > 0 {
                        warn!(
                            %channel,
                            delivered = stats.delivered,
                            lagging = stats.lagging,
                            closed = stats.closed,
                            "event_bus: evicted unavailable subscribers while publishing"
                        );
                    }
                }

                EventBusMsg::PublishExcept {
                    payload,
                    channel,
                    excluded_client_id,
                } => {
                    let stats = subs.publish(&channel, &payload, Some(excluded_client_id), None);
                    if stats.lagging > 0 || stats.closed > 0 {
                        warn!(
                            %channel,
                            delivered = stats.delivered,
                            lagging = stats.lagging,
                            closed = stats.closed,
                            "event_bus: evicted unavailable subscribers while publishing"
                        );
                    }
                }

                EventBusMsg::PublishSession {
                    payload,
                    channel,
                    session_id,
                } => {
                    let stats = subs.publish(&channel, &payload, None, Some(&session_id));
                    if stats.lagging > 0 || stats.closed > 0 {
                        warn!(
                            %channel,
                            delivered = stats.delivered,
                            filtered = stats.filtered,
                            lagging = stats.lagging,
                            closed = stats.closed,
                            "event_bus: evicted unavailable subscribers while publishing session event"
                        );
                    }
                }

                EventBusMsg::PublishSessionExcept {
                    payload,
                    channel,
                    session_id,
                    excluded_client_id,
                } => {
                    let stats = subs.publish(
                        &channel,
                        &payload,
                        Some(excluded_client_id),
                        Some(&session_id),
                    );
                    if stats.lagging > 0 || stats.closed > 0 {
                        warn!(
                            %channel,
                            delivered = stats.delivered,
                            filtered = stats.filtered,
                            lagging = stats.lagging,
                            closed = stats.closed,
                            "event_bus: evicted unavailable subscribers while publishing session event"
                        );
                    }
                }

                EventBusMsg::Shutdown => {
                    debug!("event_bus: shutting down");
                    break;
                }
            }
        }

        debug!("event_bus: stopped");
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event() -> EventPayload {
        EventPayload::ShuttingDown {
            reason: "test".into(),
        }
    }

    #[test]
    fn publish_removes_closed_subscribers() {
        let mut subscriptions = EventSubscriptions::default();
        let (tx, rx) = mpsc::channel(1);
        let (disconnect_tx, mut disconnect_rx) = watch::channel(false);
        drop(rx);
        subscriptions.subscribe(1, EventChannel::System, tx, disconnect_tx);

        let stats = subscriptions.publish(&EventChannel::System, &event(), None, None);

        assert_eq!(stats.delivered, 0);
        assert_eq!(stats.lagging, 0);
        assert_eq!(stats.closed, 1);
        assert!(*disconnect_rx.borrow_and_update());
        assert_eq!(subscriptions.subscriber_count(&EventChannel::System), 0);
    }

    #[test]
    fn publish_evicts_lagging_subscriber_without_blocking_healthy_subscriber() {
        let mut subscriptions = EventSubscriptions::default();
        let (slow_tx, mut slow_rx) = mpsc::channel(1);
        let (healthy_tx, mut healthy_rx) = mpsc::channel(1);
        let (slow_disconnect_tx, mut slow_disconnect_rx) = watch::channel(false);
        let (healthy_disconnect_tx, healthy_disconnect_rx) = watch::channel(false);
        subscriptions.subscribe(
            1,
            EventChannel::System,
            slow_tx.clone(),
            slow_disconnect_tx.clone(),
        );
        subscriptions.subscribe(1, EventChannel::Jobs, slow_tx, slow_disconnect_tx);
        subscriptions.subscribe(2, EventChannel::System, healthy_tx, healthy_disconnect_tx);

        let first = subscriptions.publish(&EventChannel::System, &event(), None, None);
        assert_eq!(first.delivered, 2);
        assert!(healthy_rx.try_recv().is_ok());

        let second_stats = subscriptions.publish(&EventChannel::System, &event(), None, None);

        assert_eq!(second_stats.delivered, 1);
        assert_eq!(second_stats.lagging, 1);
        assert_eq!(second_stats.closed, 0);
        assert!(*slow_disconnect_rx.borrow_and_update());
        assert!(!*healthy_disconnect_rx.borrow());
        assert!(slow_rx.try_recv().is_ok());
        assert!(healthy_rx.try_recv().is_ok());
        assert_eq!(subscriptions.subscriber_count(&EventChannel::System), 1);
        assert_eq!(subscriptions.subscriber_count(&EventChannel::Jobs), 0);

        // A fresh transport can subscribe normally after the evicted one closes.
        let (reconnected_tx, mut reconnected_rx) = mpsc::channel(1);
        let (reconnected_disconnect_tx, reconnected_disconnect_rx) = watch::channel(false);
        subscriptions.subscribe(
            3,
            EventChannel::System,
            reconnected_tx,
            reconnected_disconnect_tx,
        );
        let reconnect_stats = subscriptions.publish(&EventChannel::System, &event(), None, None);
        assert_eq!(reconnect_stats.delivered, 2);
        assert!(healthy_rx.try_recv().is_ok());
        assert!(reconnected_rx.try_recv().is_ok());
        assert!(!*reconnected_disconnect_rx.borrow());
    }

    #[test]
    fn publish_can_skip_one_subscriber_without_unsubscribing_it() {
        let mut subscriptions = EventSubscriptions::default();
        let (first_tx, mut first_rx) = mpsc::channel(1);
        let (second_tx, mut second_rx) = mpsc::channel(1);
        let (first_disconnect, _first_disconnect_rx) = watch::channel(false);
        let (second_disconnect, _second_disconnect_rx) = watch::channel(false);
        subscriptions.subscribe(1, EventChannel::System, first_tx, first_disconnect);
        subscriptions.subscribe(2, EventChannel::System, second_tx, second_disconnect);

        let stats = subscriptions.publish(&EventChannel::System, &event(), Some(1), None);

        assert_eq!(stats.delivered, 1);
        assert!(first_rx.try_recv().is_err());
        assert!(second_rx.try_recv().is_ok());
        assert_eq!(subscriptions.subscriber_count(&EventChannel::System), 2);
    }

    #[test]
    fn session_publish_is_visible_to_legacy_and_matching_clients_only() {
        let mut subscriptions = EventSubscriptions::default();
        let (legacy_tx, mut legacy_rx) = mpsc::channel(1);
        let (matching_tx, mut matching_rx) = mpsc::channel(1);
        let (foreign_tx, mut foreign_rx) = mpsc::channel(1);
        let (legacy_disconnect, _) = watch::channel(false);
        let (matching_disconnect, _) = watch::channel(false);
        let (foreign_disconnect, _) = watch::channel(false);
        subscriptions.subscribe(1, EventChannel::Jobs, legacy_tx, legacy_disconnect);
        subscriptions.subscribe(2, EventChannel::Jobs, matching_tx, matching_disconnect);
        subscriptions.subscribe(3, EventChannel::Jobs, foreign_tx, foreign_disconnect);
        subscriptions.set_client_session(1, None);
        subscriptions.set_client_session(2, Some("SS-owner".into()));
        subscriptions.set_client_session(3, Some("SS-foreign".into()));

        let owner = Some("SS-owner".to_string());
        let stats = subscriptions.publish(&EventChannel::Jobs, &event(), None, Some(&owner));

        assert_eq!(stats.delivered, 2);
        assert_eq!(stats.filtered, 1);
        assert!(legacy_rx.try_recv().is_ok());
        assert!(matching_rx.try_recv().is_ok());
        assert!(foreign_rx.try_recv().is_err());
    }

    #[test]
    fn session_publish_is_not_visible_before_handshake_binding() {
        let mut subscriptions = EventSubscriptions::default();
        let (tx, mut rx) = mpsc::channel(1);
        let (disconnect, _) = watch::channel(false);
        subscriptions.subscribe(9, EventChannel::Jobs, tx, disconnect);

        let owner = Some("SS-owner".to_string());
        let stats = subscriptions.publish(&EventChannel::Jobs, &event(), None, Some(&owner));

        assert_eq!(stats.delivered, 0);
        assert_eq!(stats.filtered, 1);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn changing_client_session_updates_existing_subscriptions() {
        let mut subscriptions = EventSubscriptions::default();
        let (tx, mut rx) = mpsc::channel(2);
        let (disconnect, _) = watch::channel(false);
        subscriptions.subscribe(7, EventChannel::Jobs, tx, disconnect);
        subscriptions.set_client_session(7, Some("SS-first".into()));

        let first_owner = Some("SS-first".to_string());
        let second_owner = Some("SS-second".to_string());
        assert_eq!(
            subscriptions
                .publish(&EventChannel::Jobs, &event(), None, Some(&first_owner))
                .delivered,
            1
        );
        subscriptions.set_client_session(7, Some("SS-second".into()));
        assert_eq!(
            subscriptions
                .publish(&EventChannel::Jobs, &event(), None, Some(&first_owner))
                .filtered,
            1
        );
        assert_eq!(
            subscriptions
                .publish(&EventChannel::Jobs, &event(), None, Some(&second_owner))
                .delivered,
            1
        );
        assert!(rx.try_recv().is_ok());
        assert!(rx.try_recv().is_ok());
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn unowned_session_event_is_visible_only_to_legacy_clients() {
        let mut subscriptions = EventSubscriptions::default();
        let (legacy_tx, mut legacy_rx) = mpsc::channel(1);
        let (named_tx, mut named_rx) = mpsc::channel(1);
        let (legacy_disconnect, _) = watch::channel(false);
        let (named_disconnect, _) = watch::channel(false);
        subscriptions.subscribe(1, EventChannel::Crons, legacy_tx, legacy_disconnect);
        subscriptions.subscribe(2, EventChannel::Crons, named_tx, named_disconnect);
        subscriptions.set_client_session(1, None);
        subscriptions.set_client_session(2, Some("SS-named".into()));

        let owner = None;
        let stats = subscriptions.publish(&EventChannel::Crons, &event(), None, Some(&owner));

        assert_eq!(stats.delivered, 1);
        assert_eq!(stats.filtered, 1);
        assert!(legacy_rx.try_recv().is_ok());
        assert!(named_rx.try_recv().is_err());
    }
}
