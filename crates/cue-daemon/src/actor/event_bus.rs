//! EventBus actor — fan-out event delivery to subscribed clients.

use std::collections::HashMap;

use tokio::sync::mpsc;
use tracing::{debug, warn};

use cue_core::{EventChannel, ipc::EventPayload};

use super::EventBusMsg;

#[derive(Default)]
struct EventSubscriptions {
    // channel_name -> (client_id -> sender)
    channels: HashMap<EventChannel, HashMap<u64, mpsc::Sender<EventPayload>>>,
}

#[derive(Debug, Default)]
struct PublishStats {
    delivered: usize,
    closed: usize,
}

impl EventSubscriptions {
    fn subscribe(
        &mut self,
        client_id: u64,
        channel: EventChannel,
        sender: mpsc::Sender<EventPayload>,
    ) {
        self.channels
            .entry(channel)
            .or_default()
            .insert(client_id, sender);
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
    }

    async fn publish(
        &mut self,
        channel: &EventChannel,
        payload: &EventPayload,
        excluded_client_id: Option<u64>,
    ) -> PublishStats {
        let mut stats = PublishStats::default();
        let deliveries = self
            .channels
            .get(channel)
            .map(|clients| {
                clients
                    .iter()
                    .filter(|(client_id, _)| Some(**client_id) != excluded_client_id)
                    .map(|(client_id, sender)| (*client_id, sender.clone()))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        let mut closed_clients = Vec::new();
        for (client_id, sender) in deliveries {
            if sender.send(payload.clone()).await.is_ok() {
                stats.delivered += 1;
            } else {
                stats.closed += 1;
                closed_clients.push(client_id);
            }
        }

        if !closed_clients.is_empty()
            && let Some(clients) = self.channels.get_mut(channel)
        {
            for client_id in closed_clients {
                clients.remove(&client_id);
            }
            if clients.is_empty() {
                self.channels.remove(channel);
            }
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
                } => {
                    debug!(%client_id, %channel, "event_bus: subscribe");
                    subs.subscribe(client_id, channel, sender);
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
                    let stats = subs.publish(&channel, &payload, None).await;
                    if stats.closed > 0 {
                        warn!(
                            %channel,
                            delivered = stats.delivered,
                            closed = stats.closed,
                            "event_bus: removed closed subscribers while publishing"
                        );
                    }
                }

                EventBusMsg::PublishExcept {
                    payload,
                    channel,
                    excluded_client_id,
                } => {
                    let stats = subs
                        .publish(&channel, &payload, Some(excluded_client_id))
                        .await;
                    if stats.closed > 0 {
                        warn!(
                            %channel,
                            delivered = stats.delivered,
                            closed = stats.closed,
                            "event_bus: removed closed subscribers while publishing"
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

    #[tokio::test]
    async fn publish_removes_closed_subscribers() {
        let mut subscriptions = EventSubscriptions::default();
        let (tx, rx) = mpsc::channel(1);
        drop(rx);
        subscriptions.subscribe(1, EventChannel::System, tx);

        let stats = subscriptions
            .publish(&EventChannel::System, &event(), None)
            .await;

        assert_eq!(stats.delivered, 0);
        assert_eq!(stats.closed, 1);
        assert_eq!(subscriptions.subscriber_count(&EventChannel::System), 0);
    }

    #[tokio::test]
    async fn publish_backpressures_slow_subscribers_without_dropping_events() {
        let mut subscriptions = EventSubscriptions::default();
        let (tx, mut rx) = mpsc::channel(1);
        subscriptions.subscribe(1, EventChannel::System, tx);

        let first = subscriptions
            .publish(&EventChannel::System, &event(), None)
            .await;
        let second = event();
        let second_stats = {
            let second_publish = subscriptions.publish(&EventChannel::System, &second, None);
            tokio::pin!(second_publish);

            assert_eq!(first.delivered, 1);
            tokio::select! {
                stats = &mut second_publish => panic!("second publish should wait for subscriber capacity: {stats:?}"),
                () = tokio::task::yield_now() => {}
            }

            assert!(rx.try_recv().is_ok());
            second_publish.await
        };

        assert_eq!(second_stats.delivered, 1);
        assert!(rx.try_recv().is_ok());
        assert_eq!(subscriptions.subscriber_count(&EventChannel::System), 1);
    }

    #[tokio::test]
    async fn publish_can_skip_one_subscriber_without_unsubscribing_it() {
        let mut subscriptions = EventSubscriptions::default();
        let (first_tx, mut first_rx) = mpsc::channel(1);
        let (second_tx, mut second_rx) = mpsc::channel(1);
        subscriptions.subscribe(1, EventChannel::System, first_tx);
        subscriptions.subscribe(2, EventChannel::System, second_tx);

        let stats = subscriptions
            .publish(&EventChannel::System, &event(), Some(1))
            .await;

        assert_eq!(stats.delivered, 1);
        assert!(first_rx.try_recv().is_err());
        assert!(second_rx.try_recv().is_ok());
        assert_eq!(subscriptions.subscriber_count(&EventChannel::System), 2);
    }
}
