//! EventBus actor — fan-out event delivery to subscribed clients.

use std::collections::HashMap;

use tokio::sync::mpsc;
use tracing::{debug, warn};

use cue_core::ipc::EventPayload;

use super::EventBusMsg;

/// Spawn the EventBus actor task.
pub fn spawn(mut rx: mpsc::Receiver<EventBusMsg>) {
    tokio::spawn(async move {
        // channel_name → (client_id → sender)
        let mut subs: HashMap<String, HashMap<u64, mpsc::Sender<EventPayload>>> = HashMap::new();

        debug!("event_bus: started");

        while let Some(msg) = rx.recv().await {
            match msg {
                EventBusMsg::Subscribe {
                    client_id,
                    channel,
                    sender,
                } => {
                    debug!(%client_id, %channel, "event_bus: subscribe");
                    subs.entry(channel).or_default().insert(client_id, sender);
                }

                EventBusMsg::Unsubscribe { client_id, channel } => {
                    debug!(%client_id, %channel, "event_bus: unsubscribe");
                    if let Some(clients) = subs.get_mut(&channel) {
                        clients.remove(&client_id);
                        if clients.is_empty() {
                            subs.remove(&channel);
                        }
                    }
                }

                EventBusMsg::UnsubscribeAll { client_id } => {
                    debug!(%client_id, "event_bus: unsubscribe_all");
                    subs.retain(|_ch, clients| {
                        clients.remove(&client_id);
                        !clients.is_empty()
                    });
                }

                EventBusMsg::Publish { payload, channel } => {
                    if let Some(clients) = subs.get(&channel) {
                        for (&cid, sender) in clients {
                            if sender.try_send(payload.clone()).is_err() {
                                warn!(client_id = %cid, %channel, "event_bus: client channel full or closed");
                            }
                        }
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
