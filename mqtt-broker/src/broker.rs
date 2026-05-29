use std::collections::HashMap;
use tokio::sync::mpsc;
use tracing::{info, warn, error, debug};
use bytes::Bytes;
use mqtt_core::{QoS, packet::subscribe::SubscriptionFilter};

use crate::config::Config;
use crate::topic_trie::{TopicTrie, Subscription};
use crate::retained::RetainedStore;
use crate::session::{Session, PendingPublish, SessionMessage};
use crate::auth::AuthManager;

#[derive(Debug, Clone)]
pub enum BrokerToClientMessage {
    Publish(PendingPublish),
    PubRel(u16),
    DisconnectGracefully,
}

#[derive(Debug)]
pub enum ClientToBrokerMessage {
    Connect {
        client_id: String,
        clean_session: bool,
        session_expiry_interval: Option<u32>,
        tx: mpsc::Sender<BrokerToClientMessage>,
        respond_tx: mpsc::Sender<Result<bool, String>>, // returns if session was present
    },
    Disconnect {
        client_id: String,
    },
    Subscribe {
        client_id: String,
        filters: Vec<SubscriptionFilter>,
        subscription_identifier: Option<u32>,
        respond_tx: mpsc::Sender<Vec<QoS>>,
    },
    Unsubscribe {
        client_id: String,
        filters: Vec<String>,
        respond_tx: mpsc::Sender<Vec<bool>>,
    },
    Publish {
        client_id: String,
        topic: String,
        payload: Bytes,
        qos: QoS,
        retain: bool,
        expiry_secs: Option<u32>,
        respond_tx: mpsc::Sender<Option<Vec<Subscription>>>, // QoS 1/2 subscriptions matched, or None
    },
    PubAck {
        client_id: String,
        packet_id: u16,
    },
    PubRec {
        client_id: String,
        packet_id: u16,
    },
    PubRel {
        client_id: String,
        packet_id: u16,
    },
    PubComp {
        client_id: String,
        packet_id: u16,
    },
}

pub struct Broker {
    config: Config,
    topic_trie: TopicTrie,
    retained_store: RetainedStore,
    sessions: HashMap<String, Session>,
    // client_id -> channel to the connection task
    active_connections: HashMap<String, mpsc::Sender<BrokerToClientMessage>>,
    auth_manager: AuthManager,
    // Shared subscription round-robin cursors: (group, topic_filter) -> index
    shared_cursors: HashMap<(String, String), usize>,
}

impl Broker {
    pub fn new(config: Config) -> Self {
        let auth_manager = AuthManager::new(&config);
        Self {
            config,
            topic_trie: TopicTrie::new(),
            retained_store: RetainedStore::new(),
            sessions: HashMap::new(),
            active_connections: HashMap::new(),
            auth_manager,
            shared_cursors: HashMap::new(),
        }
    }

    pub fn auth_manager(&self) -> &AuthManager {
        &self.auth_manager
    }

    pub async fn run(mut self, mut rx: mpsc::Receiver<ClientToBrokerMessage>) {
        info!("Broker actor started");
        while let Some(msg) = rx.recv().await {
            match msg {
                ClientToBrokerMessage::Connect {
                    client_id,
                    clean_session,
                    session_expiry_interval,
                    tx,
                    respond_tx,
                } => {
                    let present = self.handle_connect(client_id, clean_session, session_expiry_interval, tx).await;
                    let _ = respond_tx.send(Ok(present)).await;
                }
                ClientToBrokerMessage::Disconnect { client_id } => {
                    self.handle_disconnect(&client_id).await;
                }
                ClientToBrokerMessage::Subscribe {
                    client_id,
                    filters,
                    subscription_identifier,
                    respond_tx,
                } => {
                    let qoses = self.handle_subscribe(&client_id, filters, subscription_identifier).await;
                    let _ = respond_tx.send(qoses).await;
                }
                ClientToBrokerMessage::Unsubscribe {
                    client_id,
                    filters,
                    respond_tx,
                } => {
                    let results = self.handle_unsubscribe(&client_id, filters).await;
                    let _ = respond_tx.send(results).await;
                }
                ClientToBrokerMessage::Publish {
                    client_id,
                    topic,
                    payload,
                    qos,
                    retain,
                    expiry_secs,
                    respond_tx,
                } => {
                    let subs = self.handle_publish(&client_id, topic, payload, qos, retain, expiry_secs).await;
                    let _ = respond_tx.send(subs).await;
                }
                ClientToBrokerMessage::PubAck { client_id, packet_id } => {
                    self.handle_puback(&client_id, packet_id);
                }
                ClientToBrokerMessage::PubRec { client_id, packet_id } => {
                    self.handle_pubrec(&client_id, packet_id).await;
                }
                ClientToBrokerMessage::PubRel { client_id, packet_id } => {
                    self.handle_pubrel(&client_id, packet_id).await;
                }
                ClientToBrokerMessage::PubComp { client_id, packet_id } => {
                    self.handle_pubcomp(&client_id, packet_id);
                }
            }
        }
        info!("Broker actor stopped");
    }

    async fn handle_connect(
        &mut self,
        client_id: String,
        clean_session: bool,
        session_expiry_interval: Option<u32>,
        tx: mpsc::Sender<BrokerToClientMessage>,
    ) -> bool {
        debug!(client_id = ?client_id, clean_session = clean_session, "Handling connect");

        // If client is already connected, disconnect it first (§3.1.4)
        if let Some(old_tx) = self.active_connections.remove(&client_id) {
            warn!(client_id = ?client_id, "Client already connected. Evicting old connection.");
            let _ = old_tx.send(BrokerToClientMessage::DisconnectGracefully).await;
        }

        let session_present = self.sessions.contains_key(&client_id) && !clean_session;

        if clean_session {
            // Clean session: remove existing subscriptions from trie
            if let Some(old_session) = self.sessions.remove(&client_id) {
                for (filter, _) in old_session.subscriptions {
                    self.topic_trie.remove(&filter, &client_id);
                }
            }
            let session = Session::new(client_id.clone(), clean_session, session_expiry_interval);
            self.sessions.insert(client_id.clone(), session);
        } else {
            self.sessions.entry(client_id.clone()).or_insert_with(|| {
                Session::new(client_id.clone(), clean_session, session_expiry_interval)
            });
        }

        let session = self.sessions.get_mut(&client_id).unwrap();
        session.mark_connected();
        self.active_connections.insert(client_id.clone(), tx.clone());

        // Deliver queued offline messages
        let offline_msgs = std::mem::take(&mut session.offline_queue);
        for offline_msg in offline_msgs {
            match offline_msg {
                SessionMessage::Publish(pub_msg) => {
                    let _ = tx.send(BrokerToClientMessage::Publish(pub_msg)).await;
                }
                SessionMessage::PubRel(packet_id) => {
                    let _ = tx.send(BrokerToClientMessage::PubRel(packet_id)).await;
                }
            }
        }

        session_present
    }

    async fn handle_disconnect(&mut self, client_id: &str) {
        debug!(client_id = ?client_id, "Handling disconnect");
        self.active_connections.remove(client_id);

        if let Some(session) = self.sessions.get_mut(client_id) {
            session.mark_disconnected();
            
            // Clean up session immediately if CleanSession/Expiry is 0
            let should_remove = session.clean_session || session.session_expiry_interval == Some(0);
            if should_remove {
                let subscriptions = std::mem::take(&mut session.subscriptions);
                for filter in subscriptions.keys() {
                    self.topic_trie.remove(filter, client_id);
                }
                self.sessions.remove(client_id);
            }
        }
    }

    async fn handle_subscribe(
        &mut self,
        client_id: &str,
        filters: Vec<SubscriptionFilter>,
        subscription_identifier: Option<u32>,
    ) -> Vec<QoS> {
        let mut qoses = Vec::new();
        let session = match self.sessions.get_mut(client_id) {
            Some(s) => s,
            None => {
                error!(client_id = ?client_id, "Subscribe failed: no session found");
                return filters.iter().map(|_| QoS::AtMostOnce).collect();
            }
        };

        for f in filters {
            let sub = Subscription {
                client_id: client_id.to_string(),
                filter: f.topic_filter.clone(),
                qos: f.qos,
                no_local: f.no_local,
                retain_as_published: f.retain_as_published,
                retain_handling: f.retain_handling as u8,
                subscription_identifier,
            };

            self.topic_trie.insert(sub.clone());
            session.subscriptions.insert(f.topic_filter.clone(), sub);
            qoses.push(f.qos);

            // Deliver retained messages matching this subscription filter
            let retained = self.retained_store.get_matching(&f.topic_filter);
            for ret in retained {
                // Respect Retain Handling option in MQTT 5.0
                let should_send = match f.retain_handling {
                    mqtt_core::packet::subscribe::RetainHandling::SendOnSubscribe => true,
                    mqtt_core::packet::subscribe::RetainHandling::SendOnNewSubscription => true, // simplification
                    mqtt_core::packet::subscribe::RetainHandling::DoNotSend => false,
                };

                if should_send {
                    let packet_id = session.get_next_packet_id();
                    let pub_msg = PendingPublish {
                        packet_id,
                        topic: ret.topic.clone(),
                        payload: ret.payload.clone(),
                        qos: std::cmp::min(ret.qos, f.qos),
                        retain: true,
                        subscription_identifier,
                        dup: false,
                    };

                    if let Some(tx) = self.active_connections.get(client_id) {
                        if pub_msg.qos == QoS::AtMostOnce {
                            let _ = tx.send(BrokerToClientMessage::Publish(pub_msg)).await;
                        } else {
                            session.inflight_qos1.insert(packet_id, pub_msg.clone());
                            let _ = tx.send(BrokerToClientMessage::Publish(pub_msg)).await;
                        }
                    }
                }
            }
        }

        qoses
    }

    async fn handle_unsubscribe(&mut self, client_id: &str, filters: Vec<String>) -> Vec<bool> {
        let mut results = Vec::new();
        let session = match self.sessions.get_mut(client_id) {
            Some(s) => s,
            None => return filters.iter().map(|_| false).collect(),
        };

        for filter in filters {
            let removed = self.topic_trie.remove(&filter, client_id);
            session.subscriptions.remove(&filter);
            results.push(removed);
        }

        results
    }

    async fn handle_publish(
        &mut self,
        publisher_client_id: &str,
        topic: String,
        payload: Bytes,
        qos: QoS,
        retain: bool,
        expiry_secs: Option<u32>,
    ) -> Option<Vec<Subscription>> {
        debug!(topic = ?topic, qos = ?qos, retain = retain, "Handling publish");

        if retain {
            self.retained_store.insert(topic.clone(), payload.clone(), qos, expiry_secs);
        }

        let (mut regular_subs, shared_sub_map) = self.topic_trie.matches(&topic);

        // Process shared subscriptions (round-robin selection per group)
        for (group_name, mut subs) in shared_sub_map {
            if subs.is_empty() {
                continue;
            }
            // Sort by client_id to keep order consistent
            subs.sort_by(|a, b| a.client_id.cmp(&b.client_id));
            
            let key = (group_name, topic.clone());
            let cursor = self.shared_cursors.entry(key).or_insert(0);
            
            let selected_idx = *cursor % subs.len();
            let selected_sub = subs[selected_idx].clone();
            *cursor = (*cursor + 1) % subs.len();

            regular_subs.push(selected_sub);
        }

        let mut ack_subs = Vec::new();

        for sub in regular_subs {
            // MQTT 5.0 NoLocal option: don't deliver to the publisher itself
            if sub.no_local && sub.client_id == publisher_client_id {
                continue;
            }

            let delivery_qos = std::cmp::min(qos, sub.qos);
            
            let dest_session = match self.sessions.get_mut(&sub.client_id) {
                Some(s) => s,
                None => continue,
            };

            let packet_id = if delivery_qos == QoS::AtMostOnce {
                0
            } else {
                dest_session.get_next_packet_id()
            };

            let pending = PendingPublish {
                packet_id,
                topic: topic.clone(),
                payload: payload.clone(),
                qos: delivery_qos,
                retain: sub.retain_as_published && retain,
                subscription_identifier: sub.subscription_identifier,
                dup: false,
            };

            if delivery_qos > QoS::AtMostOnce {
                ack_subs.push(sub.clone());
            }

            if dest_session.is_connected {
                if let Some(tx) = self.active_connections.get(&sub.client_id) {
                    if delivery_qos == QoS::AtMostOnce {
                        let _ = tx.send(BrokerToClientMessage::Publish(pending)).await;
                    } else if delivery_qos == QoS::AtLeastOnce {
                        dest_session.inflight_qos1.insert(packet_id, pending.clone());
                        let _ = tx.send(BrokerToClientMessage::Publish(pending)).await;
                    } else if delivery_qos == QoS::ExactlyOnce {
                        dest_session.inflight_qos2.insert(packet_id, pending.clone());
                        let _ = tx.send(BrokerToClientMessage::Publish(pending)).await;
                    }
                }
            } else if !dest_session.clean_session {
                // Persistent offline session queue
                dest_session.offline_queue.push(SessionMessage::Publish(pending));
            }
        }

        if ack_subs.is_empty() {
            None
        } else {
            Some(ack_subs)
        }
    }

    fn handle_puback(&mut self, client_id: &str, packet_id: u16) {
        if let Some(sess) = self.sessions.get_mut(client_id) {
            sess.inflight_qos1.remove(&packet_id);
        }
    }

    async fn handle_pubrec(&mut self, client_id: &str, packet_id: u16) {
        if let Some(sess) = self.sessions.get_mut(client_id) {
            if let Some(_pending) = sess.inflight_qos2.remove(&packet_id) {
                // Move from inflight_qos2 to inflight_pubrel
                sess.inflight_pubrel.insert(packet_id, std::time::Instant::now());
                if let Some(tx) = self.active_connections.get(client_id) {
                    let _ = tx.send(BrokerToClientMessage::PubRel(packet_id)).await;
                }
            }
        }
    }

    async fn handle_pubrel(&mut self, _client_id: &str, _packet_id: u16) {
        // Handled in connection.rs
    }

    fn handle_pubcomp(&mut self, client_id: &str, packet_id: u16) {
        if let Some(sess) = self.sessions.get_mut(client_id) {
            sess.inflight_pubrel.remove(&packet_id);
        }
    }
}
