use std::collections::HashMap;
use std::time::{Instant, Duration};
use bytes::Bytes;
use mqtt_core::QoS;
use crate::topic_trie::Subscription;

#[derive(Debug, Clone)]
pub struct PendingPublish {
    pub packet_id: u16,
    pub topic: String,
    pub payload: Bytes,
    pub qos: QoS,
    pub retain: bool,
    pub subscription_identifier: Option<u32>,
    pub dup: bool,
}

#[derive(Debug, Clone)]
pub enum SessionMessage {
    Publish(PendingPublish),
    PubRel(u16),
}

pub struct Session {
    pub client_id: String,
    pub clean_session: bool,
    pub is_connected: bool,
    pub subscriptions: HashMap<String, Subscription>,
    
    // Offline queue for QoS 1 & 2 messages when disconnected
    pub offline_queue: Vec<SessionMessage>,
    
    // In-flight tracking (Packet ID -> message)
    pub inflight_qos1: HashMap<u16, PendingPublish>,
    pub inflight_qos2: HashMap<u16, PendingPublish>, // PUBREC received, waiting for PUBREL/PUBCOMP
    pub inflight_pubrel: HashMap<u16, Instant>,       // PUBREL sent, waiting for PUBCOMP

    // Next outgoing packet identifier
    pub next_packet_id: u16,
    
    // Session Expiry
    pub session_expiry_interval: Option<u32>,
    pub session_expiry_time: Option<Instant>,
}

impl Session {
    pub fn new(client_id: String, clean_session: bool, session_expiry_interval: Option<u32>) -> Self {
        Self {
            client_id,
            clean_session,
            is_connected: false,
            subscriptions: HashMap::new(),
            offline_queue: Vec::new(),
            inflight_qos1: HashMap::new(),
            inflight_qos2: HashMap::new(),
            inflight_pubrel: HashMap::new(),
            next_packet_id: 1,
            session_expiry_interval,
            session_expiry_time: None,
        }
    }

    pub fn get_next_packet_id(&mut self) -> u16 {
        let id = self.next_packet_id;
        if self.next_packet_id == u16::MAX {
            self.next_packet_id = 1;
        } else {
            self.next_packet_id += 1;
        }
        id
    }

    pub fn is_expired(&self) -> bool {
        if self.is_connected {
            return false;
        }
        if let Some(expiry) = self.session_expiry_time {
            Instant::now() >= expiry
        } else {
            false
        }
    }

    pub fn mark_disconnected(&mut self) {
        self.is_connected = false;
        if let Some(secs) = self.session_expiry_interval {
            if secs > 0 && secs != 0xFFFFFFFF {
                self.session_expiry_time = Some(Instant::now() + Duration::from_secs(secs as u64));
            }
        }
    }

    pub fn mark_connected(&mut self) {
        self.is_connected = true;
        self.session_expiry_time = None;
    }
}
