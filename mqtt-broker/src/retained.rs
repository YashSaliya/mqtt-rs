use std::collections::HashMap;
use std::time::{Duration, Instant};
use bytes::Bytes;
use mqtt_core::QoS;

#[derive(Debug, Clone)]
pub struct RetainedMessage {
    pub topic: String,
    pub payload: Bytes,
    pub qos: QoS,
    pub expiry_time: Option<Instant>,
}

impl RetainedMessage {
    pub fn is_expired(&self) -> bool {
        if let Some(expiry) = self.expiry_time {
            Instant::now() >= expiry
        } else {
            false
        }
    }
}

pub struct RetainedStore {
    messages: HashMap<String, RetainedMessage>,
}

impl RetainedStore {
    pub fn new() -> Self {
        Self {
            messages: HashMap::new(),
        }
    }

    pub fn insert(&mut self, topic: String, payload: Bytes, qos: QoS, expiry_secs: Option<u32>) {
        if payload.is_empty() {
            // A PUBLISH packet with a RETAIN flag set to 1 and a payload of zero bytes
            // will be processed as normal by the Server but any existing retained message
            // for its topic is deleted (§3.3.1.3)
            self.messages.remove(&topic);
        } else {
            let expiry_time = expiry_secs.map(|secs| Instant::now() + Duration::from_secs(secs as u64));
            self.messages.insert(
                topic.clone(),
                RetainedMessage {
                    topic,
                    payload,
                    qos,
                    expiry_time,
                },
            );
        }
    }

    pub fn get_matching(&mut self, filter: &str) -> Vec<RetainedMessage> {
        use mqtt_core::topic::topic_matches;
        use mqtt_core::topic::parse_shared_subscription;

        // Clean up expired messages first
        self.messages.retain(|_, msg| !msg.is_expired());

        let bare_filter = if let Some((_, bare)) = parse_shared_subscription(filter) {
            bare
        } else {
            filter
        };

        self.messages
            .values()
            .filter(|msg| topic_matches(bare_filter, &msg.topic))
            .cloned()
            .collect()
    }
}
