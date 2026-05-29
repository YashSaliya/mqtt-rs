//! PUBLISH and its ACK packets — §3.3 / §3.4 / §3.5 / §3.6 / §3.7

use bytes::Bytes;
use crate::{QoS, properties::Properties};

// ── PUBLISH ───────────────────────────────────────────────────────────────────

/// PUBLISH packet — carries an application message from client to broker
/// or broker to client.
#[derive(Debug, Clone, PartialEq)]
pub struct Publish {
    /// DUP flag: if `true`, this is a re-delivery of an earlier QoS 1/2 message.
    pub dup: bool,

    /// Quality of Service for this message.
    pub qos: QoS,

    /// RETAIN flag: broker should store this as the last known value for topic.
    pub retain: bool,

    /// The topic this message is published on.
    pub topic: String,

    /// Packet identifier — present only for QoS 1 and 2.
    pub packet_id: Option<u16>,

    /// Application message payload.
    pub payload: Bytes,

    /// MQTT 5.0 Publish Properties (None for 3.1.1).
    pub properties: Option<Properties>,
}

impl Publish {
    /// Create a simple QoS 0 publish (no packet ID needed).
    pub fn new(topic: impl Into<String>, payload: impl Into<Bytes>) -> Self {
        Self {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: false,
            topic: topic.into(),
            packet_id: None,
            payload: payload.into(),
            properties: None,
        }
    }
}
