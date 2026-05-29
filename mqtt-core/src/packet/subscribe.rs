//! SUBSCRIBE and SUBACK packets — §3.8 / §3.9

use crate::{QoS, properties::Properties};

// ── SUBSCRIBE ─────────────────────────────────────────────────────────────────

/// SUBSCRIBE packet — client requests delivery of messages on topic filters.
#[derive(Debug, Clone, PartialEq)]
pub struct Subscribe {
    /// Packet identifier (must be > 0).
    pub packet_id: u16,

    /// One or more topic filters with their requested QoS.
    pub filters: Vec<SubscriptionFilter>,

    /// MQTT 5.0 Subscribe Properties (None for 3.1.1).
    pub properties: Option<Properties>,
}

/// A single topic filter within a SUBSCRIBE packet.
#[derive(Debug, Clone, PartialEq)]
pub struct SubscriptionFilter {
    /// The topic filter string (may contain `+` or `#` wildcards).
    pub topic_filter: String,

    /// Maximum QoS level the client wishes to receive.
    pub qos: QoS,

    // ── MQTT 5.0 subscription options ────────────────────────────────────────

    /// If `true`, the server must not forward messages published by *this*
    /// client back to itself on this subscription.  (5.0 only)
    pub no_local: bool,

    /// If `true`, the RETAIN flag in forwarded messages is set to the value
    /// the original publisher used, not cleared.  (5.0 only)
    pub retain_as_published: bool,

    /// Controls when retained messages are sent for this subscription.
    /// (5.0 only; defaults to `SendOnSubscribe` for 3.1.1 behaviour)
    pub retain_handling: RetainHandling,
}

impl SubscriptionFilter {
    /// Create a simple subscription filter (3.1.1 style — no 5.0 options).
    pub fn new(topic_filter: impl Into<String>, qos: QoS) -> Self {
        Self {
            topic_filter: topic_filter.into(),
            qos,
            no_local: false,
            retain_as_published: false,
            retain_handling: RetainHandling::SendOnSubscribe,
        }
    }
}

/// MQTT 5.0 retain handling options for a subscription (§3.8.3.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum RetainHandling {
    /// 0 — Send retained messages at subscription time.
    #[default]
    SendOnSubscribe = 0,
    /// 1 — Send retained messages only if this is a new subscription.
    SendOnNewSubscription = 1,
    /// 2 — Do not send retained messages at subscription time.
    DoNotSend = 2,
}

impl RetainHandling {
    pub fn from_u8(v: u8) -> Self {
        match v {
            1 => Self::SendOnNewSubscription,
            2 => Self::DoNotSend,
            _ => Self::SendOnSubscribe,
        }
    }
}

// ── SUBACK ────────────────────────────────────────────────────────────────────

/// SUBACK packet — broker acknowledges a SUBSCRIBE request.
#[derive(Debug, Clone, PartialEq)]
pub struct SubAck {
    /// Packet identifier matching the SUBSCRIBE.
    pub packet_id: u16,

    /// One reason code per filter in the SUBSCRIBE (in the same order).
    pub reason_codes: Vec<SubAckReason>,

    /// MQTT 5.0 SubAck Properties (None for 3.1.1).
    pub properties: Option<Properties>,
}

/// Reason codes for each filter in a SUBACK.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SubAckReason {
    /// 0x00 — Granted QoS 0.
    GrantedQoS0 = 0x00,
    /// 0x01 — Granted QoS 1.
    GrantedQoS1 = 0x01,
    /// 0x02 — Granted QoS 2.
    GrantedQoS2 = 0x02,
    /// 0x80 — Unspecified error / subscription rejected.
    UnspecifiedError = 0x80,
    /// 0x83 — Implementation specific error.
    ImplementationSpecificError = 0x83,
    /// 0x87 — Not authorized.
    NotAuthorized = 0x87,
    /// 0x8F — Topic filter invalid.
    TopicFilterInvalid = 0x8F,
    /// 0x91 — Packet identifier in use.
    PacketIdentifierInUse = 0x91,
    /// 0x97 — Quota exceeded.
    QuotaExceeded = 0x97,
    /// 0x9E — Shared subscriptions not supported.
    SharedSubscriptionsNotSupported = 0x9E,
    /// 0x9F — Connection rate exceeded.
    SubscriptionIdentifiersNotSupported = 0xA1,
    /// 0xA2 — Wildcard subscriptions not supported.
    WildcardSubscriptionsNotSupported = 0xA2,
}

impl SubAckReason {
    pub fn from_u8(v: u8) -> Self {
        match v {
            0x00 => Self::GrantedQoS0,
            0x01 => Self::GrantedQoS1,
            0x02 => Self::GrantedQoS2,
            0x83 => Self::ImplementationSpecificError,
            0x87 => Self::NotAuthorized,
            0x8F => Self::TopicFilterInvalid,
            0x91 => Self::PacketIdentifierInUse,
            0x97 => Self::QuotaExceeded,
            0x9E => Self::SharedSubscriptionsNotSupported,
            0xA1 => Self::SubscriptionIdentifiersNotSupported,
            0xA2 => Self::WildcardSubscriptionsNotSupported,
            _ => Self::UnspecifiedError,
        }
    }

    pub fn as_u8(self) -> u8 {
        self as u8
    }

    pub fn granted_qos(self) -> Option<QoS> {
        match self {
            Self::GrantedQoS0 => Some(QoS::AtMostOnce),
            Self::GrantedQoS1 => Some(QoS::AtLeastOnce),
            Self::GrantedQoS2 => Some(QoS::ExactlyOnce),
            _ => None,
        }
    }

    pub fn is_success(self) -> bool {
        self.granted_qos().is_some()
    }
}
