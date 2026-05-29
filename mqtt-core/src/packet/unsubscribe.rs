//! UNSUBSCRIBE and UNSUBACK packets — §3.10 / §3.11

use crate::properties::Properties;

/// UNSUBSCRIBE packet — client requests removal of subscriptions.
#[derive(Debug, Clone, PartialEq)]
pub struct Unsubscribe {
    /// Packet identifier (must be > 0).
    pub packet_id: u16,

    /// Topic filters to unsubscribe from.
    pub filters: Vec<String>,

    /// MQTT 5.0 Unsubscribe Properties (None for 3.1.1).
    pub properties: Option<Properties>,
}

/// UNSUBACK packet — broker acknowledges an UNSUBSCRIBE.
#[derive(Debug, Clone, PartialEq)]
pub struct UnsubAck {
    /// Packet identifier matching the UNSUBSCRIBE.
    pub packet_id: u16,

    /// MQTT 5.0: one reason code per filter. Empty for 3.1.1.
    pub reason_codes: Vec<UnsubAckReason>,

    /// MQTT 5.0 UnsubAck Properties (None for 3.1.1).
    pub properties: Option<Properties>,
}

/// Reason codes for each filter in an UNSUBACK (MQTT 5.0 §3.11.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum UnsubAckReason {
    /// 0x00 — Subscription deleted.
    Success = 0x00,
    /// 0x11 — No subscription existed.
    NoSubscriptionExisted = 0x11,
    /// 0x80 — Unspecified error.
    UnspecifiedError = 0x80,
    /// 0x83 — Implementation specific error.
    ImplementationSpecificError = 0x83,
    /// 0x87 — Not authorized.
    NotAuthorized = 0x87,
    /// 0x8F — Topic filter invalid.
    TopicFilterInvalid = 0x8F,
    /// 0x91 — Packet identifier in use.
    PacketIdentifierInUse = 0x91,
}

impl UnsubAckReason {
    pub fn from_u8(v: u8) -> Self {
        match v {
            0x00 => Self::Success,
            0x11 => Self::NoSubscriptionExisted,
            0x83 => Self::ImplementationSpecificError,
            0x87 => Self::NotAuthorized,
            0x8F => Self::TopicFilterInvalid,
            0x91 => Self::PacketIdentifierInUse,
            _ => Self::UnspecifiedError,
        }
    }

    pub fn as_u8(self) -> u8 {
        self as u8
    }
}
