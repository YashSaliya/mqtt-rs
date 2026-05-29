//! PUBACK, PUBREC, PUBREL, PUBCOMP packets — §3.4 / §3.5 / §3.6 / §3.7
//!
//! These four packets form the QoS 1 and QoS 2 acknowledgement handshakes:
//!
//! QoS 1:  PUBLISH → PUBACK
//! QoS 2:  PUBLISH → PUBREC → PUBREL → PUBCOMP

use crate::properties::Properties;

// ── Shared reason codes for PUBACK/PUBREC/PUBREL/PUBCOMP ─────────────────────

/// Reason codes used in PUBACK, PUBREC, PUBREL, PUBCOMP (MQTT 5.0 §3.4.2).
/// For MQTT 3.1.1 only `Success` is used (no reason code on the wire).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum PubReason {
    #[default]
    Success = 0x00,
    NoMatchingSubscribers = 0x10,
    UnspecifiedError = 0x80,
    ImplementationSpecificError = 0x83,
    NotAuthorized = 0x87,
    TopicNameInvalid = 0x90,
    PacketIdentifierInUse = 0x91,
    PacketIdentifierNotFound = 0x92,
    QuotaExceeded = 0x97,
    PayloadFormatInvalid = 0x99,
}

impl PubReason {
    pub fn from_u8(v: u8) -> Self {
        match v {
            0x00 => Self::Success,
            0x10 => Self::NoMatchingSubscribers,
            0x80 => Self::UnspecifiedError,
            0x83 => Self::ImplementationSpecificError,
            0x87 => Self::NotAuthorized,
            0x90 => Self::TopicNameInvalid,
            0x91 => Self::PacketIdentifierInUse,
            0x92 => Self::PacketIdentifierNotFound,
            0x97 => Self::QuotaExceeded,
            0x99 => Self::PayloadFormatInvalid,
            _ => Self::UnspecifiedError,
        }
    }

    pub fn as_u8(self) -> u8 {
        self as u8
    }
}

// ── Macro to generate the four similar structs ────────────────────────────────

macro_rules! pub_ack_packet {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq)]
        pub struct $name {
            /// Packet identifier matching the original PUBLISH.
            pub packet_id: u16,
            /// Reason code (MQTT 5.0 only; always `Success` for 3.1.1).
            pub reason_code: PubReason,
            /// MQTT 5.0 properties (None for 3.1.1).
            pub properties: Option<Properties>,
        }

        impl $name {
            /// Create a simple success acknowledgement (works for both versions).
            pub fn success(packet_id: u16) -> Self {
                Self {
                    packet_id,
                    reason_code: PubReason::Success,
                    properties: None,
                }
            }
        }
    };
}

pub_ack_packet!(
    /// PUBACK — QoS 1 acknowledgement from receiver to sender.
    PubAck
);

pub_ack_packet!(
    /// PUBREC — QoS 2 step 1: receiver confirms it received the PUBLISH.
    PubRec
);

pub_ack_packet!(
    /// PUBREL — QoS 2 step 2: sender confirms it received the PUBREC.
    PubRel
);

pub_ack_packet!(
    /// PUBCOMP — QoS 2 step 3: receiver confirms delivery is complete.
    PubComp
);
