//! CONNECT and CONNACK packets — §3.1 / §3.2

use bytes::Bytes;
use crate::{QoS, properties::Properties};

// ── CONNECT ───────────────────────────────────────────────────────────────────

/// CONNECT packet sent by a client to initiate a session with the broker.
#[derive(Debug, Clone, PartialEq)]
pub struct Connect {
    // ── Fixed fields (both versions) ─────────────────────────────────────────
    /// The client identifier. May be empty string (broker assigns one).
    pub client_id: String,

    /// If `true`, start a fresh session; discard any previous session state.
    /// In MQTT 5.0 this is combined with `session_expiry_interval` in Properties.
    pub clean_start: bool,

    /// Keep-alive period in seconds. `0` means no keep-alive timeout.
    pub keep_alive: u16,

    /// Optional username for authentication.
    pub username: Option<String>,

    /// Optional password for authentication.
    pub password: Option<Bytes>,

    /// Optional Last Will message (published by broker on unexpected disconnect).
    pub will: Option<Will>,

    /// MQTT 5.0 Connect Properties (None for 3.1.1).
    pub properties: Option<Properties>,
}

/// The Last Will message embedded in a CONNECT packet.
#[derive(Debug, Clone, PartialEq)]
pub struct Will {
    pub topic: String,
    pub payload: Bytes,
    pub qos: QoS,
    pub retain: bool,
    /// MQTT 5.0 Will Properties (None for 3.1.1).
    pub properties: Option<Properties>,
}

// ── CONNACK ───────────────────────────────────────────────────────────────────

/// CONNACK packet sent by the broker to acknowledge a CONNECT.
#[derive(Debug, Clone, PartialEq)]
pub struct ConnAck {
    /// If `true`, the broker has restored a previous session.
    pub session_present: bool,

    /// MQTT 3.1.1 return code *or* MQTT 5.0 reason code.
    pub reason_code: ConnectReason,

    /// MQTT 5.0 CONNACK Properties (None for 3.1.1).
    pub properties: Option<Properties>,
}

/// Unified reason/return code for CONNACK.
///
/// The numeric values match the MQTT 5.0 reason code table.  The 3.1.1 return
/// codes map to the same numeric values for the common cases.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ConnectReason {
    // ── Success ───────────────────────────────────────────────────────────────
    /// 0x00 — Connection accepted.
    Success = 0x00,

    // ── MQTT 3.1.1 return codes (also valid 5.0 codes) ────────────────────────
    /// 0x01 — 3.1.1 only: Unacceptable protocol version.
    UnacceptableProtocolVersion = 0x01,
    /// 0x02 — 3.1.1 only: Client identifier rejected.
    IdentifierRejected = 0x02,
    /// 0x03 — 3.1.1 only: Server unavailable.
    ServerUnavailable = 0x03,
    /// 0x04 — 3.1.1 only: Bad username or password.
    BadUserNameOrPassword = 0x04,
    /// 0x05 — 3.1.1 only: Not authorized.
    NotAuthorized = 0x05,

    // ── MQTT 5.0 reason codes ─────────────────────────────────────────────────
    /// 0x80 — Unspecified error.
    UnspecifiedError = 0x80,
    /// 0x81 — Malformed packet.
    MalformedPacket = 0x81,
    /// 0x82 — Protocol error.
    ProtocolError = 0x82,
    /// 0x83 — Implementation specific error.
    ImplementationSpecificError = 0x83,
    /// 0x84 — Unsupported protocol version.
    UnsupportedProtocolVersion = 0x84,
    /// 0x85 — Client identifier not valid.
    ClientIdentifierNotValid = 0x85,
    /// 0x86 — Bad username or password (5.0 variant).
    BadUsernameOrPassword = 0x86,
    /// 0x87 — Not authorized (5.0 variant).
    NotAuthorized5 = 0x87,
    /// 0x88 — Server unavailable (5.0 variant).
    ServerUnavailable5 = 0x88,
    /// 0x89 — Server busy.
    ServerBusy = 0x89,
    /// 0x8A — Banned.
    Banned = 0x8A,
    /// 0x8C — Bad authentication method.
    BadAuthenticationMethod = 0x8C,
    /// 0x90 — Topic name invalid.
    TopicNameInvalid = 0x90,
    /// 0x95 — Packet too large.
    PacketTooLarge = 0x95,
    /// 0x97 — Quota exceeded.
    QuotaExceeded = 0x97,
    /// 0x99 — Payload format invalid.
    PayloadFormatInvalid = 0x99,
    /// 0x9A — Retain not supported.
    RetainNotSupported = 0x9A,
    /// 0x9B — QoS not supported.
    QoSNotSupported = 0x9B,
    /// 0x9C — Use another server.
    UseAnotherServer = 0x9C,
    /// 0x9D — Server moved.
    ServerMoved = 0x9D,
    /// 0x9F — Connection rate exceeded.
    ConnectionRateExceeded = 0x9F,
}

impl ConnectReason {
    pub fn from_u8(v: u8) -> Self {
        match v {
            0x00 => Self::Success,
            0x01 => Self::UnacceptableProtocolVersion,
            0x02 => Self::IdentifierRejected,
            0x03 => Self::ServerUnavailable,
            0x04 => Self::BadUserNameOrPassword,
            0x05 => Self::NotAuthorized,
            0x80 => Self::UnspecifiedError,
            0x81 => Self::MalformedPacket,
            0x82 => Self::ProtocolError,
            0x83 => Self::ImplementationSpecificError,
            0x84 => Self::UnsupportedProtocolVersion,
            0x85 => Self::ClientIdentifierNotValid,
            0x86 => Self::BadUsernameOrPassword,
            0x87 => Self::NotAuthorized5,
            0x88 => Self::ServerUnavailable5,
            0x89 => Self::ServerBusy,
            0x8A => Self::Banned,
            0x8C => Self::BadAuthenticationMethod,
            0x90 => Self::TopicNameInvalid,
            0x95 => Self::PacketTooLarge,
            0x97 => Self::QuotaExceeded,
            0x99 => Self::PayloadFormatInvalid,
            0x9A => Self::RetainNotSupported,
            0x9B => Self::QoSNotSupported,
            0x9C => Self::UseAnotherServer,
            0x9D => Self::ServerMoved,
            0x9F => Self::ConnectionRateExceeded,
            _ => Self::UnspecifiedError,
        }
    }

    pub fn as_u8(self) -> u8 {
        self as u8
    }

    pub fn is_success(self) -> bool {
        self == Self::Success
    }
}
