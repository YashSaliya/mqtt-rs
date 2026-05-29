//! DISCONNECT packet — §3.14
//!
//! In MQTT 3.1.1 DISCONNECT has only a fixed header (client → broker only).
//! In MQTT 5.0 DISCONNECT carries a reason code and properties, and may be
//! sent by either the client OR the broker.

use crate::properties::Properties;

/// DISCONNECT packet.
#[derive(Debug, Clone, PartialEq)]
pub struct Disconnect {
    /// Reason code (MQTT 5.0 only; `0x00 = Normal disconnection` for 3.1.1).
    pub reason_code: DisconnectReason,

    /// MQTT 5.0 Disconnect Properties (None for 3.1.1).
    pub properties: Option<Properties>,
}

impl Disconnect {
    /// Create a normal disconnect (works for both 3.1.1 and 5.0).
    pub fn normal() -> Self {
        Self {
            reason_code: DisconnectReason::NormalDisconnection,
            properties: None,
        }
    }
}

impl Default for Disconnect {
    fn default() -> Self {
        Self::normal()
    }
}

/// Reason codes for DISCONNECT (MQTT 5.0 §3.14.2.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum DisconnectReason {
    #[default]
    NormalDisconnection = 0x00,
    DisconnectWithWillMessage = 0x04,
    UnspecifiedError = 0x80,
    MalformedPacket = 0x81,
    ProtocolError = 0x82,
    ImplementationSpecificError = 0x83,
    NotAuthorized = 0x87,
    ServerBusy = 0x89,
    ServerShuttingDown = 0x8B,
    KeepAliveTimeout = 0x8D,
    SessionTakenOver = 0x8E,
    TopicFilterInvalid = 0x8F,
    TopicNameInvalid = 0x90,
    ReceiveMaximumExceeded = 0x93,
    TopicAliasInvalid = 0x94,
    PacketTooLarge = 0x95,
    MessageRateTooHigh = 0x96,
    QuotaExceeded = 0x97,
    AdministrativeAction = 0x98,
    PayloadFormatInvalid = 0x99,
    RetainNotSupported = 0x9A,
    QoSNotSupported = 0x9B,
    UseAnotherServer = 0x9C,
    ServerMoved = 0x9D,
    SharedSubscriptionsNotSupported = 0x9E,
    ConnectionRateExceeded = 0x9F,
    MaximumConnectTime = 0xA0,
    SubscriptionIdentifiersNotSupported = 0xA1,
    WildcardSubscriptionsNotSupported = 0xA2,
}

impl DisconnectReason {
    pub fn from_u8(v: u8) -> Self {
        match v {
            0x00 => Self::NormalDisconnection,
            0x04 => Self::DisconnectWithWillMessage,
            0x80 => Self::UnspecifiedError,
            0x81 => Self::MalformedPacket,
            0x82 => Self::ProtocolError,
            0x83 => Self::ImplementationSpecificError,
            0x87 => Self::NotAuthorized,
            0x89 => Self::ServerBusy,
            0x8B => Self::ServerShuttingDown,
            0x8D => Self::KeepAliveTimeout,
            0x8E => Self::SessionTakenOver,
            0x8F => Self::TopicFilterInvalid,
            0x90 => Self::TopicNameInvalid,
            0x93 => Self::ReceiveMaximumExceeded,
            0x94 => Self::TopicAliasInvalid,
            0x95 => Self::PacketTooLarge,
            0x96 => Self::MessageRateTooHigh,
            0x97 => Self::QuotaExceeded,
            0x98 => Self::AdministrativeAction,
            0x99 => Self::PayloadFormatInvalid,
            0x9A => Self::RetainNotSupported,
            0x9B => Self::QoSNotSupported,
            0x9C => Self::UseAnotherServer,
            0x9D => Self::ServerMoved,
            0x9E => Self::SharedSubscriptionsNotSupported,
            0x9F => Self::ConnectionRateExceeded,
            0xA0 => Self::MaximumConnectTime,
            0xA1 => Self::SubscriptionIdentifiersNotSupported,
            0xA2 => Self::WildcardSubscriptionsNotSupported,
            _ => Self::UnspecifiedError,
        }
    }

    pub fn as_u8(self) -> u8 {
        self as u8
    }
}
