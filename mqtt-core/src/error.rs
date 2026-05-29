use thiserror::Error;

/// All errors that can occur in the mqtt-core codec layer.
#[derive(Debug, Error, Clone, PartialEq)]
pub enum Error {
    // ── Decoding ──────────────────────────────────────────────────────────────
    #[error("not enough data to decode packet (need more bytes)")]
    Incomplete,

    #[error("remaining length encoding is malformed (> 4 bytes)")]
    MalformedRemainingLength,

    #[error("packet too large: {0} bytes exceeds maximum allowed")]
    PacketTooLarge(usize),

    #[error("unknown control packet type: {0}")]
    UnknownPacketType(u8),

    #[error("invalid protocol name: expected 'MQTT', got '{0}'")]
    InvalidProtocolName(String),

    #[error("unsupported protocol version: {0} (supported: 4=3.1.1, 5=5.0)")]
    UnsupportedProtocolVersion(u8),

    #[error("invalid QoS value: {0}")]
    InvalidQoS(u8),

    #[error("invalid topic: {0}")]
    InvalidTopic(String),

    #[error("reserved flag bits are set in fixed header")]
    ReservedFlagBits,

    #[error("packet identifier is zero (not allowed for QoS 1/2)")]
    ZeroPacketId,

    #[error("invalid UTF-8 string in packet")]
    InvalidUtf8,

    #[error("invalid property identifier: 0x{0:02X}")]
    UnknownPropertyId(u8),

    #[error("property 0x{id:02X} appeared {count} times (max allowed: {max})")]
    DuplicateProperty { id: u8, count: usize, max: usize },

    #[error("packet payload is malformed: {0}")]
    MalformedPayload(String),

    // ── Encoding ──────────────────────────────────────────────────────────────
    #[error("topic alias {0} has not been registered")]
    UnknownTopicAlias(u16),

    #[error("topic alias {0} exceeds negotiated maximum {1}")]
    TopicAliasExceedsMax(u16, u16),
}
