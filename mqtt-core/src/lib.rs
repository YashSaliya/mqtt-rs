pub mod codec;
pub mod error;
pub mod packet;
pub mod properties;
pub mod topic;
pub mod version;

pub use error::Error;
pub use packet::ControlPacket;
pub use properties::Properties;
pub use version::ProtocolVersion;

/// Quality of Service level for MQTT message delivery.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum QoS {
    /// At most once delivery — fire and forget, no ACK.
    AtMostOnce = 0,
    /// At least once delivery — PUBACK handshake.
    AtLeastOnce = 1,
    /// Exactly once delivery — PUBREC → PUBREL → PUBCOMP handshake.
    ExactlyOnce = 2,
}

impl QoS {
    pub fn from_u8(v: u8) -> Result<Self, Error> {
        match v {
            0 => Ok(QoS::AtMostOnce),
            1 => Ok(QoS::AtLeastOnce),
            2 => Ok(QoS::ExactlyOnce),
            _ => Err(Error::InvalidQoS(v)),
        }
    }

    pub fn as_u8(self) -> u8 {
        self as u8
    }
}
