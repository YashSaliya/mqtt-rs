use crate::error::Error;

/// The MQTT protocol version negotiated during the CONNECT handshake.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProtocolVersion {
    /// MQTT 3.1.1  — protocol level byte = 4
    V311,
    /// MQTT 5.0    — protocol level byte = 5
    V500,
}

impl ProtocolVersion {
    /// Decode from the protocol level byte in the CONNECT packet.
    pub fn from_u8(level: u8) -> Result<Self, Error> {
        match level {
            4 => Ok(ProtocolVersion::V311),
            5 => Ok(ProtocolVersion::V500),
            v => Err(Error::UnsupportedProtocolVersion(v)),
        }
    }

    /// Encode back to the wire byte.
    pub fn as_u8(self) -> u8 {
        match self {
            ProtocolVersion::V311 => 4,
            ProtocolVersion::V500 => 5,
        }
    }

    pub fn is_v5(self) -> bool {
        matches!(self, ProtocolVersion::V500)
    }
}

impl std::fmt::Display for ProtocolVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProtocolVersion::V311 => write!(f, "MQTT 3.1.1"),
            ProtocolVersion::V500 => write!(f, "MQTT 5.0"),
        }
    }
}
