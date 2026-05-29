//! AUTH packet — §3.15 (MQTT 5.0 only)
//!
//! Used to perform enhanced authentication (challenge-response) between client
//! and broker after the initial CONNECT is sent.
//!
//! Flow:
//!   Client → Broker: CONNECT { authentication_method, authentication_data }
//!   Broker → Client: AUTH    { reason=ContinueAuthentication, data=challenge }
//!   Client → Broker: AUTH    { reason=ContinueAuthentication, data=response  }
//!   Broker → Client: CONNACK { reason=Success }

use crate::properties::Properties;

/// AUTH packet — MQTT 5.0 §3.15.
#[derive(Debug, Clone, PartialEq)]
pub struct Auth {
    /// The authentication reason code.
    pub reason_code: AuthReason,

    /// AUTH Properties (must include `authentication_method`).
    pub properties: Option<Properties>,
}

/// Reason codes for the AUTH packet (§3.15.2.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum AuthReason {
    /// 0x00 — Authentication is successful.
    #[default]
    Success = 0x00,
    /// 0x18 — Server sends this to initiate or continue a challenge.
    ContinueAuthentication = 0x18,
    /// 0x19 — Client initiates a re-authentication of the current connection.
    ReAuthenticate = 0x19,
}

impl AuthReason {
    pub fn from_u8(v: u8) -> Self {
        match v {
            0x00 => Self::Success,
            0x18 => Self::ContinueAuthentication,
            0x19 => Self::ReAuthenticate,
            _ => Self::Success,
        }
    }

    pub fn as_u8(self) -> u8 {
        self as u8
    }
}
