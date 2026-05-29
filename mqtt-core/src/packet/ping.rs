//! PINGREQ and PINGRESP packets — §3.12 / §3.13
//!
//! Both packets have only a fixed header — no variable header, no payload.
//! They are used to maintain the keep-alive connection.

/// PINGREQ — client → broker: "I'm still alive."
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PingReq;

/// PINGRESP — broker → client: "I see you."
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PingResp;
