//! All MQTT control packet types.
//!
//! MQTT 3.1.1 defines 14 packet types (§2.1.1).
//! MQTT 5.0 adds a 15th: AUTH.

pub mod auth;
pub mod connect;
pub mod disconnect;
pub mod ping;
pub mod puback;
pub mod publish;
pub mod suback;
pub mod subscribe;
pub mod unsuback;
pub mod unsubscribe;

pub use auth::Auth;
pub use connect::{ConnAck, Connect, ConnectReason, Will};
pub use disconnect::Disconnect;
pub use ping::{PingReq, PingResp};
pub use puback::{PubAck, PubComp, PubRec, PubRel};
pub use publish::Publish;
pub use suback::SubAck;
pub use subscribe::{RetainHandling, Subscribe, SubscriptionFilter};
pub use unsuback::UnsubAck;
pub use unsubscribe::Unsubscribe;

/// A fully-decoded MQTT control packet (either version).
#[derive(Debug, Clone, PartialEq)]
pub enum ControlPacket {
    Connect(Connect),
    ConnAck(ConnAck),
    Publish(Publish),
    PubAck(PubAck),
    PubRec(PubRec),
    PubRel(PubRel),
    PubComp(PubComp),
    Subscribe(Subscribe),
    SubAck(SubAck),
    Unsubscribe(Unsubscribe),
    UnsubAck(UnsubAck),
    PingReq(PingReq),
    PingResp(PingResp),
    Disconnect(Disconnect),
    /// MQTT 5.0 only.
    Auth(Auth),
}

impl ControlPacket {
    pub fn packet_type_name(&self) -> &'static str {
        match self {
            ControlPacket::Connect(_) => "CONNECT",
            ControlPacket::ConnAck(_) => "CONNACK",
            ControlPacket::Publish(_) => "PUBLISH",
            ControlPacket::PubAck(_) => "PUBACK",
            ControlPacket::PubRec(_) => "PUBREC",
            ControlPacket::PubRel(_) => "PUBREL",
            ControlPacket::PubComp(_) => "PUBCOMP",
            ControlPacket::Subscribe(_) => "SUBSCRIBE",
            ControlPacket::SubAck(_) => "SUBACK",
            ControlPacket::Unsubscribe(_) => "UNSUBSCRIBE",
            ControlPacket::UnsubAck(_) => "UNSUBACK",
            ControlPacket::PingReq(_) => "PINGREQ",
            ControlPacket::PingResp(_) => "PINGRESP",
            ControlPacket::Disconnect(_) => "DISCONNECT",
            ControlPacket::Auth(_) => "AUTH",
        }
    }
}
