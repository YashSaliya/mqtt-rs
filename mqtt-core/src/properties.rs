//! MQTT 5.0 Properties — §2.2.2
//!
//! In MQTT 5.0 every control packet (and the Will in CONNECT) carries an
//! optional Properties section: a variable-length integer giving the byte
//! length of the section, followed by zero or more Property TLV entries.
//!
//! Each property is identified by a single byte (the Property Identifier)
//! followed by a value whose type is defined by the specification.
//!
//! We store all properties in one flat `Properties` struct.  Fields that are
//! not present on the wire are `None` (or an empty `Vec`).

use bytes::{Buf, BufMut, Bytes, BytesMut};
use crate::error::Error;

// ── Property identifiers (spec §2.2.2.2) ────────────────────────────────────
pub const PROP_PAYLOAD_FORMAT_INDICATOR:    u8 = 0x01;
pub const PROP_MESSAGE_EXPIRY_INTERVAL:     u8 = 0x02;
pub const PROP_CONTENT_TYPE:                u8 = 0x03;
pub const PROP_RESPONSE_TOPIC:              u8 = 0x08;
pub const PROP_CORRELATION_DATA:            u8 = 0x09;
pub const PROP_SUBSCRIPTION_IDENTIFIER:    u8 = 0x0B;
pub const PROP_SESSION_EXPIRY_INTERVAL:    u8 = 0x11;
pub const PROP_ASSIGNED_CLIENT_IDENTIFIER: u8 = 0x12;
pub const PROP_SERVER_KEEP_ALIVE:          u8 = 0x13;
pub const PROP_AUTHENTICATION_METHOD:      u8 = 0x15;
pub const PROP_AUTHENTICATION_DATA:        u8 = 0x16;
pub const PROP_REQUEST_PROBLEM_INFO:       u8 = 0x17;
pub const PROP_WILL_DELAY_INTERVAL:        u8 = 0x18;
pub const PROP_REQUEST_RESPONSE_INFO:      u8 = 0x19;
pub const PROP_RESPONSE_INFORMATION:       u8 = 0x1A;
pub const PROP_SERVER_REFERENCE:           u8 = 0x1C;
pub const PROP_REASON_STRING:              u8 = 0x1F;
pub const PROP_RECEIVE_MAXIMUM:            u8 = 0x21;
pub const PROP_TOPIC_ALIAS_MAXIMUM:        u8 = 0x22;
pub const PROP_TOPIC_ALIAS:                u8 = 0x23;
pub const PROP_MAXIMUM_QOS:                u8 = 0x24;
pub const PROP_RETAIN_AVAILABLE:           u8 = 0x25;
pub const PROP_USER_PROPERTY:              u8 = 0x26;
pub const PROP_MAXIMUM_PACKET_SIZE:        u8 = 0x27;
pub const PROP_WILDCARD_SUB_AVAILABLE:     u8 = 0x28;
pub const PROP_SUBSCRIPTION_ID_AVAILABLE:  u8 = 0x29;
pub const PROP_SHARED_SUB_AVAILABLE:       u8 = 0x2A;

// ── Properties struct ────────────────────────────────────────────────────────

/// All MQTT 5.0 properties that can appear in a packet.
///
/// The struct holds all possible properties; fields not present on the wire
/// remain `None` or empty.  User Properties (`0x26`) may appear multiple times
/// and are stored in a `Vec`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Properties {
    /// `0x01` — 0 = unspecified/bytes, 1 = UTF-8 encoded character data
    pub payload_format_indicator: Option<u8>,

    /// `0x02` — lifetime of the Application Message in seconds (PUBLISH)
    pub message_expiry_interval: Option<u32>,

    /// `0x03` — MIME content type of the message
    pub content_type: Option<String>,

    /// `0x08` — topic the receiver should publish a response to
    pub response_topic: Option<String>,

    /// `0x09` — opaque data for correlating request/response
    pub correlation_data: Option<Bytes>,

    /// `0x0B` — subscription identifier (1..=268_435_455), may repeat
    pub subscription_identifiers: Vec<u32>,

    /// `0x11` — session lifetime in seconds after disconnect
    pub session_expiry_interval: Option<u32>,

    /// `0x12` — client identifier assigned by the server
    pub assigned_client_identifier: Option<String>,

    /// `0x13` — keep alive the server wishes the client to use
    pub server_keep_alive: Option<u16>,

    /// `0x15` — name of the authentication method
    pub authentication_method: Option<String>,

    /// `0x16` — binary authentication data
    pub authentication_data: Option<Bytes>,

    /// `0x17` — 0 = server may omit reason string/user props on errors
    pub request_problem_information: Option<bool>,

    /// `0x18` — delay before sending Will message after disconnect
    pub will_delay_interval: Option<u32>,

    /// `0x19` — 1 = request response information in CONNACK
    pub request_response_information: Option<bool>,

    /// `0x1A` — basis for constructing response topics
    pub response_information: Option<String>,

    /// `0x1C` — another server the client can use
    pub server_reference: Option<String>,

    /// `0x1F` — human readable reason for an action
    pub reason_string: Option<String>,

    /// `0x21` — maximum inflight QoS 1/2 messages
    pub receive_maximum: Option<u16>,

    /// `0x22` — maximum topic alias value this endpoint will accept
    pub topic_alias_maximum: Option<u16>,

    /// `0x23` — numeric alias for a topic string (PUBLISH only)
    pub topic_alias: Option<u16>,

    /// `0x24` — maximum QoS the server supports
    pub maximum_qos: Option<u8>,

    /// `0x25` — false if server does not support retained messages
    pub retain_available: Option<bool>,

    /// `0x26` — arbitrary key-value pairs, may repeat
    pub user_properties: Vec<(String, String)>,

    /// `0x27` — maximum packet size this endpoint will accept
    pub maximum_packet_size: Option<u32>,

    /// `0x28` — false if server does not support wildcard subscriptions
    pub wildcard_subscription_available: Option<bool>,

    /// `0x29` — false if server does not support subscription identifiers
    pub subscription_identifier_available: Option<bool>,

    /// `0x2A` — false if server does not support shared subscriptions
    pub shared_subscription_available: Option<bool>,
}

impl Properties {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns true if no properties are set (empty section).
    pub fn is_empty(&self) -> bool {
        self == &Self::default()
    }

    // ── Decode ───────────────────────────────────────────────────────────────

    /// Decode a Properties section from `src`.
    ///
    /// The caller must have already verified that `src` contains at least as
    /// many bytes as the variable-length property-length prefix indicates.
    pub fn decode(src: &mut impl Buf) -> Result<Self, Error> {
        let prop_len = read_variable_length(src)?;
        if src.remaining() < prop_len {
            return Err(Error::Incomplete);
        }

        let mut section = src.take(prop_len);
        let mut p = Properties::default();

        while section.remaining() > 0 {
            let id = section.get_u8();
            match id {
                PROP_PAYLOAD_FORMAT_INDICATOR => {
                    p.payload_format_indicator = Some(section.get_u8());
                }
                PROP_MESSAGE_EXPIRY_INTERVAL => {
                    p.message_expiry_interval = Some(section.get_u32());
                }
                PROP_CONTENT_TYPE => {
                    p.content_type = Some(read_utf8_string(&mut section)?);
                }
                PROP_RESPONSE_TOPIC => {
                    p.response_topic = Some(read_utf8_string(&mut section)?);
                }
                PROP_CORRELATION_DATA => {
                    p.correlation_data = Some(read_binary_data(&mut section)?);
                }
                PROP_SUBSCRIPTION_IDENTIFIER => {
                    let sub_id = read_variable_length(&mut section)? as u32;
                    p.subscription_identifiers.push(sub_id);
                }
                PROP_SESSION_EXPIRY_INTERVAL => {
                    p.session_expiry_interval = Some(section.get_u32());
                }
                PROP_ASSIGNED_CLIENT_IDENTIFIER => {
                    p.assigned_client_identifier = Some(read_utf8_string(&mut section)?);
                }
                PROP_SERVER_KEEP_ALIVE => {
                    p.server_keep_alive = Some(section.get_u16());
                }
                PROP_AUTHENTICATION_METHOD => {
                    p.authentication_method = Some(read_utf8_string(&mut section)?);
                }
                PROP_AUTHENTICATION_DATA => {
                    p.authentication_data = Some(read_binary_data(&mut section)?);
                }
                PROP_REQUEST_PROBLEM_INFO => {
                    p.request_problem_information = Some(section.get_u8() != 0);
                }
                PROP_WILL_DELAY_INTERVAL => {
                    p.will_delay_interval = Some(section.get_u32());
                }
                PROP_REQUEST_RESPONSE_INFO => {
                    p.request_response_information = Some(section.get_u8() != 0);
                }
                PROP_RESPONSE_INFORMATION => {
                    p.response_information = Some(read_utf8_string(&mut section)?);
                }
                PROP_SERVER_REFERENCE => {
                    p.server_reference = Some(read_utf8_string(&mut section)?);
                }
                PROP_REASON_STRING => {
                    p.reason_string = Some(read_utf8_string(&mut section)?);
                }
                PROP_RECEIVE_MAXIMUM => {
                    p.receive_maximum = Some(section.get_u16());
                }
                PROP_TOPIC_ALIAS_MAXIMUM => {
                    p.topic_alias_maximum = Some(section.get_u16());
                }
                PROP_TOPIC_ALIAS => {
                    p.topic_alias = Some(section.get_u16());
                }
                PROP_MAXIMUM_QOS => {
                    p.maximum_qos = Some(section.get_u8());
                }
                PROP_RETAIN_AVAILABLE => {
                    p.retain_available = Some(section.get_u8() != 0);
                }
                PROP_USER_PROPERTY => {
                    let key = read_utf8_string(&mut section)?;
                    let val = read_utf8_string(&mut section)?;
                    p.user_properties.push((key, val));
                }
                PROP_MAXIMUM_PACKET_SIZE => {
                    p.maximum_packet_size = Some(section.get_u32());
                }
                PROP_WILDCARD_SUB_AVAILABLE => {
                    p.wildcard_subscription_available = Some(section.get_u8() != 0);
                }
                PROP_SUBSCRIPTION_ID_AVAILABLE => {
                    p.subscription_identifier_available = Some(section.get_u8() != 0);
                }
                PROP_SHARED_SUB_AVAILABLE => {
                    p.shared_subscription_available = Some(section.get_u8() != 0);
                }
                unknown => return Err(Error::UnknownPropertyId(unknown)),
            }
        }

        Ok(p)
    }

    // ── Encode ───────────────────────────────────────────────────────────────

    /// Encode the Properties section into `dst` (including the length prefix).
    pub fn encode(&self, dst: &mut BytesMut) {
        // Encode properties into a temporary buffer first so we know the length.
        let mut tmp = BytesMut::new();
        self.encode_properties_into(&mut tmp);
        write_variable_length(dst, tmp.len());
        dst.put(tmp);
    }

    /// Encoded byte length of this Properties section (including the length prefix).
    pub fn encoded_len(&self) -> usize {
        let mut tmp = BytesMut::new();
        self.encode_properties_into(&mut tmp);
        let payload_len = tmp.len();
        variable_length_size(payload_len) + payload_len
    }

    fn encode_properties_into(&self, buf: &mut BytesMut) {
        macro_rules! put_u8_prop {
            ($id:expr, $val:expr) => {
                if let Some(v) = $val {
                    buf.put_u8($id);
                    buf.put_u8(v as u8);
                }
            };
        }
        macro_rules! put_u16_prop {
            ($id:expr, $val:expr) => {
                if let Some(v) = $val {
                    buf.put_u8($id);
                    buf.put_u16(v);
                }
            };
        }
        macro_rules! put_u32_prop {
            ($id:expr, $val:expr) => {
                if let Some(v) = $val {
                    buf.put_u8($id);
                    buf.put_u32(v);
                }
            };
        }
        macro_rules! put_str_prop {
            ($id:expr, $val:expr) => {
                if let Some(ref v) = $val {
                    buf.put_u8($id);
                    write_utf8_string(buf, v);
                }
            };
        }
        macro_rules! put_bytes_prop {
            ($id:expr, $val:expr) => {
                if let Some(ref v) = $val {
                    buf.put_u8($id);
                    write_binary_data(buf, v);
                }
            };
        }
        macro_rules! put_bool_prop {
            ($id:expr, $val:expr) => {
                if let Some(v) = $val {
                    buf.put_u8($id);
                    buf.put_u8(if v { 1 } else { 0 });
                }
            };
        }

        put_u8_prop!(PROP_PAYLOAD_FORMAT_INDICATOR, self.payload_format_indicator);
        put_u32_prop!(PROP_MESSAGE_EXPIRY_INTERVAL, self.message_expiry_interval);
        put_str_prop!(PROP_CONTENT_TYPE, self.content_type);
        put_str_prop!(PROP_RESPONSE_TOPIC, self.response_topic);
        put_bytes_prop!(PROP_CORRELATION_DATA, self.correlation_data);
        for &sub_id in &self.subscription_identifiers {
            buf.put_u8(PROP_SUBSCRIPTION_IDENTIFIER);
            write_variable_length(buf, sub_id as usize);
        }
        put_u32_prop!(PROP_SESSION_EXPIRY_INTERVAL, self.session_expiry_interval);
        put_str_prop!(PROP_ASSIGNED_CLIENT_IDENTIFIER, self.assigned_client_identifier);
        put_u16_prop!(PROP_SERVER_KEEP_ALIVE, self.server_keep_alive);
        put_str_prop!(PROP_AUTHENTICATION_METHOD, self.authentication_method);
        put_bytes_prop!(PROP_AUTHENTICATION_DATA, self.authentication_data);
        put_bool_prop!(PROP_REQUEST_PROBLEM_INFO, self.request_problem_information);
        put_u32_prop!(PROP_WILL_DELAY_INTERVAL, self.will_delay_interval);
        put_bool_prop!(PROP_REQUEST_RESPONSE_INFO, self.request_response_information);
        put_str_prop!(PROP_RESPONSE_INFORMATION, self.response_information);
        put_str_prop!(PROP_SERVER_REFERENCE, self.server_reference);
        put_str_prop!(PROP_REASON_STRING, self.reason_string);
        put_u16_prop!(PROP_RECEIVE_MAXIMUM, self.receive_maximum);
        put_u16_prop!(PROP_TOPIC_ALIAS_MAXIMUM, self.topic_alias_maximum);
        put_u16_prop!(PROP_TOPIC_ALIAS, self.topic_alias);
        put_u8_prop!(PROP_MAXIMUM_QOS, self.maximum_qos);
        put_bool_prop!(PROP_RETAIN_AVAILABLE, self.retain_available);
        for (k, v) in &self.user_properties {
            buf.put_u8(PROP_USER_PROPERTY);
            write_utf8_string(buf, k);
            write_utf8_string(buf, v);
        }
        put_u32_prop!(PROP_MAXIMUM_PACKET_SIZE, self.maximum_packet_size);
        put_bool_prop!(PROP_WILDCARD_SUB_AVAILABLE, self.wildcard_subscription_available);
        put_bool_prop!(PROP_SUBSCRIPTION_ID_AVAILABLE, self.subscription_identifier_available);
        put_bool_prop!(PROP_SHARED_SUB_AVAILABLE, self.shared_subscription_available);
    }
}

// ── Wire helpers ─────────────────────────────────────────────────────────────

/// Read a MQTT variable-length integer (1–4 bytes, §1.5.5).
pub fn read_variable_length(src: &mut impl Buf) -> Result<usize, Error> {
    let mut result: usize = 0;
    let mut shift = 0usize;

    loop {
        if !src.has_remaining() {
            return Err(Error::Incomplete);
        }
        let byte = src.get_u8() as usize;
        result |= (byte & 0x7F) << shift;
        shift += 7;
        if byte & 0x80 == 0 {
            break;
        }
        if shift > 21 {
            return Err(Error::MalformedRemainingLength);
        }
    }

    Ok(result)
}

/// Write a MQTT variable-length integer into `dst`.
pub fn write_variable_length(dst: &mut BytesMut, mut value: usize) {
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;
        if value > 0 {
            byte |= 0x80;
        }
        dst.put_u8(byte);
        if value == 0 {
            break;
        }
    }
}

/// Number of bytes a variable-length integer encoding will occupy.
pub fn variable_length_size(value: usize) -> usize {
    match value {
        0..=127 => 1,
        128..=16_383 => 2,
        16_384..=2_097_151 => 3,
        _ => 4,
    }
}

/// Read a UTF-8 string: 2-byte big-endian length prefix + bytes (§1.5.4).
pub fn read_utf8_string(src: &mut impl Buf) -> Result<String, Error> {
    if src.remaining() < 2 {
        return Err(Error::Incomplete);
    }
    let len = src.get_u16() as usize;
    if src.remaining() < len {
        return Err(Error::Incomplete);
    }
    let mut bytes = vec![0u8; len];
    src.copy_to_slice(&mut bytes);
    String::from_utf8(bytes).map_err(|_| Error::InvalidUtf8)
}

/// Write a UTF-8 string: 2-byte big-endian length + bytes.
pub fn write_utf8_string(dst: &mut BytesMut, s: &str) {
    dst.put_u16(s.len() as u16);
    dst.put_slice(s.as_bytes());
}

/// Read binary data: 2-byte big-endian length prefix + bytes (§1.5.6).
pub fn read_binary_data(src: &mut impl Buf) -> Result<Bytes, Error> {
    if src.remaining() < 2 {
        return Err(Error::Incomplete);
    }
    let len = src.get_u16() as usize;
    if src.remaining() < len {
        return Err(Error::Incomplete);
    }
    Ok(src.copy_to_bytes(len))
}

/// Write binary data: 2-byte big-endian length + bytes.
pub fn write_binary_data(dst: &mut BytesMut, data: &[u8]) {
    dst.put_u16(data.len() as u16);
    dst.put_slice(data);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_variable_length() {
        for &value in &[0usize, 127, 128, 16_383, 16_384, 2_097_151, 268_435_455] {
            let mut buf = BytesMut::new();
            write_variable_length(&mut buf, value);
            let decoded = read_variable_length(&mut buf.freeze().as_ref()).unwrap();
            assert_eq!(value, decoded, "roundtrip failed for {}", value);
        }
    }

    #[test]
    fn roundtrip_properties() {
        let mut props = Properties::new();
        props.message_expiry_interval = Some(3600);
        props.response_topic = Some("reply/here".into());
        props.user_properties.push(("unit".into(), "celsius".into()));
        props.user_properties.push(("sensor".into(), "A1".into()));
        props.receive_maximum = Some(20);
        props.session_expiry_interval = Some(300);

        let mut buf = BytesMut::new();
        props.encode(&mut buf);

        let decoded = Properties::decode(&mut buf.freeze().as_ref()).unwrap();
        assert_eq!(props, decoded);
    }
}
