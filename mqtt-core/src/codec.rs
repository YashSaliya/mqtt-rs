//! MQTT frame codec — encode and decode all 15 control packet types.
//!
//! This is a **Sans-I/O** codec: it operates purely on `bytes::BytesMut`
//! buffers.  No sockets, no async, no threads.  The I/O layer (in
//! `mqtt-broker`) calls `decode` each time new bytes arrive on the socket
//! and `encode` to write a packet to a send buffer.
//!
//! ## Decoding
//!
//! The CONNECT packet contains the protocol version byte, so decoding is
//! bootstrapped: we always try to parse a CONNECT first with
//! [`decode_connect`]; after that the version is known and subsequent packets
//! are decoded with [`decode`].
//!
//! ## Wire format reminder (§2.1)
//!
//! ```text
//! ┌──────────────────────────┐
//! │  Fixed Header  (≥ 2 B)   │  byte[0]: packet-type (4b) | flags (4b)
//! │                          │  byte[1..]: remaining length (variable)
//! ├──────────────────────────┤
//! │  Variable Header (opt.)  │
//! ├──────────────────────────┤
//! │  Payload       (opt.)    │
//! └──────────────────────────┘
//! ```

use bytes::{Buf, BufMut, BytesMut};

use crate::{
    error::Error,
    packet::{
        auth::{Auth, AuthReason},
        connect::{ConnAck, Connect, ConnectReason, Will},
        disconnect::{Disconnect, DisconnectReason},
        ping::{PingReq, PingResp},
        puback::{PubAck, PubComp, PubRec, PubRel, PubReason},
        publish::Publish,
        subscribe::{RetainHandling, Subscribe, SubscriptionFilter},
        suback::{SubAck, SubAckReason},
        unsubscribe::Unsubscribe,
        unsuback::{UnsubAck, UnsubAckReason},
        ControlPacket,
    },
    properties::{
        read_binary_data, read_utf8_string, read_variable_length,
        write_binary_data, write_utf8_string, write_variable_length,
        Properties,
    },
    version::ProtocolVersion,
    QoS,
};

// ── Packet type nibbles (top 4 bits of byte 0) ────────────────────────────────
const TYPE_CONNECT:     u8 = 1;
const TYPE_CONNACK:     u8 = 2;
const TYPE_PUBLISH:     u8 = 3;
const TYPE_PUBACK:      u8 = 4;
const TYPE_PUBREC:      u8 = 5;
const TYPE_PUBREL:      u8 = 6;
const TYPE_PUBCOMP:     u8 = 7;
const TYPE_SUBSCRIBE:   u8 = 8;
const TYPE_SUBACK:      u8 = 9;
const TYPE_UNSUBSCRIBE: u8 = 10;
const TYPE_UNSUBACK:    u8 = 11;
const TYPE_PINGREQ:     u8 = 12;
const TYPE_PINGRESP:    u8 = 13;
const TYPE_DISCONNECT:  u8 = 14;
const TYPE_AUTH:        u8 = 15;

/// Maximum packet size we will attempt to decode (16 MiB).
/// A real broker would make this configurable.
const MAX_PACKET_SIZE: usize = 16 * 1024 * 1024;

// ═══════════════════════════════════════════════════════════════════════════════
// Public API
// ═══════════════════════════════════════════════════════════════════════════════

/// Try to decode **one** MQTT packet from `src`.
///
/// Returns `Ok(None)` when there are not yet enough bytes for a complete
/// packet (caller should read more data and call again).
///
/// The `version` is the protocol version that was negotiated during CONNECT.
/// Pass `None` only when decoding the very first packet (which must be
/// CONNECT and which contains the version itself).
pub fn decode(
    src: &mut BytesMut,
    version: Option<ProtocolVersion>,
) -> Result<Option<ControlPacket>, Error> {
    // We need at least 2 bytes (fixed header byte + first remaining-length byte).
    if src.len() < 2 {
        return Ok(None);
    }

    // Peek at the fixed header without consuming bytes yet.
    let first_byte = src[0];
    let packet_type = first_byte >> 4;
    let flags = first_byte & 0x0F;

    // Decode the remaining-length field (1–4 bytes starting at src[1]).
    let (remaining_len, header_len) = {
        let mut tmp = &src[1..];
        let len = match read_variable_length(&mut tmp) {
            Ok(l) => l,
            Err(Error::Incomplete) => return Ok(None),
            Err(e) => return Err(e),
        };
        // header_len = 1 (first byte) + bytes consumed by variable-length
        let consumed = src.len() - 1 - tmp.remaining();
        (len, 1 + consumed)
    };

    let total_len = header_len + remaining_len;
    if total_len > MAX_PACKET_SIZE {
        return Err(Error::PacketTooLarge(total_len));
    }
    if src.len() < total_len {
        // Not enough data yet — reserve space and signal incomplete.
        src.reserve(total_len - src.len());
        return Ok(None);
    }

    // We have a full packet — advance past the fixed header.
    src.advance(header_len);
    let mut payload = src.split_to(remaining_len);

    let ver = version.unwrap_or(ProtocolVersion::V311); // default until CONNECT parsed

    let packet = match packet_type {
        TYPE_CONNECT    => decode_connect(&mut payload)?,
        TYPE_CONNACK    => ControlPacket::ConnAck(decode_connack(&mut payload, ver)?),
        TYPE_PUBLISH    => ControlPacket::Publish(decode_publish(&mut payload, flags, ver)?),
        TYPE_PUBACK     => ControlPacket::PubAck(decode_puback(&mut payload, ver)?),
        TYPE_PUBREC     => ControlPacket::PubRec(decode_pubrec(&mut payload, ver)?),
        TYPE_PUBREL     => ControlPacket::PubRel(decode_pubrel(&mut payload, ver)?),
        TYPE_PUBCOMP    => ControlPacket::PubComp(decode_pubcomp(&mut payload, ver)?),
        TYPE_SUBSCRIBE  => ControlPacket::Subscribe(decode_subscribe(&mut payload, ver)?),
        TYPE_SUBACK     => ControlPacket::SubAck(decode_suback(&mut payload, ver)?),
        TYPE_UNSUBSCRIBE => ControlPacket::Unsubscribe(decode_unsubscribe(&mut payload, ver)?),
        TYPE_UNSUBACK   => ControlPacket::UnsubAck(decode_unsuback(&mut payload, ver)?),
        TYPE_PINGREQ    => ControlPacket::PingReq(PingReq),
        TYPE_PINGRESP   => ControlPacket::PingResp(PingResp),
        TYPE_DISCONNECT => ControlPacket::Disconnect(decode_disconnect(&mut payload, ver)?),
        TYPE_AUTH       => ControlPacket::Auth(decode_auth(&mut payload)?),
        t               => return Err(Error::UnknownPacketType(t)),
    };

    Ok(Some(packet))
}

/// Encode a `ControlPacket` into `dst`.
pub fn encode(packet: &ControlPacket, version: ProtocolVersion, dst: &mut BytesMut) {
    match packet {
        ControlPacket::Connect(p)     => encode_connect(p, version, dst),
        ControlPacket::ConnAck(p)     => encode_connack(p, version, dst),
        ControlPacket::Publish(p)     => encode_publish(p, version, dst),
        ControlPacket::PubAck(p)      => encode_pub_ack(TYPE_PUBACK,  0b0000, p.packet_id, p.reason_code.as_u8(), &p.properties, version, dst),
        ControlPacket::PubRec(p)      => encode_pub_ack(TYPE_PUBREC,  0b0000, p.packet_id, p.reason_code.as_u8(), &p.properties, version, dst),
        ControlPacket::PubRel(p)      => encode_pub_ack(TYPE_PUBREL,  0b0010, p.packet_id, p.reason_code.as_u8(), &p.properties, version, dst),
        ControlPacket::PubComp(p)     => encode_pub_ack(TYPE_PUBCOMP, 0b0000, p.packet_id, p.reason_code.as_u8(), &p.properties, version, dst),
        ControlPacket::Subscribe(p)   => encode_subscribe(p, version, dst),
        ControlPacket::SubAck(p)      => encode_suback(p, version, dst),
        ControlPacket::Unsubscribe(p) => encode_unsubscribe(p, version, dst),
        ControlPacket::UnsubAck(p)    => encode_unsuback(p, version, dst),
        ControlPacket::PingReq(_)     => encode_fixed_only(TYPE_PINGREQ,  0, dst),
        ControlPacket::PingResp(_)    => encode_fixed_only(TYPE_PINGRESP, 0, dst),
        ControlPacket::Disconnect(p)  => encode_disconnect(p, version, dst),
        ControlPacket::Auth(p)        => encode_auth(p, dst),
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// CONNECT decode / encode
// ═══════════════════════════════════════════════════════════════════════════════

fn decode_connect(src: &mut impl Buf) -> Result<ControlPacket, Error> {
    // Protocol name (must be "MQTT")
    let protocol_name = read_utf8_string(src)?;
    if protocol_name != "MQTT" {
        return Err(Error::InvalidProtocolName(protocol_name));
    }

    let version = ProtocolVersion::from_u8(src.get_u8())?;
    let connect_flags = src.get_u8();
    let keep_alive = src.get_u16();

    let clean_start    = (connect_flags & 0b0000_0010) != 0;
    let will_flag      = (connect_flags & 0b0000_0100) != 0;
    let will_qos       = QoS::from_u8((connect_flags >> 3) & 0b11)?;
    let will_retain    = (connect_flags & 0b0010_0000) != 0;
    let password_flag  = (connect_flags & 0b0100_0000) != 0;
    let username_flag  = (connect_flags & 0b1000_0000) != 0;

    // MQTT 5.0: Connect Properties
    let properties = if version.is_v5() {
        Some(Properties::decode(src)?)
    } else {
        None
    };

    // Payload
    let client_id = read_utf8_string(src)?;

    let will = if will_flag {
        let will_props = if version.is_v5() {
            Some(Properties::decode(src)?)
        } else {
            None
        };
        let will_topic   = read_utf8_string(src)?;
        let will_payload = read_binary_data(src)?;
        Some(Will {
            topic:      will_topic,
            payload:    will_payload,
            qos:        will_qos,
            retain:     will_retain,
            properties: will_props,
        })
    } else {
        None
    };

    let username = if username_flag {
        Some(read_utf8_string(src)?)
    } else {
        None
    };
    let password = if password_flag {
        Some(read_binary_data(src)?)
    } else {
        None
    };

    Ok(ControlPacket::Connect(Connect {
        client_id,
        clean_start,
        keep_alive,
        username,
        password,
        will,
        properties,
    }))
}

fn encode_connect(p: &Connect, version: ProtocolVersion, dst: &mut BytesMut) {
    let mut payload = BytesMut::new();

    // Variable header
    let mut var_header = BytesMut::new();
    write_utf8_string(&mut var_header, "MQTT");
    var_header.put_u8(version.as_u8());

    // Connect flags
    let mut flags: u8 = 0;
    if p.clean_start { flags |= 0b0000_0010; }
    if let Some(ref w) = p.will {
        flags |= 0b0000_0100;
        flags |= (w.qos.as_u8() & 0b11) << 3;
        if w.retain { flags |= 0b0010_0000; }
    }
    if p.password.is_some() { flags |= 0b0100_0000; }
    if p.username.is_some() { flags |= 0b1000_0000; }

    var_header.put_u8(flags);
    var_header.put_u16(p.keep_alive);

    // MQTT 5.0 Connect Properties
    if version.is_v5() {
        let props = p.properties.as_ref().cloned().unwrap_or_default();
        props.encode(&mut var_header);
    }

    // Payload
    write_utf8_string(&mut payload, &p.client_id);
    if let Some(ref w) = p.will {
        if version.is_v5() {
            let will_props = w.properties.as_ref().cloned().unwrap_or_default();
            will_props.encode(&mut payload);
        }
        write_utf8_string(&mut payload, &w.topic);
        write_binary_data(&mut payload, &w.payload);
    }
    if let Some(ref u) = p.username {
        write_utf8_string(&mut payload, u);
    }
    if let Some(ref pw) = p.password {
        write_binary_data(&mut payload, pw);
    }

    let remaining_len = var_header.len() + payload.len();
    dst.put_u8((TYPE_CONNECT << 4) | 0);
    write_variable_length(dst, remaining_len);
    dst.put(var_header);
    dst.put(payload);
}

// ═══════════════════════════════════════════════════════════════════════════════
// CONNACK
// ═══════════════════════════════════════════════════════════════════════════════

fn decode_connack(src: &mut impl Buf, version: ProtocolVersion) -> Result<ConnAck, Error> {
    let ack_flags = src.get_u8();
    let session_present = (ack_flags & 0x01) != 0;
    let reason_code = ConnectReason::from_u8(src.get_u8());
    let properties = if version.is_v5() && src.has_remaining() {
        Some(Properties::decode(src)?)
    } else {
        None
    };
    Ok(ConnAck { session_present, reason_code, properties })
}

fn encode_connack(p: &ConnAck, version: ProtocolVersion, dst: &mut BytesMut) {
    let mut body = BytesMut::new();
    body.put_u8(if p.session_present { 1 } else { 0 });
    body.put_u8(p.reason_code.as_u8());
    if version.is_v5() {
        let props = p.properties.as_ref().cloned().unwrap_or_default();
        props.encode(&mut body);
    }
    dst.put_u8((TYPE_CONNACK << 4) | 0);
    write_variable_length(dst, body.len());
    dst.put(body);
}

// ═══════════════════════════════════════════════════════════════════════════════
// PUBLISH
// ═══════════════════════════════════════════════════════════════════════════════

fn decode_publish(src: &mut impl Buf, flags: u8, version: ProtocolVersion) -> Result<Publish, Error> {
    let dup    = (flags & 0b1000) != 0;
    let qos    = QoS::from_u8((flags >> 1) & 0b11)?;
    let retain = (flags & 0b0001) != 0;

    let topic = read_utf8_string(src)?;

    let packet_id = if qos != QoS::AtMostOnce {
        let id = src.get_u16();
        if id == 0 { return Err(Error::ZeroPacketId); }
        Some(id)
    } else {
        None
    };

    let properties = if version.is_v5() {
        Some(Properties::decode(src)?)
    } else {
        None
    };

    let payload = src.copy_to_bytes(src.remaining());

    Ok(Publish { dup, qos, retain, topic, packet_id, payload, properties })
}

fn encode_publish(p: &Publish, version: ProtocolVersion, dst: &mut BytesMut) {
    let mut body = BytesMut::new();
    write_utf8_string(&mut body, &p.topic);
    if p.qos != QoS::AtMostOnce {
        body.put_u16(p.packet_id.unwrap_or(0));
    }
    if version.is_v5() {
        let props = p.properties.as_ref().cloned().unwrap_or_default();
        props.encode(&mut body);
    }
    body.put_slice(&p.payload);

    let mut flags: u8 = 0;
    if p.dup    { flags |= 0b1000; }
    flags |= (p.qos.as_u8() & 0b11) << 1;
    if p.retain { flags |= 0b0001; }

    dst.put_u8((TYPE_PUBLISH << 4) | flags);
    write_variable_length(dst, body.len());
    dst.put(body);
}

// ═══════════════════════════════════════════════════════════════════════════════
// PUBACK / PUBREC / PUBREL / PUBCOMP (shared structure)
// ═══════════════════════════════════════════════════════════════════════════════

macro_rules! decode_pub_ack {
    ($fn_name:ident, $struct:ident) => {
        fn $fn_name(src: &mut impl Buf, version: ProtocolVersion) -> Result<$struct, Error> {
            let packet_id = src.get_u16();
            if packet_id == 0 { return Err(Error::ZeroPacketId); }
            let (reason_code, properties) = if version.is_v5() && src.has_remaining() {
                let rc = PubReason::from_u8(src.get_u8());
                let props = if src.has_remaining() {
                    Some(Properties::decode(src)?)
                } else {
                    None
                };
                (rc, props)
            } else {
                (PubReason::Success, None)
            };
            Ok($struct { packet_id, reason_code, properties })
        }
    };
}

decode_pub_ack!(decode_puback,  PubAck);
decode_pub_ack!(decode_pubrec,  PubRec);
decode_pub_ack!(decode_pubrel,  PubRel);
decode_pub_ack!(decode_pubcomp, PubComp);

fn encode_pub_ack(
    ptype: u8,
    flags: u8,
    packet_id: u16,
    reason_code: u8,
    properties: &Option<Properties>,
    version: ProtocolVersion,
    dst: &mut BytesMut,
) {
    let mut body = BytesMut::new();
    body.put_u16(packet_id);
    if version.is_v5() {
        body.put_u8(reason_code);
        let props = properties.as_ref().cloned().unwrap_or_default();
        props.encode(&mut body);
    }
    dst.put_u8((ptype << 4) | flags);
    write_variable_length(dst, body.len());
    dst.put(body);
}

// ═══════════════════════════════════════════════════════════════════════════════
// SUBSCRIBE / SUBACK
// ═══════════════════════════════════════════════════════════════════════════════

fn decode_subscribe(src: &mut impl Buf, version: ProtocolVersion) -> Result<Subscribe, Error> {
    let packet_id = src.get_u16();
    if packet_id == 0 { return Err(Error::ZeroPacketId); }

    let properties = if version.is_v5() {
        let p = Properties::decode(src)?;
        if p.is_empty() { None } else { Some(p) }
    } else {
        None
    };

    let mut filters = Vec::new();
    while src.has_remaining() {
        let topic_filter = read_utf8_string(src)?;
        let options_byte = src.get_u8();
        let qos = QoS::from_u8(options_byte & 0b11)?;
        let (no_local, retain_as_published, retain_handling) = if version.is_v5() {
            (
                (options_byte & 0b0000_0100) != 0,
                (options_byte & 0b0000_1000) != 0,
                RetainHandling::from_u8((options_byte >> 4) & 0b11),
            )
        } else {
            (false, false, RetainHandling::SendOnSubscribe)
        };
        filters.push(SubscriptionFilter {
            topic_filter,
            qos,
            no_local,
            retain_as_published,
            retain_handling,
        });
    }

    Ok(Subscribe { packet_id, filters, properties })
}

fn encode_subscribe(p: &Subscribe, version: ProtocolVersion, dst: &mut BytesMut) {
    let mut body = BytesMut::new();
    body.put_u16(p.packet_id);
    if version.is_v5() {
        let props = p.properties.as_ref().cloned().unwrap_or_default();
        props.encode(&mut body);
    }
    for f in &p.filters {
        write_utf8_string(&mut body, &f.topic_filter);
        let mut opt: u8 = f.qos.as_u8();
        if version.is_v5() {
            if f.no_local             { opt |= 0b0000_0100; }
            if f.retain_as_published  { opt |= 0b0000_1000; }
            opt |= (f.retain_handling as u8) << 4;
        }
        body.put_u8(opt);
    }
    dst.put_u8((TYPE_SUBSCRIBE << 4) | 0b0010);
    write_variable_length(dst, body.len());
    dst.put(body);
}

fn decode_suback(src: &mut impl Buf, version: ProtocolVersion) -> Result<SubAck, Error> {
    let packet_id = src.get_u16();
    let properties = if version.is_v5() {
        Some(Properties::decode(src)?)
    } else {
        None
    };
    let mut reason_codes = Vec::new();
    while src.has_remaining() {
        reason_codes.push(SubAckReason::from_u8(src.get_u8()));
    }
    Ok(SubAck { packet_id, reason_codes, properties })
}

fn encode_suback(p: &SubAck, version: ProtocolVersion, dst: &mut BytesMut) {
    let mut body = BytesMut::new();
    body.put_u16(p.packet_id);
    if version.is_v5() {
        let props = p.properties.as_ref().cloned().unwrap_or_default();
        props.encode(&mut body);
    }
    for rc in &p.reason_codes {
        body.put_u8(rc.as_u8());
    }
    dst.put_u8((TYPE_SUBACK << 4) | 0);
    write_variable_length(dst, body.len());
    dst.put(body);
}

// ═══════════════════════════════════════════════════════════════════════════════
// UNSUBSCRIBE / UNSUBACK
// ═══════════════════════════════════════════════════════════════════════════════

fn decode_unsubscribe(src: &mut impl Buf, version: ProtocolVersion) -> Result<Unsubscribe, Error> {
    let packet_id = src.get_u16();
    if packet_id == 0 { return Err(Error::ZeroPacketId); }
    let properties = if version.is_v5() {
        Some(Properties::decode(src)?)
    } else {
        None
    };
    let mut filters = Vec::new();
    while src.has_remaining() {
        filters.push(read_utf8_string(src)?);
    }
    Ok(Unsubscribe { packet_id, filters, properties })
}

fn encode_unsubscribe(p: &Unsubscribe, version: ProtocolVersion, dst: &mut BytesMut) {
    let mut body = BytesMut::new();
    body.put_u16(p.packet_id);
    if version.is_v5() {
        let props = p.properties.as_ref().cloned().unwrap_or_default();
        props.encode(&mut body);
    }
    for f in &p.filters {
        write_utf8_string(&mut body, f);
    }
    dst.put_u8((TYPE_UNSUBSCRIBE << 4) | 0b0010);
    write_variable_length(dst, body.len());
    dst.put(body);
}

fn decode_unsuback(src: &mut impl Buf, version: ProtocolVersion) -> Result<UnsubAck, Error> {
    let packet_id = src.get_u16();
    let properties = if version.is_v5() && src.has_remaining() {
        Some(Properties::decode(src)?)
    } else {
        None
    };
    let mut reason_codes = Vec::new();
    while src.has_remaining() {
        reason_codes.push(UnsubAckReason::from_u8(src.get_u8()));
    }
    Ok(UnsubAck { packet_id, reason_codes, properties })
}

fn encode_unsuback(p: &UnsubAck, version: ProtocolVersion, dst: &mut BytesMut) {
    let mut body = BytesMut::new();
    body.put_u16(p.packet_id);
    if version.is_v5() {
        let props = p.properties.as_ref().cloned().unwrap_or_default();
        props.encode(&mut body);
    }
    for rc in &p.reason_codes {
        body.put_u8(rc.as_u8());
    }
    dst.put_u8((TYPE_UNSUBACK << 4) | 0);
    write_variable_length(dst, body.len());
    dst.put(body);
}

// ═══════════════════════════════════════════════════════════════════════════════
// DISCONNECT
// ═══════════════════════════════════════════════════════════════════════════════

fn decode_disconnect(src: &mut impl Buf, version: ProtocolVersion) -> Result<Disconnect, Error> {
    if version.is_v5() && src.has_remaining() {
        let reason_code = DisconnectReason::from_u8(src.get_u8());
        let properties = if src.has_remaining() {
            Some(Properties::decode(src)?)
        } else {
            None
        };
        Ok(Disconnect { reason_code, properties })
    } else {
        Ok(Disconnect::normal())
    }
}

fn encode_disconnect(p: &Disconnect, version: ProtocolVersion, dst: &mut BytesMut) {
    if version.is_v5() {
        let mut body = BytesMut::new();
        body.put_u8(p.reason_code.as_u8());
        let props = p.properties.as_ref().cloned().unwrap_or_default();
        props.encode(&mut body);
        dst.put_u8((TYPE_DISCONNECT << 4) | 0);
        write_variable_length(dst, body.len());
        dst.put(body);
    } else {
        encode_fixed_only(TYPE_DISCONNECT, 0, dst);
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// AUTH (5.0 only)
// ═══════════════════════════════════════════════════════════════════════════════

fn decode_auth(src: &mut impl Buf) -> Result<Auth, Error> {
    let reason_code = if src.has_remaining() {
        AuthReason::from_u8(src.get_u8())
    } else {
        AuthReason::Success
    };
    let properties = if src.has_remaining() {
        Some(Properties::decode(src)?)
    } else {
        None
    };
    Ok(Auth { reason_code, properties })
}

fn encode_auth(p: &Auth, dst: &mut BytesMut) {
    let mut body = BytesMut::new();
    body.put_u8(p.reason_code.as_u8());
    let props = p.properties.as_ref().cloned().unwrap_or_default();
    props.encode(&mut body);
    dst.put_u8((TYPE_AUTH << 4) | 0);
    write_variable_length(dst, body.len());
    dst.put(body);
}

// ═══════════════════════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════════════════════

/// Encode a packet that has only a fixed header (PINGREQ, PINGRESP).
fn encode_fixed_only(ptype: u8, flags: u8, dst: &mut BytesMut) {
    dst.put_u8((ptype << 4) | flags);
    dst.put_u8(0); // remaining length = 0
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use crate::packet::connect::ConnectReason;

    fn roundtrip(packet: ControlPacket, version: ProtocolVersion) -> ControlPacket {
        let mut buf = BytesMut::new();
        encode(&packet, version, &mut buf);
        decode(&mut buf, Some(version))
            .expect("decode error")
            .expect("incomplete")
    }

    // ── CONNECT ───────────────────────────────────────────────────────────────
    #[test]
    fn roundtrip_connect_v311() {
        let pkt = ControlPacket::Connect(Connect {
            client_id: "test-client".into(),
            clean_start: true,
            keep_alive: 60,
            username: Some("user".into()),
            password: Some(Bytes::from_static(b"pass")),
            will: None,
            properties: None,
        });
        assert_eq!(roundtrip(pkt.clone(), ProtocolVersion::V311), pkt);
    }

    #[test]
    fn roundtrip_connect_v5_with_will() {
        let mut props = Properties::new();
        props.session_expiry_interval = Some(3600);
        props.receive_maximum = Some(10);

        let mut will_props = Properties::new();
        will_props.will_delay_interval = Some(30);

        let pkt = ControlPacket::Connect(Connect {
            client_id: "v5-client".into(),
            clean_start: false,
            keep_alive: 120,
            username: None,
            password: None,
            will: Some(Will {
                topic: "devices/v5-client/status".into(),
                payload: Bytes::from_static(b"offline"),
                qos: QoS::AtLeastOnce,
                retain: true,
                properties: Some(will_props),
            }),
            properties: Some(props),
        });
        assert_eq!(roundtrip(pkt.clone(), ProtocolVersion::V500), pkt);
    }

    // ── CONNACK ───────────────────────────────────────────────────────────────
    #[test]
    fn roundtrip_connack_v311() {
        let pkt = ControlPacket::ConnAck(ConnAck {
            session_present: false,
            reason_code: ConnectReason::Success,
            properties: None,
        });
        assert_eq!(roundtrip(pkt.clone(), ProtocolVersion::V311), pkt);
    }

    #[test]
    fn roundtrip_connack_v5() {
        let mut props = Properties::new();
        props.assigned_client_identifier = Some("server-assigned-id".into());
        props.server_keep_alive = Some(30);
        props.shared_subscription_available = Some(true);

        let pkt = ControlPacket::ConnAck(ConnAck {
            session_present: true,
            reason_code: ConnectReason::Success,
            properties: Some(props),
        });
        assert_eq!(roundtrip(pkt.clone(), ProtocolVersion::V500), pkt);
    }

    // ── PUBLISH ───────────────────────────────────────────────────────────────
    #[test]
    fn roundtrip_publish_qos0() {
        let pkt = ControlPacket::Publish(Publish::new("a/b/c", Bytes::from_static(b"hello")));
        assert_eq!(roundtrip(pkt.clone(), ProtocolVersion::V311), pkt);
    }

    #[test]
    fn roundtrip_publish_qos1_v5() {
        let mut props = Properties::new();
        props.message_expiry_interval = Some(60);
        props.response_topic = Some("replies/here".into());
        props.user_properties.push(("unit".into(), "celsius".into()));

        let pkt = ControlPacket::Publish(Publish {
            dup: false,
            qos: QoS::AtLeastOnce,
            retain: false,
            topic: "sensors/temp".into(),
            packet_id: Some(42),
            payload: Bytes::from_static(b"22.5"),
            properties: Some(props),
        });
        assert_eq!(roundtrip(pkt.clone(), ProtocolVersion::V500), pkt);
    }

    // ── PUBACK ────────────────────────────────────────────────────────────────
    #[test]
    fn roundtrip_puback_v311() {
        let pkt = ControlPacket::PubAck(PubAck::success(7));
        assert_eq!(roundtrip(pkt.clone(), ProtocolVersion::V311), pkt);
    }

    // ── SUBSCRIBE ─────────────────────────────────────────────────────────────
    #[test]
    fn roundtrip_subscribe_v5_options() {
        let pkt = ControlPacket::Subscribe(Subscribe {
            packet_id: 3,
            filters: vec![
                SubscriptionFilter {
                    topic_filter: "sensors/#".into(),
                    qos: QoS::AtLeastOnce,
                    no_local: true,
                    retain_as_published: true,
                    retain_handling: RetainHandling::SendOnNewSubscription,
                },
                SubscriptionFilter::new("alerts/+", QoS::ExactlyOnce),
            ],
            properties: None,
        });
        assert_eq!(roundtrip(pkt.clone(), ProtocolVersion::V500), pkt);
    }

    // ── PING ─────────────────────────────────────────────────────────────────
    #[test]
    fn roundtrip_pingreq() {
        let pkt = ControlPacket::PingReq(PingReq);
        assert_eq!(roundtrip(pkt.clone(), ProtocolVersion::V311), pkt);
    }

    #[test]
    fn roundtrip_pingresp() {
        let pkt = ControlPacket::PingResp(PingResp);
        assert_eq!(roundtrip(pkt.clone(), ProtocolVersion::V500), pkt);
    }

    // ── DISCONNECT ────────────────────────────────────────────────────────────
    #[test]
    fn roundtrip_disconnect_v311() {
        let pkt = ControlPacket::Disconnect(Disconnect::normal());
        assert_eq!(roundtrip(pkt.clone(), ProtocolVersion::V311), pkt);
    }

    #[test]
    fn roundtrip_disconnect_v5_reason() {
        let mut props = Properties::new();
        props.reason_string = Some("server shutting down".into());

        let pkt = ControlPacket::Disconnect(Disconnect {
            reason_code: DisconnectReason::ServerShuttingDown,
            properties: Some(props),
        });
        assert_eq!(roundtrip(pkt.clone(), ProtocolVersion::V500), pkt);
    }

    // ── AUTH ─────────────────────────────────────────────────────────────────
    #[test]
    fn roundtrip_auth() {
        let mut props = Properties::new();
        props.authentication_method = Some("SCRAM-SHA-256".into());
        props.authentication_data = Some(Bytes::from_static(b"challenge-bytes"));

        let pkt = ControlPacket::Auth(Auth {
            reason_code: AuthReason::ContinueAuthentication,
            properties: Some(props),
        });
        assert_eq!(roundtrip(pkt.clone(), ProtocolVersion::V500), pkt);
    }

    // ── Partial data ─────────────────────────────────────────────────────────
    #[test]
    fn incomplete_returns_none() {
        let mut buf = BytesMut::from(&[0x30u8, 0x05][..]); // PUBLISH, remaining=5, but no data
        let result = decode(&mut buf, Some(ProtocolVersion::V311)).unwrap();
        assert!(result.is_none());
    }
}
