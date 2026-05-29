# mqtt-core 🦀

The zero-I/O binary protocol engine and packet codec for **MQTT 3.1.1 & MQTT 5.0**. Part of the **RustMQ** ecosystem.

This is a **Sans-I/O** codec: it operates purely on `bytes::BytesMut` buffers. It contains no sockets, no async runtimes, and no threading, making it extremely lightweight, portable, and memory-safe.

---

## 🚀 Features
- **Pure Sans-I/O Codec**: Operate directly on memory buffers (`BytesMut`) for maximum flexibility.
- **Dual-Version Framing**: Supports all 15 control packet types defined in MQTT `3.1.1` and `5.0`.
- **MQTT 5.0 Properties TLV**: Comprehensive serialization and deserialization of all 26 property identifiers.
- **Topic Engine**: Full single-level (`+`) and multi-level (`#`) wildcard topic validation and matching utilities.
- **No Unsafe Code**: 100% safe, fast Rust.

---

## 📦 Installation

Add this to your `Cargo.toml` dependencies:
```toml
[dependencies]
mqtt-core = "0.1.1"
```

---

## 💻 Quick Start

### Decoding Packets
```rust
use bytes::BytesMut;
use mqtt_core::{codec::decode, version::ProtocolVersion};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut buf = BytesMut::from(&[
        0x10, 0x0C, // CONNECT Header
        0x00, 0x04, b'M', b'Q', b'T', b'T', // Protocol Name
        0x04, // Protocol Level (3.1.1)
        0x02, // Connect Flags (Clean Session)
        0x00, 0x3C, // Keep Alive (60s)
        0x00, 0x00, // Client ID length (0)
    ][..]);

    // Decode the CONNECT packet
    if let Some(packet) = decode(&mut buf, None)? {
        println!("Successfully decoded: {:?}", packet);
    }
    Ok(())
}
```

---

## 📄 License
Licensed under the MIT License.
