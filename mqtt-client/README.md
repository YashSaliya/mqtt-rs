# mqtt-client-rs 🔌

An asynchronous, highly performant **MQTT 3.1.1 & MQTT 5.0 client library** built entirely in Rust using Tokio. Part of the **RustMQ** ecosystem.

---

## 🚀 Features
- **Dual-Version Support**: Connect to any broker using either MQTT `3.1.1` or `5.0`.
- **Full QoS Support**: Supports QoS `0` (At Most Once), QoS `1` (At Least Once), and QoS `2` (Exactly Once) delivery.
- **Asynchronous Channels**: Native Tokio `mpsc` channel-based message delivery for seamless integration into async architectures.
- **Clean Handshakes**: Automatic keep-alive pingers and connection lifecycle management.

---

## 📦 Installation

Add this to your `Cargo.toml` dependencies:
```toml
[dependencies]
mqtt-client-rs = "0.1.1"
```

---

## 💻 Quick Start

Here is a simple example showing how to connect, subscribe to a topic, and publish a message:

```rust
use mqtt_client_rs::{ClientBuilder, QoS, ProtocolVersion};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Configure and connect the client
    let client = ClientBuilder::new("127.0.0.1", 1883)
        .client_id("example-client")
        .version(ProtocolVersion::V500) // Choose between V311 and V500
        .connect()
        .await?;

    // 2. Subscribe to a topic filter
    client.subscribe("sensors/temp", QoS::AtLeastOnce).await?;
    println!("Subscribed successfully!");

    // 3. Receive incoming messages in the background
    if let Some(mut rx) = client.messages().await {
        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                let payload = String::from_utf8_lossy(&msg.payload);
                println!("[Incoming] Topic: {}, Payload: {}", msg.topic, payload);
            }
        });
    }

    // 4. Publish a message
    client.publish("sensors/temp", "24.5", QoS::AtLeastOnce, false).await?;
    println!("Message published successfully!");

    // Keep active for a moment to receive the message
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // 5. Disconnect gracefully
    client.disconnect().await?;
    Ok(())
}
```

---

## 📄 License
Licensed under the MIT License.
