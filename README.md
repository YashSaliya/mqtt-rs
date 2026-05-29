# RustMQ 🦀
A blazing-fast, dual-version **MQTT 3.1.1 & MQTT 5.0 Broker and Client** implemented entirely from scratch in Rust with zero third-party MQTT parsing dependencies.

---

## 📦 Project Structure

RustMQ is structured as a robust Cargo workspace:
- **[`mqtt-core`](./mqtt-core)**: The zero-I/O protocol engine. Contains encoders, decoders, topic validators, wildcards, and MQTT 5.0 properties logic.
- **[`mqtt-broker`](./mqtt-broker)**: An asynchronous actor-based broker supporting concurrent client connections, QoS 0/1/2 levels, persistent sessions, will messages, and shared subscriptions.
- **[`mqtt-client`](./mqtt-client)**: An async client library for connecting, subscribing, and publishing messages asynchronously.
- **[`mqtt-cli`](./mqtt-cli)**: A command-line companion tool to interact with the broker (publish/subscribe) via custom subcommands.

---

## 🚀 Features

- **Dual-Version Compatibility**: Automatically detects protocol versions (`3.1.1` or `5.0`) on handshake and behaves accordingly.
- **Full QoS Implementation**: QoS `0` (At Most Once), QoS `1` (At Least Once), and QoS `2` (Exactly Once) supported on both publisher and subscriber connections.
- **Shared Subscriptions**: MQTT 5.0 style shared subscriptions (`$share/group_name/topic`) supporting round-robin load balancing.
- **Clean & Persistent Sessions**: Supports session state storage, offline queues for persistent connections, and session lifetime intervals.
- **Telemetry & structured logs**: Integrates the `tracing` framework for high-fidelity server diagnostics.

---

## 🛠️ Getting Started

### Prerequisites
Ensure you have the Rust toolchain installed:
```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

---

## 🏃 Running the Application

### 1. Run the Broker
To start the TCP broker on `127.0.0.1:1883`:
```bash
cargo run -p mqtt-broker
```

### 2. Subscribe to a Topic
Using the CLI tool, subscribe to a topic:
```bash
cargo run -p mqtt-cli -- sub --topic "sensors/temp" --qos 1 --mqtt-version 5
```

### 3. Publish a Message
Open another terminal window and publish a message:
```bash
cargo run -p mqtt-cli -- pub --topic "sensors/temp" --message "24.5" --qos 1 --mqtt-version 5
```

---

## ⚡ Stress Testing
You can stress-test the broker by spawning 50 concurrent clients sending 10,000 QoS 1 messages:
```bash
cargo run -p mqtt-client --example stress_test
```

---

## 📚 Generating Documentation
To build and view the comprehensive API documentation for the entire project (including functions, types, and structs) in your browser:
```bash
cargo doc --no-deps --open
```

---

## 📄 License
This project is licensed under the MIT License.
