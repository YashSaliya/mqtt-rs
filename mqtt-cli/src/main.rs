use clap::{Parser, Subcommand};
use bytes::Bytes;
use mqtt_core::{QoS, version::ProtocolVersion};
use mqtt_client::ClientBuilder;

#[derive(Parser)]
#[command(name = "mqttcli")]
#[command(about = "A premium dual-version MQTT command-line interface", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Publish a message to a topic
    Pub {
        #[arg(long, default_value = "127.0.0.1")]
        host: String,

        #[arg(long, short, default_value_t = 1883)]
        port: u16,

        #[arg(long, short)]
        topic: String,

        #[arg(long, short)]
        message: String,

        #[arg(long, short, default_value_t = 0)]
        qos: u8,

        #[arg(long, short)]
        retain: bool,

        #[arg(long, name = "mqtt-version", default_value = "5")]
        mqtt_version: String,
    },
    /// Subscribe to a topic filter and print incoming messages
    Sub {
        #[arg(long, default_value = "127.0.0.1")]
        host: String,

        #[arg(long, short, default_value_t = 1883)]
        port: u16,

        #[arg(long, short)]
        topic: String,

        #[arg(long, short, default_value_t = 0)]
        qos: u8,

        #[arg(long, name = "mqtt-version", default_value = "5")]
        mqtt_version: String,
    },
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Pub {
            host,
            port,
            topic,
            message,
            qos,
            retain,
            mqtt_version,
        } => {
            let version = match mqtt_version.as_str() {
                "3" | "3.1.1" => ProtocolVersion::V311,
                _ => ProtocolVersion::V500,
            };

            let qos_val = match qos {
                1 => QoS::AtLeastOnce,
                2 => QoS::ExactlyOnce,
                _ => QoS::AtMostOnce,
            };

            let client = ClientBuilder::new(host, port)
                .client_id("mqtt-cli-pub")
                .version(version)
                .connect()
                .await?;

            client.publish(topic, Bytes::from(message), qos_val, retain).await?;
            client.disconnect().await?;
            println!("Message published successfully!");
        }
        Commands::Sub {
            host,
            port,
            topic,
            qos,
            mqtt_version,
        } => {
            let version = match mqtt_version.as_str() {
                "3" | "3.1.1" => ProtocolVersion::V311,
                _ => ProtocolVersion::V500,
            };

            let qos_val = match qos {
                1 => QoS::AtLeastOnce,
                2 => QoS::ExactlyOnce,
                _ => QoS::AtMostOnce,
            };

            let client = ClientBuilder::new(host, port)
                .client_id("mqtt-cli-sub")
                .version(version)
                .connect()
                .await?;

            client.subscribe(topic.clone(), qos_val).await?;
            println!("Subscribed to {}! Waiting for messages...", topic);

            if let Some(mut rx) = client.messages().await {
                while let Some(msg) = rx.recv().await {
                    let payload_str = String::from_utf8_lossy(&msg.payload);
                    println!("[{}] {}", msg.topic, payload_str);
                }
            }
        }
    }

    Ok(())
}
