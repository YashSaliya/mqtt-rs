#![allow(dead_code)]
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tracing::{info, error, Level};
use tracing_subscriber::FmtSubscriber;

mod config;
mod auth;
mod topic_alias;
mod topic_trie;
mod retained;
mod session;
mod broker;
mod connection;

use config::Config;
use broker::Broker;
use connection::Connection;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing subscriber for premium, state-of-the-art logging
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::DEBUG)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    info!("Starting RustMQ Broker...");

    let config = Config::default();
    let addr = format!("{}:{}", config.host, config.port);

    // Initialize the central Broker actor channel
    let (broker_tx, broker_rx) = mpsc::channel(1000);
    
    // Spawn Broker actor in the background
    let broker = Broker::new(config);
    tokio::spawn(broker.run(broker_rx));

    // Bind the TCP Listener
    let listener = TcpListener::bind(&addr).await?;
    info!("RustMQ listening on {}", addr);

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let broker_tx_clone = broker_tx.clone();
                tokio::spawn(async move {
                    let connection = Connection::new(stream, broker_tx_clone);
                    connection.handle().await;
                });
            }
            Err(e) => {
                error!("Failed to accept incoming connection: {:?}", e);
            }
        }
    }
}
