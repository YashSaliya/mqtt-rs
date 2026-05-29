use std::time::Instant;
use tokio::task::JoinSet;
use mqtt_core::QoS;
use mqtt_client::ClientBuilder;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("==================================================");
    println!("🚀 Starting RustMQ High-Performance Stress Test 🚀");
    println!("==================================================");

    let host = "127.0.0.1";
    let port = 1883;
    let num_clients = 50;
    let messages_per_client = 200;
    let qos = QoS::AtLeastOnce;

    println!("Configuration:");
    println!("  - Target Broker:  {}:{}", host, port);
    println!("  - Total Clients:   {}", num_clients);
    println!("  - Msgs per Client: {}", messages_per_client);
    println!("  - Target QoS:     {:?}", qos);
    println!("--------------------------------------------------");

    let start_time = Instant::now();
    let mut join_set = JoinSet::new();

    for client_idx in 0..num_clients {
        let client_id = format!("stress-client-{}", client_idx);
        let host_str = host.to_string();
        
        join_set.spawn(async move {
            let client = match ClientBuilder::new(host_str, port)
                .client_id(client_id.clone())
                .clean_start(true)
                .connect()
                .await
            {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("Client {} failed to connect: {:?}", client_id, e);
                    return 0;
                }
            };

            let mut successful_publishes = 0;
            for msg_idx in 0..messages_per_client {
                let topic = format!("stress/topic/{}", client_idx);
                let payload = format!("Payload #{} from {}", msg_idx, client_id);
                
                if client.publish(topic, bytes::Bytes::from(payload), qos, false).await.is_ok() {
                    successful_publishes += 1;
                }
            }

            let _ = client.disconnect().await;
            successful_publishes
        });
    }

    let mut total_messages = 0;
    while let Some(res) = join_set.join_next().await {
        if let Ok(msgs) = res {
            total_messages += msgs;
        }
    }

    let elapsed = start_time.elapsed();
    let msg_rate = total_messages as f64 / elapsed.as_secs_f64();

    println!("--------------------------------------------------");
    println!("⚡ Stress Test Completed ⚡");
    println!("--------------------------------------------------");
    println!("  - Total Time Elapsed:  {:.2?}", elapsed);
    println!("  - Successful Publishes: {} / {}", total_messages, num_clients * messages_per_client);
    println!("  - Message Throughput:   {:.2} msgs/sec", msg_rate);
    println!("==================================================");

    Ok(())
}
