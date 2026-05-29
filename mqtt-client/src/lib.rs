use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::time::{timeout, Duration};
use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{debug, error, info, warn};

use mqtt_core::{
    codec::{decode, encode},
    packet::{
        connect::{Connect, ConnectReason},
        disconnect::Disconnect,
        ping::PingReq,
        puback::{PubAck, PubComp, PubRec, PubRel, PubReason},
        publish::Publish,
        subscribe::{Subscribe, SubscriptionFilter, RetainHandling},
        unsubscribe::Unsubscribe,
        ControlPacket,
    },
    version::ProtocolVersion,
    QoS,
};

#[derive(Debug, Clone)]
pub struct PublishMessage {
    pub topic: String,
    pub payload: Bytes,
    pub qos: QoS,
    pub retain: bool,
}

pub struct MqttClient {
    command_tx: mpsc::Sender<ClientCommand>,
    incoming_rx: Arc<Mutex<Option<mpsc::Receiver<PublishMessage>>>>,
}

#[derive(Debug)]
enum ClientCommand {
    Publish {
        topic: String,
        payload: Bytes,
        qos: QoS,
        retain: bool,
        respond: oneshot::Sender<Result<(), anyhow::Error>>,
    },
    Subscribe {
        filter: String,
        qos: QoS,
        respond: oneshot::Sender<Result<(), anyhow::Error>>,
    },
    Unsubscribe {
        filter: String,
        respond: oneshot::Sender<Result<(), anyhow::Error>>,
    },
    Disconnect {
        respond: oneshot::Sender<Result<(), anyhow::Error>>,
    },
}

pub struct ClientBuilder {
    host: String,
    port: u16,
    client_id: String,
    clean_start: bool,
    keep_alive: u16,
    username: Option<String>,
    password: Option<Bytes>,
    version: ProtocolVersion,
}

impl ClientBuilder {
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
            client_id: String::new(),
            clean_start: true,
            keep_alive: 60,
            username: None,
            password: None,
            version: ProtocolVersion::V500,
        }
    }

    pub fn client_id(mut self, id: impl Into<String>) -> Self {
        self.client_id = id.into();
        self
    }

    pub fn clean_start(mut self, clean: bool) -> Self {
        self.clean_start = clean;
        self
    }

    pub fn keep_alive(mut self, interval: u16) -> Self {
        self.keep_alive = interval;
        self
    }

    pub fn credentials(mut self, username: impl Into<String>, password: impl Into<Bytes>) -> Self {
        self.username = Some(username.into());
        self.password = Some(password.into());
        self
    }

    pub fn version(mut self, ver: ProtocolVersion) -> Self {
        self.version = ver;
        self
    }

    pub async fn connect(self) -> Result<MqttClient, anyhow::Error> {
        let addr = format!("{}:{}", self.host, self.port);
        debug!("Connecting to {}", addr);
        let stream = TcpStream::connect(&addr).await?;
        
        let (command_tx, command_rx) = mpsc::channel(100);
        let (incoming_tx, incoming_rx) = mpsc::channel(100);

        let mut client_handler = ClientHandler {
            stream,
            version: self.version,
            client_id: self.client_id,
            clean_start: self.clean_start,
            keep_alive: self.keep_alive,
            username: self.username,
            password: self.password,
            read_buf: BytesMut::with_capacity(4096),
            write_buf: BytesMut::with_capacity(4096),
            command_rx,
            incoming_tx,
            next_packet_id: 1,
            pending_sub: None,
            pending_unsub: None,
            pending_puback: None,
            pending_pubrec: None,
            _pending_pubrel: None,
            _pending_pubcomp: None,
        };

        client_handler.handshake().await?;
        
        tokio::spawn(async move {
            if let Err(e) = client_handler.run().await {
                error!("Client loop error: {:?}", e);
            }
        });

        Ok(MqttClient {
            command_tx,
            incoming_rx: Arc::new(Mutex::new(Some(incoming_rx))),
        })
    }
}

impl MqttClient {
    pub async fn publish(&self, topic: impl Into<String>, payload: impl Into<Bytes>, qos: QoS, retain: bool) -> Result<(), anyhow::Error> {
        let (respond, rx) = oneshot::channel();
        self.command_tx
            .send(ClientCommand::Publish {
                topic: topic.into(),
                payload: payload.into(),
                qos,
                retain,
                respond,
            })
            .await?;
        rx.await?
    }

    pub async fn subscribe(&self, filter: impl Into<String>, qos: QoS) -> Result<(), anyhow::Error> {
        let (respond, rx) = oneshot::channel();
        self.command_tx
            .send(ClientCommand::Subscribe {
                filter: filter.into(),
                qos,
                respond,
            })
            .await?;
        rx.await?
    }

    pub async fn unsubscribe(&self, filter: impl Into<String>) -> Result<(), anyhow::Error> {
        let (respond, rx) = oneshot::channel();
        self.command_tx
            .send(ClientCommand::Unsubscribe {
                filter: filter.into(),
                respond,
            })
            .await?;
        rx.await?
    }

    pub async fn disconnect(&self) -> Result<(), anyhow::Error> {
        let (respond, rx) = oneshot::channel();
        self.command_tx.send(ClientCommand::Disconnect { respond }).await?;
        rx.await?
    }

    pub async fn messages(&self) -> Option<mpsc::Receiver<PublishMessage>> {
        let mut guard = self.incoming_rx.lock().await;
        guard.take()
    }
}

struct ClientHandler {
    stream: TcpStream,
    version: ProtocolVersion,
    client_id: String,
    clean_start: bool,
    keep_alive: u16,
    username: Option<String>,
    password: Option<Bytes>,
    
    read_buf: BytesMut,
    write_buf: BytesMut,
    
    command_rx: mpsc::Receiver<ClientCommand>,
    incoming_tx: mpsc::Sender<PublishMessage>,
    
    next_packet_id: u16,
    
    // Acknowledgement callbacks
    pending_sub: Option<(u16, oneshot::Sender<Result<(), anyhow::Error>>)>,
    pending_unsub: Option<(u16, oneshot::Sender<Result<(), anyhow::Error>>)>,
    pending_puback: Option<(u16, oneshot::Sender<Result<(), anyhow::Error>>)>,
    pending_pubrec: Option<(u16, oneshot::Sender<Result<(), anyhow::Error>>)>,
    _pending_pubrel: Option<(u16, oneshot::Sender<Result<(), anyhow::Error>>)>,
    _pending_pubcomp: Option<(u16, oneshot::Sender<Result<(), anyhow::Error>>)>,
}

impl ClientHandler {
    async fn handshake(&mut self) -> Result<(), anyhow::Error> {
        // Send CONNECT
        let connect = Connect {
            client_id: self.client_id.clone(),
            clean_start: self.clean_start,
            keep_alive: self.keep_alive,
            username: self.username.clone(),
            password: self.password.clone(),
            will: None,
            properties: None,
        };

        self.write_packet(&ControlPacket::Connect(connect)).await?;

        // Read CONNACK
        let connack_timeout = Duration::from_secs(5);
        let connack_packet = timeout(connack_timeout, self.read_packet()).await??;
        
        match connack_packet {
            ControlPacket::ConnAck(ack) => {
                if ack.reason_code == ConnectReason::Success {
                    info!("Client successfully connected to broker");
                    Ok(())
                } else {
                    Err(anyhow::anyhow!("Connection rejected by broker: {:?}", ack.reason_code))
                }
            }
            other => Err(anyhow::anyhow!("Expected CONNACK, received {:?}", other)),
        }
    }

    async fn run(mut self) -> Result<(), anyhow::Error> {
        let ping_interval = if self.keep_alive > 0 {
            Duration::from_secs(self.keep_alive as u64)
        } else {
            Duration::from_secs(3600 * 24)
        };

        loop {
            tokio::select! {
                // Command processing
                cmd = self.command_rx.recv() => {
                    match cmd {
                        Some(ClientCommand::Publish { topic, payload, qos, retain, respond }) => {
                            let res = self.handle_publish(topic, payload, qos, retain, respond).await;
                            if let Err(e) = res {
                                warn!("Publish error: {:?}", e);
                            }
                        }
                        Some(ClientCommand::Subscribe { filter, qos, respond }) => {
                            let res = self.handle_subscribe(filter, qos, respond).await;
                            if let Err(e) = res {
                                warn!("Subscribe error: {:?}", e);
                            }
                        }
                        Some(ClientCommand::Unsubscribe { filter, respond }) => {
                            let res = self.handle_unsubscribe(filter, respond).await;
                            if let Err(e) = res {
                                warn!("Unsubscribe error: {:?}", e);
                            }
                        }
                        Some(ClientCommand::Disconnect { respond }) => {
                            let disc = Disconnect::normal();
                            let _ = self.write_packet(&ControlPacket::Disconnect(disc)).await;
                            let _ = respond.send(Ok(()));
                            return Ok(());
                        }
                        None => {
                            return Ok(());
                        }
                    }
                }

                // TCP Read
                read_res = self.stream.read_buf(&mut self.read_buf) => {
                    let bytes_read = read_res?;
                    if bytes_read == 0 {
                        return Err(anyhow::anyhow!("Connection closed by broker"));
                    }

                    while let Some(packet) = decode(&mut self.read_buf, Some(self.version))? {
                        self.process_packet(packet).await?;
                    }
                }

                // Keep Alive Ping
                _ = tokio::time::sleep(ping_interval) => {
                    let ping = PingReq;
                    self.write_packet(&ControlPacket::PingReq(ping)).await?;
                }
            }
        }
    }

    fn get_next_packet_id(&mut self) -> u16 {
        let id = self.next_packet_id;
        if self.next_packet_id == u16::MAX {
            self.next_packet_id = 1;
        } else {
            self.next_packet_id += 1;
        }
        id
    }

    async fn handle_publish(&mut self, topic: String, payload: Bytes, qos: QoS, retain: bool, respond: oneshot::Sender<Result<(), anyhow::Error>>) -> Result<(), anyhow::Error> {
        let packet_id = if qos == QoS::AtMostOnce { None } else { Some(self.get_next_packet_id()) };
        
        let publish = Publish {
            dup: false,
            qos,
            retain,
            topic,
            packet_id,
            payload,
            properties: None,
        };

        if qos == QoS::AtMostOnce {
            self.write_packet(&ControlPacket::Publish(publish)).await?;
            let _ = respond.send(Ok(()));
        } else if qos == QoS::AtLeastOnce {
            let pid = packet_id.unwrap();
            self.pending_puback = Some((pid, respond));
            self.write_packet(&ControlPacket::Publish(publish)).await?;
        } else if qos == QoS::ExactlyOnce {
            let pid = packet_id.unwrap();
            self.pending_pubrec = Some((pid, respond));
            self.write_packet(&ControlPacket::Publish(publish)).await?;
        }
        Ok(())
    }

    async fn handle_subscribe(&mut self, filter: String, qos: QoS, respond: oneshot::Sender<Result<(), anyhow::Error>>) -> Result<(), anyhow::Error> {
        let packet_id = self.get_next_packet_id();
        let sub = Subscribe {
            packet_id,
            filters: vec![SubscriptionFilter {
                topic_filter: filter,
                qos,
                no_local: false,
                retain_as_published: false,
                retain_handling: RetainHandling::SendOnSubscribe,
            }],
            properties: None,
        };

        self.pending_sub = Some((packet_id, respond));
        self.write_packet(&ControlPacket::Subscribe(sub)).await?;
        Ok(())
    }

    async fn handle_unsubscribe(&mut self, filter: String, respond: oneshot::Sender<Result<(), anyhow::Error>>) -> Result<(), anyhow::Error> {
        let packet_id = self.get_next_packet_id();
        let unsub = Unsubscribe {
            packet_id,
            filters: vec![filter],
            properties: None,
        };

        self.pending_unsub = Some((packet_id, respond));
        self.write_packet(&ControlPacket::Unsubscribe(unsub)).await?;
        Ok(())
    }

    async fn process_packet(&mut self, packet: ControlPacket) -> Result<(), anyhow::Error> {
        match packet {
            ControlPacket::Publish(pub_msg) => {
                let packet_id_val = pub_msg.packet_id.unwrap_or(0);
                
                let _ = self.incoming_tx.send(PublishMessage {
                    topic: pub_msg.topic,
                    payload: pub_msg.payload,
                    qos: pub_msg.qos,
                    retain: pub_msg.retain,
                }).await;

                match pub_msg.qos {
                    QoS::AtMostOnce => {}
                    QoS::AtLeastOnce => {
                        let puback = PubAck {
                            packet_id: packet_id_val,
                            reason_code: PubReason::Success,
                            properties: None,
                        };
                        self.write_packet(&ControlPacket::PubAck(puback)).await?;
                    }
                    QoS::ExactlyOnce => {
                        let pubrec = PubRec {
                            packet_id: packet_id_val,
                            reason_code: PubReason::Success,
                            properties: None,
                        };
                        self.write_packet(&ControlPacket::PubRec(pubrec)).await?;
                    }
                }
            }
            ControlPacket::PubAck(ack) => {
                if let Some((pid, respond)) = self.pending_puback.take() {
                    if pid == ack.packet_id {
                        let _ = respond.send(Ok(()));
                    } else {
                        self.pending_puback = Some((pid, respond));
                    }
                }
            }
            ControlPacket::PubRec(rec) => {
                // Send PUBREL
                let pubrel = PubRel {
                    packet_id: rec.packet_id,
                    reason_code: PubReason::Success,
                    properties: None,
                };
                self.write_packet(&ControlPacket::PubRel(pubrel)).await?;
            }
            ControlPacket::PubRel(rel) => {
                // Send PUBCOMP
                let pubcomp = PubComp {
                    packet_id: rel.packet_id,
                    reason_code: PubReason::Success,
                    properties: None,
                };
                self.write_packet(&ControlPacket::PubComp(pubcomp)).await?;
            }
            ControlPacket::PubComp(comp) => {
                if let Some((pid, respond)) = self.pending_pubrec.take() {
                    if pid == comp.packet_id {
                        let _ = respond.send(Ok(()));
                    } else {
                        self.pending_pubrec = Some((pid, respond));
                    }
                }
            }
            ControlPacket::SubAck(ack) => {
                if let Some((pid, respond)) = self.pending_sub.take() {
                    if pid == ack.packet_id {
                        let _ = respond.send(Ok(()));
                    } else {
                        self.pending_sub = Some((pid, respond));
                    }
                }
            }
            ControlPacket::UnsubAck(ack) => {
                if let Some((pid, respond)) = self.pending_unsub.take() {
                    if pid == ack.packet_id {
                        let _ = respond.send(Ok(()));
                    } else {
                        self.pending_unsub = Some((pid, respond));
                    }
                }
            }
            ControlPacket::PingResp(_) => {
                // Received keep alive response
            }
            _ => {}
        }
        Ok(())
    }

    async fn read_packet(&mut self) -> Result<ControlPacket, anyhow::Error> {
        loop {
            if let Some(packet) = decode(&mut self.read_buf, Some(self.version))? {
                return Ok(packet);
            }
            let bytes_read = self.stream.read_buf(&mut self.read_buf).await?;
            if bytes_read == 0 {
                return Err(anyhow::anyhow!("Connection closed by peer"));
            }
        }
    }

    async fn write_packet(&mut self, packet: &ControlPacket) -> Result<(), anyhow::Error> {
        self.write_buf.clear();
        encode(packet, self.version, &mut self.write_buf);
        self.stream.write_all(&self.write_buf).await?;
        self.stream.flush().await?;
        Ok(())
    }
}
