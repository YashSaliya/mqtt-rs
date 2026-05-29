use std::collections::HashSet;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::timeout;
use bytes::{BytesMut, Buf};
use tracing::{info, warn, debug};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use mqtt_core::{
    codec::{decode, encode},
    packet::{
        connect::{ConnAck, ConnectReason},
        disconnect::{Disconnect, DisconnectReason},
        ping::PingResp,
        puback::{PubAck, PubComp, PubRec, PubRel, PubReason},
        publish::Publish,
        subscribe::{SubAck, SubAckReason},
        unsubscribe::{UnsubAck, UnsubAckReason},
        ControlPacket,
    },
    properties::read_variable_length,
    version::ProtocolVersion,
    QoS,
};

use crate::broker::{ClientToBrokerMessage, BrokerToClientMessage};
use crate::session::PendingPublish;

pub struct Connection {
    stream: TcpStream,
    broker_tx: mpsc::Sender<ClientToBrokerMessage>,
    client_rx: mpsc::Receiver<BrokerToClientMessage>,
    client_tx: mpsc::Sender<BrokerToClientMessage>,
    read_buf: BytesMut,
    write_buf: BytesMut,
    client_id: String,
    version: ProtocolVersion,
    keep_alive: u16,
    received_qos2_packet_ids: HashSet<u16>,
}

impl Connection {
    pub fn new(
        stream: TcpStream,
        broker_tx: mpsc::Sender<ClientToBrokerMessage>,
    ) -> Self {
        let (client_tx, client_rx) = mpsc::channel(100);
        Self {
            stream,
            broker_tx,
            client_rx,
            client_tx,
            read_buf: BytesMut::with_capacity(4096),
            write_buf: BytesMut::with_capacity(4096),
            client_id: String::new(),
            version: ProtocolVersion::V311,
            keep_alive: 0,
            received_qos2_packet_ids: HashSet::new(),
        }
    }

    pub async fn handle(mut self) {
        let peer_addr = match self.stream.peer_addr() {
            Ok(addr) => addr.to_string(),
            Err(_) => "unknown".to_string(),
        };
        debug!(peer = %peer_addr, "Accepted new connection");

        // 1. Connection Handshake
        if let Err(e) = self.handshake().await {
            warn!(peer = %peer_addr, error = ?e, "Handshake failed");
            let _ = self.stream.shutdown().await;
            return;
        }

        info!(peer = %peer_addr, client_id = %self.client_id, version = ?self.version, "Client handshaked successfully");

        // 2. Start read/write loop with keep-alive timeout
        if let Err(e) = self.main_loop().await {
            warn!(client_id = %self.client_id, error = ?e, "Connection loop error");
        }

        // 3. Cleanup on disconnect
        let _ = self.broker_tx.send(ClientToBrokerMessage::Disconnect {
            client_id: self.client_id.clone(),
        }).await;
        let _ = self.stream.shutdown().await;
        info!(client_id = %self.client_id, "Connection closed");
    }

    async fn handshake(&mut self) -> Result<(), anyhow::Error> {
        // Read CONNECT packet (with a reasonable handshake timeout, e.g. 5 seconds)
        let connect_timeout = Duration::from_secs(5);
        let (first_packet, peeked_version) = timeout(connect_timeout, self.read_packet_first()).await??;

        let connect = match first_packet {
            ControlPacket::Connect(conn) => conn,
            other => {
                return Err(anyhow::anyhow!("Expected CONNECT packet, received: {:?}", other));
            }
        };

        self.client_id = connect.client_id.clone();
        self.version = peeked_version;
        self.keep_alive = connect.keep_alive;

        // Perform authentication
        let (auth_tx, mut auth_rx) = mpsc::channel(1);
        let _ = self.broker_tx.send(ClientToBrokerMessage::Connect {
            client_id: self.client_id.clone(),
            clean_session: connect.clean_start,
            session_expiry_interval: connect.properties.as_ref().and_then(|p| p.session_expiry_interval),
            username: connect.username.clone(),
            password: connect.password.clone(),
            tx: self.client_tx.clone(),
            respond_tx: auth_tx,
        }).await;

        let session_present = match auth_rx.recv().await {
            Some(Ok(present)) => present,
            Some(Err(err)) => {
                // Reject connection
                self.send_connack(false, ConnectReason::NotAuthorized).await?;
                return Err(anyhow::anyhow!("Authentication rejected: {}", err));
            }
            None => {
                self.send_connack(false, ConnectReason::ServerUnavailable).await?;
                return Err(anyhow::anyhow!("Broker did not respond to connect"));
            }
        };

        // Send successful CONNACK
        self.send_connack(session_present, ConnectReason::Success).await?;
        Ok(())
    }

    async fn send_connack(&mut self, session_present: bool, reason: ConnectReason) -> Result<(), anyhow::Error> {
        let connack = ConnAck {
            session_present,
            reason_code: reason,
            properties: None,
        };
        let packet = ControlPacket::ConnAck(connack);
        self.write_buf.clear();
        encode(&packet, self.version, &mut self.write_buf);
        self.stream.write_all(&self.write_buf).await?;
        self.stream.flush().await?;
        Ok(())
    }

    async fn read_packet_first(&mut self) -> Result<(ControlPacket, ProtocolVersion), anyhow::Error> {
        loop {
            if self.read_buf.len() >= 2 {
                let first_byte = self.read_buf[0];
                let packet_type = first_byte >> 4;
                if packet_type == 1 { // CONNECT
                    let mut tmp = &self.read_buf[1..];
                    if let Ok(remaining_len) = read_variable_length(&mut tmp) {
                        let consumed = self.read_buf.len() - 1 - tmp.remaining();
                        let header_len = 1 + consumed;
                        let total_len = header_len + remaining_len;
                        if self.read_buf.len() >= total_len {
                            // Protocol Name length is at payload index 0..2, "MQTT" at 2..6, Level at 6
                            let protocol_level_idx = header_len + 6;
                            if self.read_buf.len() > protocol_level_idx {
                                let level_byte = self.read_buf[protocol_level_idx];
                                let version = ProtocolVersion::from_u8(level_byte)?;
                                if let Some(packet) = decode(&mut self.read_buf, None)? {
                                    return Ok((packet, version));
                                }
                            }
                        }
                    }
                }
            }
            let bytes_read = self.stream.read_buf(&mut self.read_buf).await?;
            if bytes_read == 0 {
                return Err(anyhow::anyhow!("Connection closed by peer before CONNECT"));
            }
        }
    }

    async fn main_loop(&mut self) -> Result<(), anyhow::Error> {
        let timeout_duration = if self.keep_alive > 0 {
            Duration::from_secs((self.keep_alive as u64 * 3) / 2)
        } else {
            Duration::from_secs(3600 * 24)
        };

        loop {
            tokio::select! {
                read_res = timeout(timeout_duration, self.stream.read_buf(&mut self.read_buf)) => {
                    let bytes_read = read_res??;
                    if bytes_read == 0 {
                        return Ok(());
                    }

                    while let Some(packet) = decode(&mut self.read_buf, Some(self.version))? {
                        self.process_packet(packet).await?;
                    }
                }

                broker_msg = self.client_rx.recv() => {
                    match broker_msg {
                        Some(BrokerToClientMessage::Publish(pub_msg)) => {
                            self.send_publish(pub_msg).await?;
                        }
                        Some(BrokerToClientMessage::PubRel(packet_id)) => {
                            let pubrel = PubRel {
                                packet_id,
                                reason_code: PubReason::Success,
                                properties: None,
                            };
                            self.write_packet(&ControlPacket::PubRel(pubrel)).await?;
                        }
                        Some(BrokerToClientMessage::DisconnectGracefully) => {
                            info!(client_id = %self.client_id, "Gracefully disconnecting client");
                            if self.version == ProtocolVersion::V500 {
                                let disc = Disconnect {
                                    reason_code: DisconnectReason::SessionTakenOver,
                                    properties: None,
                                };
                                self.write_packet(&ControlPacket::Disconnect(disc)).await?;
                            }
                            return Ok(());
                        }
                        None => {
                            return Ok(());
                        }
                    }
                }
            }
        }
    }

    async fn process_packet(&mut self, packet: ControlPacket) -> Result<(), anyhow::Error> {
        debug!(client_id = %self.client_id, packet = ?packet, "Processing packet");

        match packet {
            ControlPacket::Connect(_) => {
                return Err(anyhow::anyhow!("Protocol error: received duplicate CONNECT"));
            }
            ControlPacket::Publish(pub_msg) => {
                self.handle_incoming_publish(pub_msg).await?;
            }
            ControlPacket::PubAck(ack) => {
                let _ = self.broker_tx.send(ClientToBrokerMessage::PubAck {
                    client_id: self.client_id.clone(),
                    packet_id: ack.packet_id,
                }).await;
            }
            ControlPacket::PubRec(rec) => {
                let _ = self.broker_tx.send(ClientToBrokerMessage::PubRec {
                    client_id: self.client_id.clone(),
                    packet_id: rec.packet_id,
                }).await;
            }
            ControlPacket::PubRel(rel) => {
                let _ = self.broker_tx.send(ClientToBrokerMessage::PubRel {
                    client_id: self.client_id.clone(),
                    packet_id: rel.packet_id,
                }).await;

                let pubcomp = PubComp {
                    packet_id: rel.packet_id,
                    reason_code: PubReason::Success,
                    properties: None,
                };
                self.write_packet(&ControlPacket::PubComp(pubcomp)).await?;
            }
            ControlPacket::PubComp(comp) => {
                let _ = self.broker_tx.send(ClientToBrokerMessage::PubComp {
                    client_id: self.client_id.clone(),
                    packet_id: comp.packet_id,
                }).await;
            }
            ControlPacket::Subscribe(sub) => {
                let (respond_tx, mut respond_rx) = mpsc::channel(1);
                let _ = self.broker_tx.send(ClientToBrokerMessage::Subscribe {
                    client_id: self.client_id.clone(),
                    filters: sub.filters.clone(),
                    subscription_identifier: sub.properties.as_ref().and_then(|p| p.subscription_identifiers.first().copied()),
                    respond_tx,
                }).await;

                if let Some(qoses) = respond_rx.recv().await {
                    let reasons = qoses
                        .into_iter()
                        .map(|qos| match qos {
                            QoS::AtMostOnce => SubAckReason::GrantedQoS0,
                            QoS::AtLeastOnce => SubAckReason::GrantedQoS1,
                            QoS::ExactlyOnce => SubAckReason::GrantedQoS2,
                        })
                        .collect();

                    let suback = SubAck {
                        packet_id: sub.packet_id,
                        reason_codes: reasons,
                        properties: None,
                    };
                    self.write_packet(&ControlPacket::SubAck(suback)).await?;
                }
            }
            ControlPacket::Unsubscribe(unsub) => {
                let (respond_tx, mut respond_rx) = mpsc::channel(1);
                let _ = self.broker_tx.send(ClientToBrokerMessage::Unsubscribe {
                    client_id: self.client_id.clone(),
                    filters: unsub.filters.clone(),
                    respond_tx,
                }).await;

                if let Some(results) = respond_rx.recv().await {
                    let reasons = results
                        .into_iter()
                        .map(|removed| {
                            if removed {
                                UnsubAckReason::Success
                            } else {
                                UnsubAckReason::NoSubscriptionExisted
                            }
                        })
                        .collect();

                    let unsuback = UnsubAck {
                        packet_id: unsub.packet_id,
                        reason_codes: reasons,
                        properties: None,
                    };
                    self.write_packet(&ControlPacket::UnsubAck(unsuback)).await?;
                }
            }
            ControlPacket::PingReq(_) => {
                self.write_packet(&ControlPacket::PingResp(PingResp)).await?;
            }
            ControlPacket::Disconnect(_) => {
                return Ok(());
            }
            _ => {}
        }

        Ok(())
    }

    async fn handle_incoming_publish(&mut self, pub_msg: Publish) -> Result<(), anyhow::Error> {
        let (respond_tx, mut respond_rx) = mpsc::channel(1);
        let packet_id_val = pub_msg.packet_id.unwrap_or(0);
        
        let should_deliver = match pub_msg.qos {
            QoS::ExactlyOnce => {
                if self.received_qos2_packet_ids.contains(&packet_id_val) {
                    let pubrec = PubRec {
                        packet_id: packet_id_val,
                        reason_code: PubReason::Success,
                        properties: None,
                    };
                    self.write_packet(&ControlPacket::PubRec(pubrec)).await?;
                    false
                } else {
                    self.received_qos2_packet_ids.insert(packet_id_val);
                    true
                }
            }
            _ => true,
        };

        if should_deliver {
            let expiry_secs = pub_msg.properties.as_ref().and_then(|p| p.message_expiry_interval);
            let _ = self.broker_tx.send(ClientToBrokerMessage::Publish {
                client_id: self.client_id.clone(),
                topic: pub_msg.topic.clone(),
                payload: pub_msg.payload.clone(),
                qos: pub_msg.qos,
                retain: pub_msg.retain,
                expiry_secs,
                respond_tx,
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
                    let _ = respond_rx.recv().await;
                    let pubrec = PubRec {
                        packet_id: packet_id_val,
                        reason_code: PubReason::Success,
                        properties: None,
                    };
                    self.write_packet(&ControlPacket::PubRec(pubrec)).await?;
                }
            }
        }

        Ok(())
    }

    async fn send_publish(&mut self, pub_msg: PendingPublish) -> Result<(), anyhow::Error> {
        let publish = Publish {
            dup: pub_msg.dup,
            qos: pub_msg.qos,
            retain: pub_msg.retain,
            topic: pub_msg.topic,
            packet_id: if pub_msg.qos == QoS::AtMostOnce { None } else { Some(pub_msg.packet_id) },
            payload: pub_msg.payload,
            properties: None,
        };
        self.write_packet(&ControlPacket::Publish(publish)).await?;
        Ok(())
    }

    async fn write_packet(&mut self, packet: &ControlPacket) -> Result<(), anyhow::Error> {
        self.write_buf.clear();
        encode(packet, self.version, &mut self.write_buf);
        self.stream.write_all(&self.write_buf).await?;
        self.stream.flush().await?;
        Ok(())
    }
}
