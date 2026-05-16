use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use bytes::{BufMut, Bytes, BytesMut};
use tokio::net::UdpSocket;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use uuid::Uuid;
use crate::{BackendCommand, GameNetworkEvent, GameSocketBackend, GameStream};

// Protocol Constants
const HEADER_SIZE: usize = 18; // 16 bytes (UUID) + 2 bytes (StreamID)

pub struct UdpBackend {
    socket: Option<Arc<UdpSocket>>,
    connections: HashMap<Uuid, SocketAddr>,
    known_streams: HashMap<Uuid, Vec<u16>>,
}

impl GameSocketBackend for UdpBackend {
    fn run(mut self, mut cmd_rx: mpsc::UnboundedReceiver<BackendCommand>, event_tx: mpsc::UnboundedSender<GameNetworkEvent>) {
        let rt = Runtime::new().expect("Failed to create Tokio runtime");
        rt.block_on(async move {
            let mut buf = [0u8; 2048];

            loop {
                // We construct the receive future here to satisfy the borrow checker
                let recv_future = async {
                    match &self.socket {
                        Some(s) => s.recv_from(&mut buf).await,
                        None => std::future::pending().await,
                    }
                };

                tokio::select! {
                    //Handle Commands
                    Some(cmd) = cmd_rx.recv() => {
                        if matches!(cmd, BackendCommand::Shutdown) { break; }
                        self.process_command(cmd, &event_tx).await;
                    }

                    //Handle Network Traffic
                    res = recv_future => {
                        match res {
                            Ok((len, addr)) => self.process_packet(&buf[..len], addr, &event_tx),
                            Err(_) => tokio::task::yield_now().await, // Simple error backoff
                        }
                    }
                }
            }
        })
    }
}

impl UdpBackend {
    pub fn new() -> Self {
        Self {
            socket: None,
            connections: HashMap::new(),
            known_streams: HashMap::new(),
        }
    }

    /// Command Processor
    async fn process_command(&mut self, cmd: BackendCommand, event_tx: &mpsc::UnboundedSender<GameNetworkEvent>) {
        match cmd {
            BackendCommand::Bind { addr, port } => {
                if self.socket.is_none() {
                    if let Ok(s) = UdpSocket::bind(format!("{}:{}", addr, port)).await {
                        self.socket = Some(Arc::new(s));
                    }
                }
            }
            BackendCommand::Connect { addr, port } => {
                if self.socket.is_none() {
                    if let Ok(s) = UdpSocket::bind("0.0.0.0:0".to_string()).await {
                        self.socket = Some(Arc::new(s));
                    }
                }
                
                if let Ok(socket_addr) = format!("{}:{}", addr, port).parse::<SocketAddr>() {
                    let uuid = Uuid::new_v4();
                    self.connections.insert(uuid, socket_addr);
                    let _ = event_tx.send(GameNetworkEvent::Connected(uuid.into()));
                }
            }
            BackendCommand::Send { connection, stream, data } => {
                if let Some(socket) = &self.socket {
                    if let Some(remote_addr) = self.connections.get(&connection) {
                        let mut packet = BytesMut::with_capacity(HEADER_SIZE + data.len());
                        packet.put_slice(connection.as_bytes());
                        packet.put_u16(stream.stream_id);
                        packet.put(data);
                        let _ = socket.send_to(&packet, remote_addr).await;
                    }
                }
            }
            BackendCommand::CreateStream { connection, stream, reliability } => {
                let _ = event_tx.send(GameNetworkEvent::StreamCreated(connection.into(), GameStream::new(stream, reliability)));
            }
            BackendCommand::CloseStream { connection, stream } => {
                let _ = event_tx.send(GameNetworkEvent::StreamClosed(connection.into(), stream.into()));
            }
            _ => {} // Handled in loop
        }
    }

    /// Packet Processor
    fn process_packet(&mut self, buf: &[u8], addr: SocketAddr, event_tx: &mpsc::UnboundedSender<GameNetworkEvent>) {
        if buf.len() < HEADER_SIZE { return; }

        let Ok(incoming_uuid) = Uuid::from_slice(&buf[0..16]) else { return };
        let stream_id = u16::from_be_bytes([buf[16], buf[17]]);

        // Auto-Accept / Update Address Logic
        match self.connections.entry(incoming_uuid) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(addr);
                let _ = event_tx.send(GameNetworkEvent::Connected(incoming_uuid.into()));
            }
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                if *entry.get() != addr {
                    entry.insert(addr);
                }
            }
        }

        let streams = self.known_streams.entry(incoming_uuid).or_default();
        if !streams.contains(&stream_id) {
            streams.push(stream_id);
            let _ = event_tx.send(GameNetworkEvent::StreamCreated(
                incoming_uuid.into(),
                stream_id.into()
            ));
        }

        // Dispatch Message
        let payload = Bytes::copy_from_slice(&buf[HEADER_SIZE..]);
        let _ = event_tx.send(GameNetworkEvent::Message {
            connection: incoming_uuid.into(),
            stream: stream_id.into(),
            data: payload
        });
    }
}