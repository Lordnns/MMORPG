use std::collections::HashMap;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use uuid::Uuid;
use crate::{BackendCommand, GameNetworkEvent, GameSocketBackend, GameSocketError, GameStream};

pub struct TcpBackend {
    peers: HashMap<Uuid, mpsc::Sender<Bytes>>,
}

impl GameSocketBackend for TcpBackend {
    fn run(mut self, mut cmd_rx: mpsc::UnboundedReceiver<BackendCommand>, event_tx: mpsc::UnboundedSender<GameNetworkEvent>) {
        let rt = Runtime::new().expect("Failed to create Tokio runtime");
        rt.block_on(async move {
            let (peer_reg_tx, mut peer_reg_rx) = mpsc::channel::<(Uuid, mpsc::Sender<Bytes>)>(16);

            loop {
                tokio::select! {
                    // New Peer Registered (from Connect or Accept)
                    Some((uuid, tx)) = peer_reg_rx.recv() => {
                        self.peers.insert(uuid, tx);
                    }

                    // Commands from Game Loop
                    Some(cmd) = cmd_rx.recv() => {
                        match cmd {
                            BackendCommand::Bind { addr, port } => {
                                let listener = TcpListener::bind(format!("{}:{}", addr, port)).await.expect("Bind failed");
                                let peer_reg_tx = peer_reg_tx.clone();
                                let event_tx = event_tx.clone();
                                // Spawn Listener Task
                                tokio::spawn(async move {
                                    while let Ok((socket, _)) = listener.accept().await {
                                        if let Err(e) = socket.set_nodelay(true) {
                                            tracing::warn!("Failed to set TCP_NODELAY on accepted socket: {}", e);
                                        }
                                        
                                        let uuid = Uuid::new_v4();
                                        // Notify Game Thread
                                        let _ = event_tx.send(GameNetworkEvent::Connected(uuid.into()));

                                        // Setup Peer Handler
                                        let (write_tx, write_rx) = mpsc::channel(100);
                                        TcpBackend::spawn_peer_handler(socket, uuid, event_tx.clone(), write_rx);

                                        // Register Write Handle
                                        let _ = peer_reg_tx.send((uuid, write_tx)).await;
                                    }
                                });
                            }
                            BackendCommand::Connect { addr, port } => {
                                if let Ok(socket) = TcpStream::connect(format!("{}:{}", addr, port)).await {
                                    if let Err(e) = socket.set_nodelay(true) {
                                        tracing::warn!("Failed to set TCP_NODELAY on connected socket: {}", e);
                                    }
                                    
                                    let uuid = Uuid::new_v4();
                                    let _ = event_tx.send(GameNetworkEvent::Connected(uuid.into()));

                                    let (write_tx, write_rx) = mpsc::channel(100);
                                    TcpBackend::spawn_peer_handler(socket, uuid, event_tx.clone(), write_rx);

                                    self.peers.insert(uuid, write_tx);
                                } else {
                                    let _ = event_tx.send(GameNetworkEvent::Error {
                                        connection: Default::default(),
                                        inner: GameSocketError::ConnectionError,
                                    });
                                }
                            }
                            BackendCommand::Send { connection, stream, data } => {
                                if let Some(tx) = self.peers.get(&connection) {
                                    // Prepend StreamID to payload
                                    let mut packet = BytesMut::with_capacity(2 + data.len());
                                    packet.put_u16(stream.stream_id);
                                    packet.put(data);

                                    // Send to Handler (which frames it with Length)
                                    let _ = tx.send(packet.freeze()).await;
                                }
                            }
                            BackendCommand::Shutdown => break,
                            BackendCommand::CreateStream{ connection, stream, reliability } => {
                                let _ = event_tx.send(GameNetworkEvent::StreamCreated(connection.into(), GameStream::new(stream, reliability)));
                            }
                            BackendCommand::CloseStream{ connection, stream } => {
                                let _ = event_tx.send(GameNetworkEvent::StreamClosed(connection.into(), stream.into()));
                            }
                        }
                    }
                }
            }
        });
    }
}

impl TcpBackend {
    pub fn new() -> Self {
        Self { peers: HashMap::new() }
    }

    /// Spawns a task that manages Reading AND Writing for a single socket.
    fn spawn_peer_handler(
        socket: TcpStream,
        uuid: Uuid,
        event_tx: mpsc::UnboundedSender<GameNetworkEvent>,
        mut write_rx: mpsc::Receiver<Bytes>
    ) {
        tokio::spawn(async move {
            // Apply Length-Delimited Framing
            // This transforms the raw stream into "packets" based on a u32 length header.
            let mut framed = Framed::new(socket, LengthDelimitedCodec::new());
            let mut known_streams: Vec<u16> = Vec::new();

            loop {
                tokio::select! {
                    // READ: Socket -> Game
                    maybe_frame = framed.next() => {
                        match maybe_frame {
                            Some(Ok(mut bytes)) => {
                                // Protocol: [StreamID: u16] [Payload...]
                                if bytes.len() >= 2 {
                                    let stream_id = bytes.get_u16();

                                    if !known_streams.contains(&stream_id) {
                                        known_streams.push(stream_id);
                                        let _ = event_tx.send(GameNetworkEvent::StreamCreated(
                                            uuid.into(),
                                            stream_id.into()
                                        ));
                                    }

                                    let payload = bytes.freeze();

                                    let _ = event_tx.send(GameNetworkEvent::Message {
                                        connection: uuid.into(),
                                        stream: stream_id.into(),
                                        data: payload,
                                    });
                                }
                            }
                            Some(Err(_)) | None => {
                                let _ = event_tx.send(GameNetworkEvent::Disconnected(uuid.into()));
                                break;
                            }
                        }
                    }

                    // WRITE: Game -> Socket
                    Some(packet) = write_rx.recv() => {
                        // The Codec automatically prepends the Length Header
                        let _ = framed.send(packet).await;
                    }
                }
            }
        });
    }
}