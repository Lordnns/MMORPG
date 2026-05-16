use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::str::FromStr;
use std::sync::Arc;
use std::thread;
use bytes::{BufMut, Bytes, BytesMut};
use tokio::sync::mpsc;
use tracing::{debug, error, info};
use uuid::Uuid;
use gns::sys::*;
use gns::*;
use crate::{BackendCommand, GameNetworkEvent, GameSocketBackend, GameStream};


enum ActiveSocket {
    Server(GnsSocket<IsServer>),
    Client(GnsSocket<IsClient>),
}

impl ActiveSocket {
    /// Generic send wrapper
    fn send_messages(&self, messages: Vec<GnsNetworkMessage<ToSend>>) {
        match self {
            ActiveSocket::Server(s) => s.send_messages(messages),
            ActiveSocket::Client(c) => c.send_messages(messages),
        };
    }

    /// Generic event poll wrapper
    fn poll_event<F>(&self, callback: F) where F: FnMut(GnsConnectionEvent)
    {
        match self {
            ActiveSocket::Server(s) => { let _ = s.poll_event::<100>(callback); },
            ActiveSocket::Client(c) => { let _ = c.poll_event::<100>(callback); },
        }
    }

    /// Generic message poll wrapper
    fn poll_messages<F>(&self, callback: F) where F: FnMut(&GnsNetworkMessage<ToReceive>)
    {
        match self {
            ActiveSocket::Server(s) => { let _ = s.poll_messages::<100>(callback); },
            ActiveSocket::Client(c) => { let _ = c.poll_messages::<100>(callback); },
        }
    }

    /// Helper to close connection on either socket type
    fn close_connection(&self, conn: GnsConnection) {
        match self {
            ActiveSocket::Server(s) => s.close_connection(conn, 0, "", false),
            ActiveSocket::Client(c) => c.close_connection(conn, 0, "", false),
        };
    }

    /// Helper to accept connections (No-op if client)
    fn try_accept(&self, conn: GnsConnection) -> Result<(), EResult> {
        if let ActiveSocket::Server(s) = self {
            s.accept(conn)
        } else {
            Ok(()) // Clients auto-accept implicitly by initiating, or ignore
        }
    }
}

pub struct GnsBackend {
    gns_global: Option<Arc<GnsGlobal>>,
    // Unified Socket Container
    socket: Option<ActiveSocket>,

    handle_to_uuid: HashMap<GnsConnection, Uuid>,
    uuid_to_handle: HashMap<Uuid, GnsConnection>,

    known_streams: HashMap<Uuid, Vec<u16>>
}

impl GameSocketBackend for GnsBackend {
    fn run(mut self, mut cmd_rx: mpsc::UnboundedReceiver<BackendCommand>, event_tx: mpsc::UnboundedSender<GameNetworkEvent> ) {
        // 1. Initialize GNS Global
        let gns_global = match GnsGlobal::get() {
            Ok(g) => g,
            Err(_) => {
                error!("Failed to initialize GNS Global. Aborting backend.");
                return;
            }
        };

        match gns_global.utils().set_global_config_value(
            ESteamNetworkingConfigValue::k_ESteamNetworkingConfig_NagleTime,
            GnsConfig::Int32(0) // 0 microseconds
        ) {
            Ok(_) => {},
            Err(_) => error!("Failed to set Nagle Time. GNS may not function properly."),
        }

        // Enable Debug Output
        gns_global.utils().enable_debug_output(
            ESteamNetworkingSocketsDebugOutputType::k_ESteamNetworkingSocketsDebugOutputType_Msg,
            |ty, message| debug!("[GNS Debug] {:#?}: {}", ty, message),
        );

        self.gns_global = Some(gns_global.clone());
        let mut did_work;
        loop {
            did_work = false;
            // A. Process Commands
            while let Ok(cmd) = cmd_rx.try_recv() {
                match cmd {
                    BackendCommand::Bind { addr, port } => {
                        let socket = GnsSocket::new(gns_global.clone());
                        let ip = Ipv4Addr::from_str(&addr).unwrap_or(Ipv4Addr::LOCALHOST);
                        match socket.listen(ip.into(), port) {
                            Ok(s) => {
                                self.socket = Some(ActiveSocket::Server(s));
                            }
                            Err(_) => {
                                error!("Failed to initialize GNS Global. Aborting backend.");
                                return;
                            }
                        }
                    }
                    BackendCommand::Connect { addr, port } => {
                        let socket = GnsSocket::new(gns_global.clone());
                        let ip = Ipv4Addr::from_str(&addr).unwrap_or(Ipv4Addr::LOCALHOST);

                        match socket.connect(ip.into(), port) {
                            Ok(s) => {
                                info!("GNS Connecting to {}:{}", addr, port);
                                self.socket = Some(ActiveSocket::Client(s));
                            }
                            Err(e) => error!("GNS Connect Failed: {:?}", e),
                        }
                    }
                    BackendCommand::Send { connection, stream, data } => {
                        if let Some(conn_handle) = self.uuid_to_handle.get(&connection) {
                            if let Some(socket) = &self.socket {
                                let flags = if stream.is_reliable() {
                                    k_nSteamNetworkingSend_Reliable
                                } else {
                                    k_nSteamNetworkingSend_Unreliable
                                };

                                // Framing
                                let mut packet_buf = BytesMut::with_capacity(2 + data.len());
                                packet_buf.put_u16(stream.stream_id);
                                packet_buf.put(data);
                                let final_bytes = packet_buf.freeze();

                                // Allocation
                                let msg = gns_global.utils().allocate_message(
                                    *conn_handle,
                                    flags,
                                    &final_bytes,
                                );

                                let lane = if stream.is_reliable() {
                                    0
                                } else {
                                    1
                                };
                                let msg = msg.set_lane(lane);

                                // Dispatch
                                socket.send_messages(vec![msg]);
                            }
                        }
                    }
                    BackendCommand::Shutdown => return,
                    BackendCommand::CreateStream { connection, stream, reliability } => {
                        let _ = event_tx.send(GameNetworkEvent::StreamCreated(connection.into(), GameStream::new(stream, reliability)));
                    },
                    BackendCommand::CloseStream { connection, stream } => {
                        let _ = event_tx.send(GameNetworkEvent::StreamClosed(connection.into(), stream.into()));
                    },
                }
            }

            // B. Poll Callbacks
            gns_global.poll_callbacks();

            // C. Poll Socket (Unified)
            if let Some(socket) = self.socket.take() {

                // 1. Poll Events
                socket.poll_event(|event| {
                    self.handle_event(&socket, event, &event_tx);
                    did_work = true;
                });

                // 2. Poll Messages
                socket.poll_messages(|message| {
                    self.handle_message(message, &event_tx);
                    did_work = true;
                });

                // Put the socket back
                self.socket = Some(socket);
            }

            // D. Yield
            if !did_work{
                thread::yield_now();
            }
        }
    }
}

impl GnsBackend {
    pub fn new() -> Self {
        Self {
            gns_global: None,
            socket: None,
            handle_to_uuid: HashMap::new(),
            uuid_to_handle: HashMap::new(),
            known_streams: HashMap::new(),
        }
    }

    fn handle_event(&mut self, socket: &ActiveSocket, event: GnsConnectionEvent, event_tx: &mpsc::UnboundedSender<GameNetworkEvent>) {
        let old_state = event.old_state();
        let new_state = event.info().state();
        let conn = event.connection();

        match (old_state, new_state) {
            // Accept Connection (Server Only)
            (ESteamNetworkingConnectionState::k_ESteamNetworkingConnectionState_None,
                ESteamNetworkingConnectionState::k_ESteamNetworkingConnectionState_Connecting) => {

                if let Err(e) = socket.try_accept(conn) {
                    error!("Failed to accept client: {:?}", e);
                } else {
                    let uuid = Uuid::new_v4();
                    self.handle_to_uuid.insert(conn, uuid);
                    self.uuid_to_handle.insert(uuid, conn);
                    let _ = event_tx.send(GameNetworkEvent::Connected(uuid.into()));
                    info!("GNS Accepted Client: {:?}", uuid);
                }
            }

            // Connected (Client or Server completion)
            (ESteamNetworkingConnectionState::k_ESteamNetworkingConnectionState_Connecting,
                ESteamNetworkingConnectionState::k_ESteamNetworkingConnectionState_Connected) => {

                // A lane is defined by a priority and a weight.
                let reliable_lane : GnsLane = (1,1);
                let unreliable_lane = (1,1);
                match socket {
                    ActiveSocket::Server(s) => {
                        let _ = s.configure_connection_lanes(conn, &[reliable_lane, unreliable_lane]);
                    },
                    ActiveSocket::Client(c) => {
                        match c.configure_connection_lanes(conn, &[reliable_lane, unreliable_lane]) {
                            Ok(_) => {},
                            Err(e) => error!("Failed to configure lanes: {:?}", e),
                        }
                    }
                }

                // Register if not already known
                if !self.handle_to_uuid.contains_key(&conn) {
                    let uuid = Uuid::new_v4();
                    self.handle_to_uuid.insert(conn, uuid);
                    self.uuid_to_handle.insert(uuid, conn);
                    let _ = event_tx.send(GameNetworkEvent::Connected(uuid.into()));
                    info!("GNS Connection Established: {:?}", uuid);
                }

            }

            // Disconnect
            (_, ESteamNetworkingConnectionState::k_ESteamNetworkingConnectionState_ClosedByPeer) |
            (_, ESteamNetworkingConnectionState::k_ESteamNetworkingConnectionState_ProblemDetectedLocally) => {
                if let Some(uuid) = self.handle_to_uuid.remove(&conn) {
                    self.uuid_to_handle.remove(&uuid);
                    self.known_streams.remove(&uuid);
                    let _ = event_tx.send(GameNetworkEvent::Disconnected(uuid.into()));
                    info!("GNS Disconnected: {:?}", uuid);
                }
                socket.close_connection(conn);
            }
            _ => {}
        }
    }

    // Fixed: Takes message by value (GnsMessage)
    fn handle_message(&mut self, message: &GnsNetworkMessage<ToReceive>, event_tx: &mpsc::UnboundedSender<GameNetworkEvent>) {
        let payload = message.payload();
        let conn = message.connection();

        if let Some(uuid) = self.handle_to_uuid.get(&conn) {
            if payload.len() >= 2 {
                // De-Frame
                let stream_id = u16::from_be_bytes([payload[0], payload[1]]);

                let streams = self.known_streams.entry(*uuid).or_default();
                if !streams.contains(&stream_id) {
                    streams.push(stream_id);
                    let _ = event_tx.send(GameNetworkEvent::StreamCreated(
                        (*uuid).into(),
                        stream_id.into()
                    ));
                }
                
                let data = Bytes::copy_from_slice(&payload[2..]);

                let _ = event_tx.send(GameNetworkEvent::Message {
                    connection: (*uuid).into(),
                    stream: stream_id.into(),
                    data,
                });
            }
        }
    }
}