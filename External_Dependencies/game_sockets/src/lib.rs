pub mod protocols;

use std::thread;
use bytes::Bytes;
use thiserror::Error;
use tokio::sync::mpsc;
use uuid::Uuid;

#[derive(Debug)]
pub enum BackendCommand {
    Bind { addr: String, port: u16 },
    Connect { addr: String, port: u16 },
    Send { connection: Uuid, stream: GameStream, data: Bytes },
    CreateStream { connection: Uuid, stream: u16, reliability: GameStreamReliability },
    CloseStream { connection: Uuid, stream: u16 },
    Shutdown,
}

pub trait GameSocketBackend: Send + 'static {
    fn run(self, cmd_rx: mpsc::UnboundedReceiver<BackendCommand>, event_tx: mpsc::UnboundedSender<GameNetworkEvent>);
}

#[derive(Debug, Default, Copy, Clone, PartialEq, Eq, Hash)]
pub struct GameConnection {
    pub connection_id: uuid::Uuid
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
pub struct GameStream {
    pub stream_id: u16
}

const RELIABILITY_MASK: u16 = 0b11;
const ORDERING_MASK: u16 = 0b10;

impl GameStream {
    pub fn new(stream_id: u16, game_stream_reliability: GameStreamReliability) -> Self {
        let mut stream_id = stream_id << 2;
        if game_stream_reliability == GameStreamReliability::Ordered {
            stream_id |= ORDERING_MASK;
        }
        if game_stream_reliability == GameStreamReliability::Reliable {
            stream_id |= RELIABILITY_MASK;
        }

        Self {
            stream_id
        }
    }

    pub fn is_reliable(&self) -> bool {
        //Check last bit of stream_id
        self.stream_id & RELIABILITY_MASK != 0
    }

    pub fn is_ordered(&self) -> bool {
        //Check second last bit of stream_id
        self.stream_id & ORDERING_MASK != 0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum GameStreamReliability {
    Reliable,
    Unreliable,
    Ordered,
}

#[derive(Debug, Error)]
pub enum GameSocketError {
    #[error("Generic error from protocol : {inner_msg}.")]
    ProtocolError { inner_msg: String },
    #[error("Error initializing protocol : {inner_msg}.")]
    InitError { inner_msg: String},
    #[error("Error connecting to remote host.")]
    ConnectionError,
    #[error("Unable to bind socket.")]
    BindError(#[from] std::io::Error),
    #[error("Error sending a packet : {inner_msg}.")]
    SendFailed{ inner_msg: String}
}

#[derive(Debug)]
pub enum GameNetworkEvent {
    Connected(GameConnection),
    Disconnected(GameConnection),
    Message{
        connection: GameConnection,
        stream: GameStream,
        data: bytes::Bytes
    },
    Error {
        connection: GameConnection,
        inner: GameSocketError
    },
    StreamCreated(GameConnection, GameStream),
    StreamClosed(GameConnection, GameStream),
}

impl From<uuid::Uuid> for GameConnection {
    fn from(id: uuid::Uuid) -> Self {
        Self { connection_id: id }
    }
}

impl From<u16> for GameStream {
    fn from(id: u16) -> Self {
        Self { stream_id: id }
    }
}

#[derive(Debug)]
pub struct GamePeer {
    cmd_tx: Option<mpsc::UnboundedSender<BackendCommand>>,
    event_rx: Option<mpsc::UnboundedReceiver<GameNetworkEvent>>,
    thread_handle: Option<thread::JoinHandle<()>>,
    // We track stream IDs here since the backend doesn't return them usually
    next_stream_id: u16,
}

impl GamePeer {
    pub fn new<B: GameSocketBackend>(backend: B) -> Self {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let (event_tx, event_rx) = mpsc::unbounded_channel();

        // Spawn the Backend Thread
        let handle = thread::spawn(move || {
            backend.run(cmd_rx, event_tx);
        });

        Self {
            cmd_tx: Some(cmd_tx),
            event_rx: Some(event_rx),
            thread_handle: Some(handle),
            next_stream_id: 0,
        }
    }

    fn send_cmd(&self, cmd: BackendCommand) -> Result<(), GameSocketError> {
        self.cmd_tx.as_ref()
            .ok_or(GameSocketError::ConnectionError)?
            .send(cmd)
            .map_err(|_| GameSocketError::ConnectionError)
    }

    pub fn listen(&self, ip: &str, port: u16) -> Result<(), GameSocketError> {
        self.send_cmd(BackendCommand::Bind { addr: ip.to_string(), port })
    }

    pub fn connect(&self, addr: &str, port: u16) -> Result<(), GameSocketError> {
        self.send_cmd(BackendCommand::Connect { addr: addr.to_string(), port })
    }

    pub fn create_stream(&mut self, conn: GameConnection, reliability: GameStreamReliability) -> Result<(), GameSocketError> {
        self.next_stream_id += 1;
        self.send_cmd(BackendCommand::CreateStream {
            connection: conn.connection_id,
            stream: self.next_stream_id,
            reliability
        })
    }

    pub fn close_stream(&self, conn: GameConnection, stream: GameStream) -> Result<(), GameSocketError> {
        self.send_cmd(BackendCommand::CloseStream { connection: conn.connection_id, stream: stream.stream_id })
    }

    pub fn send(&self, conn: &GameConnection, stream: &GameStream, msg: Bytes) -> Result<(), GameSocketError> {
        self.send_cmd(BackendCommand::Send { connection: conn.connection_id, stream: stream.clone(), data: msg })
    }

    pub fn poll(&mut self) -> Result<Option<GameNetworkEvent>, GameSocketError> {
        match &mut self.event_rx {
            Some(rx) => match rx.try_recv() {
                Ok(e) => Ok(Some(e)),
                Err(mpsc::error::TryRecvError::Empty) => Ok(None),
                Err(_) => Err(GameSocketError::ConnectionError),
            },
            None => Err(GameSocketError::ProtocolError { inner_msg: "Not initialized".into() }),
        }
    }

    pub fn shutdown(&mut self) -> Result<(), GameSocketError> {
        let _ = self.send_cmd(BackendCommand::Shutdown);
        if let Some(h) = self.thread_handle.take() { let _ = h.join(); }
        Ok(())
    }
}