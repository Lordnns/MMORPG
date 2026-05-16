use serde::{Deserialize, Serialize};

/// DS → Orchestrator, every 5s over UDP.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Heartbeat {
    pub id: String,
    pub ip: String,
    pub port: u16,
    pub zone: String,
    pub player_count: usize,
    pub max_players: usize,
}

impl Heartbeat {
    pub fn status(&self) -> ServerStatus {
        if self.player_count >= self.max_players {
            ServerStatus::Full
        } else {
            ServerStatus::Available
        }
    }
}

/// Gatekeeper → client.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ServerInfo {
    pub ip: String,
    pub port: u16,
    pub zone: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ServerStatus {
    Available,
    Full,
    Starting,
}

impl ServerStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Available => "available",
            Self::Full => "full",
            Self::Starting => "starting",
        }
    }
}

/// Client → DS.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ClientMsg {
    Join { player_id: String },
    Leave,
}

/// DS → Client.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ServerMsg {
    Welcome { username: String },
    Error { reason: String },
}

/// Client → Gatekeeper, POST /login body.
#[derive(Debug, Serialize, Deserialize)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}

/// Gatekeeper → Client, 200 response.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LoginResponse {
    pub player_id: String,
    pub server: ServerInfo,
}

/// Gatekeeper → Client, error response (401, 503).
#[derive(Debug, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub error: String,
}
/// SSE event types sent on GET /login/stream.
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum LoginEvent {
    /// Sent periodically while no server is available yet.
    Waiting { message: String, elapsed_ms: u64 },
    /// Terminal success event — payload identical to LoginResponse.
    Ready(LoginResponse),
    /// Terminal failure event.
    Error { error: String },
}

/// Status of a VM host in the fleet.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum HostStatus {
    Idle,
    Running,
    Starting,
    Stopping,
}

impl HostStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Running => "running",
            Self::Starting => "starting",
            Self::Stopping => "stopping",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "idle" => Some(Self::Idle),
            "running" => Some(Self::Running),
            "starting" => Some(Self::Starting),
            "stopping" => Some(Self::Stopping),
            _ => None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AgentStatus {
    pub hostname: String,
    pub status: HostStatus,
    pub container_id: Option<String>,
    pub ds_id: Option<String>,
}

/// Body of POST /spawn from orchestrator to agent.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SpawnRequest {
    pub ds_id: String,
    pub ds_port: u16,
    pub ds_advertise_host: String,
    pub ds_zone: String,
    pub ds_max_players: usize,
    pub orch_host: String,
    pub orch_port: u16,
    pub redis_url: String,
}

/// Successful response to POST /spawn.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SpawnResponse {
    pub container_id: String,
}

/// Body of POST /kill from orchestrator to agent.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct KillRequest {
    pub container_id: String,
}

/// Generic error reply for the agent HTTP API.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AgentError {
    pub error: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AgentDsInfo {
    pub ds_id: String,
    pub port: u16,
    pub container_id: String,
}