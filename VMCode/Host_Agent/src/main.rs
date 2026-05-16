use std::env;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use shared::{AgentError, AgentStatus, HostStatus, KillRequest, SpawnRequest, SpawnResponse};
use tokio::sync::Mutex;
use tracing::{info, warn};

// ─────────────────────────────────────────────────────────────────────────
// Config
// ─────────────────────────────────────────────────────────────────────────

struct Config {
    listen_ip: String,
    port: u16,
    hostname: String,
    redis_url: String,
    agent_token: String,
    heartbeat_interval_secs: u64,
    heartbeat_ttl_secs: usize,
    redis_timeout_secs: u64,
    ds_image_label: String,
}

impl Config {
    fn from_env() -> Self {
        let redis_url = env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".into());

        // The orchestrator addresses this agent by IP (DS_HOSTS) and writes
        // `host:<IP>` in Redis on its bootstrap probe. The agent must use the
        // same key when it heartbeats — otherwise the two writes diverge and
        // the orchestrator's idle-host picker keeps hitting a stale entry.
        // Default to the local IP reachable from Redis (matches DS_HOSTS in
        // typical LAN setups). AGENT_HOSTNAME still wins if explicitly set.
        let host = env::var("AGENT_HOSTNAME").ok()
            .or_else(|| detect_local_ip(&redis_url))
            .or_else(|| hostname::get().ok().map(|h| h.to_string_lossy().to_string()))
            .unwrap_or_else(|| "unknown".to_string());

        Self {
            listen_ip: env::var("AGENT_LISTEN_IP").unwrap_or_else(|_| "0.0.0.0".into()),
            port: env::var("AGENT_PORT").unwrap_or_else(|_| "8090".into()).parse().unwrap(),
            hostname: host,
            redis_url,
            agent_token: env::var("AGENT_TOKEN").unwrap_or_else(|_| "change-me".into()),
            heartbeat_interval_secs: env::var("HEARTBEAT_INTERVAL").unwrap_or_else(|_| "30".into()).parse().unwrap(),
            heartbeat_ttl_secs: env::var("HEARTBEAT_TTL").unwrap_or_else(|_| "60".into()).parse().unwrap(),
            redis_timeout_secs: env::var("REDIS_TIMEOUT").unwrap_or_else(|_| "2".into()).parse().unwrap(),
            ds_image_label: env::var("DS_IMAGE_LABEL").unwrap_or_else(|_| "mmorpg.role=ds".into()),
        }
    }
}

/// Discover the local IP the OS would use to reach Redis. Uses the UDP-connect
/// trick: connecting a UDP socket triggers route selection without sending any
/// packets, and the resulting local_addr is the IP a peer would see us from.
fn detect_local_ip(redis_url: &str) -> Option<String> {
    let stripped = redis_url
        .trim_start_matches("redis://")
        .trim_start_matches("rediss://");
    let after_auth = stripped.split_once('@').map(|(_, b)| b).unwrap_or(stripped);
    let host_only = after_auth.split('/').next()?.split(':').next()?;

    let socket = std::net::UdpSocket::bind("0.0.0.0:0").ok()?;
    socket.connect((host_only, 80)).ok()?;
    Some(socket.local_addr().ok()?.ip().to_string())
}

// ─────────────────────────────────────────────────────────────────────────
// Local state — what the agent thinks the DS situation is
// ─────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct LocalState {
    status: HostStatus,
    container_id: Option<String>,
    ds_id: Option<String>,
}

impl LocalState {
    fn idle() -> Self {
        Self { status: HostStatus::Idle, container_id: None, ds_id: None }
    }
}

struct AppState {
    config: Config,
    local: Mutex<LocalState>,
    redis: Mutex<Option<ConnectionManager>>,
}

// ─────────────────────────────────────────────────────────────────────────
// Auth
// ─────────────────────────────────────────────────────────────────────────

fn check_auth(headers: &HeaderMap, expected: &str) -> bool {
    let Some(auth) = headers.get("authorization").and_then(|v| v.to_str().ok()) else {
        return false;
    };
    let Some(token) = auth.strip_prefix("Bearer ") else { return false };
    token == expected
}

fn unauthorized() -> Response {
    (StatusCode::UNAUTHORIZED, Json(AgentError { error: "unauthorized".into() })).into_response()
}

// ─────────────────────────────────────────────────────────────────────────
// HTTP handlers
// ─────────────────────────────────────────────────────────────────────────

async fn health() -> &'static str { "ok" }

async fn status(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response {
    if !check_auth(&headers, &state.config.agent_token) {
        return unauthorized();
    }

    let local = state.local.lock().await.clone();
    let body = AgentStatus {
        hostname: state.config.hostname.clone(),
        status: local.status,
        container_id: local.container_id,
        ds_id: local.ds_id,
    };
    Json(body).into_response()
}

/*async fn spawn(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<SpawnRequest>,
) -> Response {
    if !check_auth(&headers, &state.config.agent_token) {
        return unauthorized();
    }

    // Must be idle to accept a spawn.
    {
        let local = state.local.lock().await;
        if local.status != HostStatus::Idle {
            return (
                StatusCode::CONFLICT,
                Json(AgentError { error: format!("not idle (currently {:?})", local.status) }),
            ).into_response();
        }
    }

    // Transition to starting, publish to Redis immediately so the orchestrator
    // doesn't race and try to spawn here again.
    transition_state(&state, HostStatus::Starting, None, Some(req.ds_id.clone())).await;

    // Run docker.
    let container_id = match docker_run(&state.config, &req).await {
        Ok(id) => id,
        Err(e) => {
            warn!(error = %e, "docker run failed");
            transition_state(&state, HostStatus::Idle, None, None).await;
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(AgentError { error: format!("docker run failed: {e}") }),
            ).into_response();
        }
    };

    transition_state(&state, HostStatus::Running, Some(container_id.clone()), Some(req.ds_id)).await;

    Json(SpawnResponse { container_id }).into_response()
}*/

async fn spawn(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<SpawnRequest>,
) -> Response {
    info!(ds_id = %req.ds_id, "--- SPAWN REQUEST RECEIVED ---");

    // 1. Detailed Auth Check
    let auth_header = headers.get("authorization").and_then(|v| v.to_str().ok());
    if !check_auth(&headers, &state.config.agent_token) {
        warn!(
            provided_header = ?auth_header,
            expected_token = %state.config.agent_token,
            "Spawn rejected: Unauthorized"
        );
        return unauthorized();
    }
    info!("Auth successful for spawn request");

    // 2. Status Check
    {
        let local = state.local.lock().await;
        info!(current_status = ?local.status, "Checking host availability");
        if local.status != HostStatus::Idle {
            warn!(status = ?local.status, "Spawn rejected: Host is not idle");
            return (
                StatusCode::CONFLICT,
                Json(AgentError { error: format!("not idle (currently {:?})", local.status) }),
            ).into_response();
        }
    }

    info!(ds_id = %req.ds_id, "Transitioning to 'Starting' state");
    transition_state(&state, HostStatus::Starting, None, Some(req.ds_id.clone())).await;

    // 3. Docker Execution with Logs
    info!("Attempting Docker run...");
    let container_id = match docker_run(&state.config, &req).await {
        Ok(id) => {
            info!(container_id = %id, "Docker container started successfully");
            id
        },
        Err(e) => {
            warn!(error = %e, "CRITICAL: Docker run failed");
            transition_state(&state, HostStatus::Idle, None, None).await;
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(AgentError { error: format!("docker run failed: {e}") }),
            ).into_response();
        }
    };

    info!(ds_id = %req.ds_id, "Spawn complete. Transitioning to 'Running'");
    transition_state(&state, HostStatus::Running, Some(container_id.clone()), Some(req.ds_id)).await;

    Json(SpawnResponse { container_id }).into_response()
}

async fn kill(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<KillRequest>,
) -> Response {
    if !check_auth(&headers, &state.config.agent_token) {
        return unauthorized();
    }

    {
        let local = state.local.lock().await;
        if local.container_id.as_deref() != Some(req.container_id.as_str()) {
            return (
                StatusCode::NOT_FOUND,
                Json(AgentError { error: "container_id not running here".into() }),
            ).into_response();
        }
    }

    transition_state(&state, HostStatus::Stopping, Some(req.container_id.clone()), None).await;

    if let Err(e) = docker_stop(&req.container_id).await {
        warn!(error = %e, "docker stop failed");
        // Best-effort: still reset state because if stop failed the container is in some weird state anyway.
    }

    transition_state(&state, HostStatus::Idle, None, None).await;
    StatusCode::NO_CONTENT.into_response()
}

// ─────────────────────────────────────────────────────────────────────────
// Docker shellouts
// ─────────────────────────────────────────────────────────────────────────
/*
async fn docker_run(config: &Config, req: &SpawnRequest) -> Result<String> {
    let port_map = format!("{p}:{p}/udp", p = req.ds_port);
    let label_role = "mmorpg.role=ds".to_string();
    let label_id = format!("mmorpg.ds_id={}", req.ds_id);
    let env_id = format!("DS_ID={}", req.ds_id);
    let env_port = format!("DS_PORT={}", req.ds_port);
    let env_zone = format!("DS_ZONE={}", req.ds_zone);
    let env_max = format!("DS_MAX_PLAYERS={}", req.ds_max_players);
    let env_adv = format!("DS_ADVERTISE_HOST={}", req.ds_advertise_host);
    let env_orch_h = format!("ORCH_HOST={}", req.orch_host);
    let env_orch_p = format!("ORCH_PORT={}", req.orch_port);
    let env_redis = format!("REDIS_URL={}", req.redis_url);

    // Pull image name from env on agent side. The orchestrator dictates
    // the spawn parameters but not which image (agent owns that locally).
    let image = env::var("DS_DOCKER_IMAGE")
        .unwrap_or_else(|_| "mmorpg/dedicated_server:latest".into());

    let output = tokio::process::Command::new("docker")
        .args([
            "run", "-d", "--rm",
            "--network", "host",
            "-p", &port_map,
            "--label", &label_role,
            "--label", &label_id,
            "-e", &env_id,
            "-e", &env_port,
            "-e", &env_zone,
            "-e", &env_max,
            "-e", &env_adv,
            "-e", &env_orch_h,
            "-e", &env_orch_p,
            "-e", &env_redis,
            &image,
        ])
        .output().await?;

    let _ = config; // currently unused; kept in signature for future agent-side overrides
    let _ = label_role; // silence: passed as &str already

    if !output.status.success() {
        anyhow::bail!(
            "docker run failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }

    Ok(String::from_utf8(output.stdout)?.trim().to_string())
}
*/
async fn docker_run(config: &Config, req: &SpawnRequest) -> Result<String> {
    let port_map = format!("{p}:{p}/udp", p = req.ds_port);
    let image = env::var("DS_DOCKER_IMAGE")
        .unwrap_or_else(|_| "mmorpg/dedicated_server:latest".into());

    info!(
        image = %image,
        port = %port_map,
        ds_id = %req.ds_id,
        "Constructing Docker command"
    );

    let mut cmd = tokio::process::Command::new("docker");
    cmd.args([
        "run", "-d", "--rm",
        "--network", "host",
        "-p", &port_map,
        "--label", "mmorpg.role=ds",
        "--label", &format!("mmorpg.ds_id={}", req.ds_id),
        "-e", &format!("DS_ID={}", req.ds_id),
        "-e", &format!("DS_PORT={}", req.ds_port),
        "-e", &format!("DS_ZONE={}", req.ds_zone),
        "-e", &format!("DS_MAX_PLAYERS={}", req.ds_max_players),
        "-e", &format!("DS_ADVERTISE_HOST={}", req.ds_advertise_host),
        "-e", &format!("ORCH_HOST={}", req.orch_host),
        "-e", &format!("ORCH_PORT={}", req.orch_port),
        "-e", &format!("REDIS_URL={}", req.redis_url),
        &image,
    ]);

    info!("Executing: {:?}", cmd); // This prints the whole command string

    let output = cmd.output().await?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        warn!(
            exit_code = ?output.status.code(),
            stderr = %stderr.trim(),
            stdout = %stdout.trim(),
            "Docker process exited with error"
        );
        anyhow::bail!("docker run failed: {}", stderr.trim());
    }

    let cid = String::from_utf8(output.stdout)?.trim().to_string();
    info!(container_id = %cid, "Docker reported success");
    Ok(cid)
}

async fn docker_stop(container_id: &str) -> Result<()> {
    let output = tokio::process::Command::new("docker")
        .args(["stop", container_id])
        .output().await?;

    if !output.status.success() {
        anyhow::bail!(
            "docker stop failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(())
}

/// On agent startup: query Docker for any DS container already running on this host.
async fn discover_existing(label: &str) -> Result<Option<(String, Option<String>)>> {
    // Returns (container_id, ds_id) if a DS container is running here.
    let filter = format!("label={label}");
    let output = tokio::process::Command::new("docker")
        .args(["ps", "-q", "--filter", &filter])
        .output().await?;
    let container_id = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if container_id.is_empty() {
        return Ok(None);
    }

    // Read back the ds_id label so we can report it.
    let inspect = tokio::process::Command::new("docker")
        .args(["inspect", "-f", "{{ index .Config.Labels \"mmorpg.ds_id\" }}", &container_id])
        .output().await?;
    let ds_id = String::from_utf8_lossy(&inspect.stdout).trim().to_string();
    Ok(Some((container_id, if ds_id.is_empty() { None } else { Some(ds_id) })))
}

// ─────────────────────────────────────────────────────────────────────────
// State transitions — local + best-effort Redis write
// ─────────────────────────────────────────────────────────────────────────

async fn transition_state(
    state: &Arc<AppState>,
    status: HostStatus,
    container_id: Option<String>,
    ds_id: Option<String>,
) {
    {
        let mut local = state.local.lock().await;
        local.status = status;
        local.container_id = container_id.clone();
        local.ds_id = ds_id.clone();
    }
    write_redis_status(state, status, container_id, ds_id).await;
}

async fn write_redis_status(
    state: &Arc<AppState>,
    status: HostStatus,
    container_id: Option<String>,
    ds_id: Option<String>,
) {
    let key = format!("host:{}", state.config.hostname);
    let ttl = state.config.heartbeat_ttl_secs as i64;
    let now = epoch();

    let mut redis_g = state.redis.lock().await;
    let Some(redis) = redis_g.as_mut() else {
        // Redis not connected; next heartbeat will sync.
        return;
    };

    let write = tokio::time::timeout(
        Duration::from_secs(state.config.redis_timeout_secs),
        async {
            redis::pipe()
                .atomic()
                .hset(&key, "status", status.as_str())
                .hset(&key, "container_id", container_id.unwrap_or_default())
                .hset(&key, "ds_id", ds_id.unwrap_or_default())
                .hset(&key, "last_heartbeat", now)
                .expire(&key, ttl)
                .query_async::<()>(redis).await
        },
    ).await;

    if let Err(e) = write {
        warn!(error = ?e, "redis write timed out");
    } else if let Ok(Err(e)) = write {
        warn!(error = %e, "redis write errored");
    }
}

fn epoch() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

// ─────────────────────────────────────────────────────────────────────────
// Heartbeat loop — periodically reassert local state to Redis
// ─────────────────────────────────────────────────────────────────────────

async fn run_heartbeat(state: Arc<AppState>) {
    let mut ticker = tokio::time::interval(
        Duration::from_secs(state.config.heartbeat_interval_secs)
    );

    loop {
        ticker.tick().await;

        // Try to (re-)establish Redis connection if not present.
        {
            let mut redis_g = state.redis.lock().await;
            if redis_g.is_none() {
                let client = redis::Client::open(state.config.redis_url.as_str());
                match client {
                    Ok(c) => {
                        let mgr = tokio::time::timeout(
                            Duration::from_secs(state.config.redis_timeout_secs),
                            ConnectionManager::new(c),
                        ).await;
                        match mgr {
                            Ok(Ok(m)) => {
                                info!("redis connected");
                                *redis_g = Some(m);
                            }
                            Ok(Err(e)) => warn!(error = %e, "redis connect failed"),
                            Err(_) => warn!("redis connect timed out"),
                        }
                    }
                    Err(e) => warn!(error = %e, "redis client open failed"),
                }
            }
        }

        let local = state.local.lock().await.clone();
        write_redis_status(&state, local.status, local.container_id, local.ds_id).await;
    }
}

async fn list(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response {
    if !check_auth(&headers, &state.config.agent_token) {
        return unauthorized();
    }

    let local = state.local.lock().await;
    let mut dses = Vec::new();

    // Report the container if it's currently running or starting
    if let (Some(ds_id), Some(container_id)) = (&local.ds_id, &local.container_id) {
        dses.push(shared::AgentDsInfo {
            ds_id: ds_id.clone(),
            port: 7001, // Note: You might want to track the actual port in LocalState later
            container_id: container_id.clone(),
        });
    }

    Json(dses).into_response()
}

// ─────────────────────────────────────────────────────────────────────────
// Main
// ─────────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let config = Config::from_env();
    info!(hostname = %config.hostname, port = config.port, "starting host agent");

    // Discover any container already running (e.g. agent restarted mid-flight).
    let initial = match discover_existing(&config.ds_image_label).await {
        Ok(Some((cid, ds_id))) => {
            info!(container_id = %cid, ds_id = ?ds_id, "found existing DS container");
            LocalState {
                status: HostStatus::Running,
                container_id: Some(cid),
                ds_id,
            }
        }
        Ok(None) => {
            info!("no existing DS container");
            LocalState::idle()
        }
        Err(e) => {
            warn!(error = %e, "discover failed; assuming idle");
            LocalState::idle()
        }
    };

    let state = Arc::new(AppState {
        config,
        local: Mutex::new(initial),
        redis: Mutex::new(None),
    });

    // Heartbeat loop in the background.
    tokio::spawn(run_heartbeat(state.clone()));

    let app = Router::new()
        .route("/health", get(health))
        .route("/status", get(status))
        .route("/list", get(list))
        .route("/spawn", post(spawn))
        .route("/kill", post(kill))
        .with_state(state.clone());

    let listener = tokio::net::TcpListener::bind(
        format!("{}:{}", state.config.listen_ip, state.config.port)
    ).await?;

    info!("agent listening");
    axum::serve(listener, app).await?;
    Ok(())
}