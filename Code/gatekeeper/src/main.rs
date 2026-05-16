use std::collections::HashMap;
use std::convert::Infallible;
use std::env;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse, Response,
    },
    routing::{get, post},
    Json, Router,
};
use futures::stream::Stream;
use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use serde::{Deserialize, Serialize};
use shared::{
    ErrorResponse, LoginEvent, LoginRequest, LoginResponse, ServerInfo,
};
use tokio::sync::Mutex;
use tracing::{info, warn};
use uuid::Uuid;

// ─────────────────────────────────────────────────────────────────────────
// Config
// ─────────────────────────────────────────────────────────────────────────

struct Config {
    listen_ip: String,
    port: u16,
    redis_url: String,
    session_ttl_secs: usize,
    password: String,
    login_wait_secs: u64,
    login_poll_ms: u64,
    waiting_ping_secs: u64,
}

impl Config {
    fn from_env() -> Self {
        Self {
            listen_ip: env::var("GK_LISTEN_IP").unwrap_or_else(|_| "0.0.0.0".into()),
            port: env::var("GK_PORT").unwrap_or_else(|_| "3000".into()).parse().unwrap(),
            redis_url: env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".into()),
            session_ttl_secs: env::var("SESSION_TTL").unwrap_or_else(|_| "30".into()).parse().unwrap(),
            password: env::var("GK_PASSWORD").unwrap_or_else(|_| "1234".into()),
            login_wait_secs: env::var("LOGIN_WAIT_SECS")
                .unwrap_or_else(|_| "60".into()).parse().unwrap(),
            login_poll_ms: env::var("LOGIN_POLL_MS")
                .unwrap_or_else(|_| "500".into()).parse().unwrap(),
            waiting_ping_secs: env::var("LOGIN_PING_SECS")
                .unwrap_or_else(|_| "2".into()).parse().unwrap(),
        }
    }
}

struct AppState {
    config: Config,
    redis: Mutex<ConnectionManager>,
}

// ─────────────────────────────────────────────────────────────────────────
// Handlers
// ─────────────────────────────────────────────────────────────────────────

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}

/// Original POST /login — fast path. Returns 200 with server info or 503 if no
/// server is available right now. No waiting. For clients that want push-style
/// progress updates while waiting, use GET /login/stream instead.
async fn login(
    State(state): State<Arc<AppState>>,
    Json(req): Json<LoginRequest>,
) -> Response {
    if req.username.trim().is_empty() {
        return (
            StatusCode::UNAUTHORIZED,
            Json(ErrorResponse { error: "username required".into() }),
        ).into_response();
    }
    if req.password != state.config.password {
        return (
            StatusCode::UNAUTHORIZED,
            Json(ErrorResponse { error: "invalid credentials".into() }),
        ).into_response();
    }

    let chosen = {
        let mut conn = state.redis.lock().await;
        match find_available_server(&mut conn).await {
            Ok(Some(s)) => s,
            Ok(None) => {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(ErrorResponse { error: "No server available".into() }),
                ).into_response();
            }
            Err(e) => {
                warn!(error = %e, "redis query failed");
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(ErrorResponse { error: "registry unavailable".into() }),
                ).into_response();
            }
        }
    };

    let (player_id, chosen) = {
        let mut conn = state.redis.lock().await;
        match allocate(&mut conn, &req.username, state.config.session_ttl_secs).await {
            Ok(Some(pair)) => pair,
            Ok(None) => {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(ErrorResponse { error: "No server available".into() }),
                ).into_response();
            }
            Err(e) => {
                warn!(error = %e, "allocate failed");
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(ErrorResponse { error: "registry unavailable".into() }),
                ).into_response();
            }
        }
    };

    info!(username = %req.username, %player_id, server = %chosen.id, "login granted");

    Json(LoginResponse {
        player_id,
        server: ServerInfo {
            ip: chosen.ip,
            port: chosen.port,
            zone: chosen.zone,
        },
    }).into_response()
}

/// GET /login/stream?username=...&password=... — Server-Sent Events.
///
/// Streams `waiting` events while no server is available, then a terminal
/// `ready` event when one is allocated (or `error` on timeout/auth failure).
/// Connection closes after the terminal event.
#[derive(Deserialize)]
struct LoginQuery {
    username: String,
    password: String,
}

async fn login_stream(
    State(state): State<Arc<AppState>>,
    Query(q): Query<LoginQuery>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let stream = async_stream::stream! {
        // Initial frame so the client immediately knows we're alive.
        yield to_event(&LoginEvent::Waiting {
            message: "looking for a server...".into(),
            elapsed_ms: 0,
        });

        // Validate credentials.
        if q.username.trim().is_empty() {
            yield to_event(&LoginEvent::Error { error: "username required".into() });
            return;
        }
        if q.password != state.config.password {
            yield to_event(&LoginEvent::Error { error: "invalid credentials".into() });
            return;
        }

        let started = tokio::time::Instant::now();
        let deadline = started + Duration::from_secs(state.config.login_wait_secs);
        let poll = Duration::from_millis(state.config.login_poll_ms);
        let ping_every = Duration::from_secs(state.config.waiting_ping_secs);
        let mut last_ping = started;

        loop {
           let attempt = {
                let mut conn = state.redis.lock().await;
                allocate(&mut conn, &q.username, state.config.session_ttl_secs).await
            };

            match attempt {
                Ok(Some((player_id, chosen))) => {
                    info!(
                        username = %q.username,
                        %player_id,
                        server = %chosen.id,
                        waited_ms = started.elapsed().as_millis() as u64,
                        "login granted (stream)"
                    );

                    yield to_event(&LoginEvent::Ready(LoginResponse {
                        player_id,
                        server: ServerInfo {
                            ip: chosen.ip,
                            port: chosen.port,
                            zone: chosen.zone,
                        },
                    }));
                    return;
                }
                Ok(None) => {
                    // fall through to waiting logic
                }
                Err(e) => {
                    warn!(error = %e, "allocate failed in stream");
                    yield to_event(&LoginEvent::Error {
                        error: "registry unavailable".into(),
                    });
                    return;
                }
            }

            let now = tokio::time::Instant::now();
            if now >= deadline {
                yield to_event(&LoginEvent::Error {
                    error: format!(
                        "No server available after {}s wait",
                        state.config.login_wait_secs
                    ),
                });
                return;
            }

            if now.duration_since(last_ping) >= ping_every {
                yield to_event(&LoginEvent::Waiting {
                    message: "no server available yet, fleet is scaling up".into(),
                    elapsed_ms: started.elapsed().as_millis() as u64,
                });
                last_ping = now;
            }

            tokio::time::sleep(poll).await;
        }
    };

    Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
}

fn to_event(ev: &LoginEvent) -> Result<Event, Infallible> {
    let name = match ev {
        LoginEvent::Waiting { .. } => "waiting",
        LoginEvent::Ready(_) => "ready",
        LoginEvent::Error { .. } => "error",
    };
    let json = serde_json::to_string(ev)
        .unwrap_or_else(|_| r#"{"kind":"error","error":"serialize"}"#.into());
    Ok(Event::default().event(name).data(json))
}

// ─────────────────────────────────────────────────────────────────────────
// Redis queries
// ─────────────────────────────────────────────────────────────────────────

struct ChosenServer {
    id: String,
    ip: String,
    port: u16,
    zone: String,
}

async fn find_available_server(conn: &mut ConnectionManager) -> Result<Option<ChosenServer>> {
    let mut iter = conn.scan_match::<_, String>("server:*").await?;
    let mut keys = Vec::new();
    while let Some(k) = iter.next_item().await {
        keys.push(k);
    }
    drop(iter);

    let mut candidates: Vec<(usize, ChosenServer)> = Vec::new();

    for key in keys {
        let fields: HashMap<String, String> = conn.hgetall(&key).await?;

        if fields.get("status").map(String::as_str) != Some("available") {
            continue;
        }

        let max: usize = fields.get("max_players").and_then(|s| s.parse().ok()).unwrap_or(0);
        let cur: usize = fields.get("player_count").and_then(|s| s.parse().ok()).unwrap_or(0);
        if cur >= max { continue; }

        let Some(id) = key.strip_prefix("server:").map(String::from) else { continue };
        let Some(ip) = fields.get("ip").cloned() else { continue };
        let Some(port_s) = fields.get("port") else { continue };
        let Ok(port) = port_s.parse() else { continue };
        let zone = fields.get("zone").cloned().unwrap_or_else(|| "zone_A".into());

        candidates.push((cur, ChosenServer { id, ip, port, zone }));
    }

    candidates.sort_by(|a, b| b.0.cmp(&a.0));
    Ok(candidates.into_iter().next().map(|(_, s)| s))
}

async fn list_available_servers(conn: &mut ConnectionManager) -> Result<Vec<ChosenServer>> {
    let mut iter = conn.scan_match::<_, String>("server:*").await?;
    let mut keys = Vec::new();
    while let Some(k) = iter.next_item().await {
        keys.push(k);
    }
    drop(iter);

    let mut candidates: Vec<(usize, ChosenServer)> = Vec::new();

    for key in keys {
        let fields: HashMap<String, String> = conn.hgetall(&key).await?;

        if fields.get("status").map(String::as_str) != Some("available") {
            continue;
        }

        let max: usize = fields.get("max_players").and_then(|s| s.parse().ok()).unwrap_or(0);
        let cur: usize = fields.get("player_count").and_then(|s| s.parse().ok()).unwrap_or(0);
        if cur >= max { continue; }

        let Some(id) = key.strip_prefix("server:").map(String::from) else { continue };
        let Some(ip) = fields.get("ip").cloned() else { continue };
        let Some(port_s) = fields.get("port") else { continue };
        let Ok(port) = port_s.parse() else { continue };
        let zone = fields.get("zone").cloned().unwrap_or_else(|| "zone_A".into());

        candidates.push((cur, ChosenServer { id, ip, port, zone }));
    }

    candidates.sort_by(|a, b| b.0.cmp(&a.0));
    Ok(candidates.into_iter().map(|(_, s)| s).collect())
}

async fn write_session(
    conn: &mut ConnectionManager,
    player_id: &str,
    username: &str,
    server_id: &str,
    ttl: usize,
) -> Result<()> {
    let session_key = format!("session:{}", player_id);

    let _: () = redis::pipe()
        .atomic()
        .hset(&session_key, "username", username)
        .hset(&session_key, "server_id", server_id)
        .hset(&session_key, "issued_at", epoch())
        .expire(&session_key, ttl as i64)
        .query_async(conn).await?;
    Ok(())
}

/// Atomic check-and-claim. Returns true if the slot was claimed,
/// false if the server is already at or over capacity.
async fn try_claim_slot(
    conn: &mut ConnectionManager,
    server_id: &str,
) -> Result<bool> {
    const CLAIM_SCRIPT: &str = r#"
        local cur = tonumber(redis.call('HGET', KEYS[1], 'player_count') or '0')
        local max = tonumber(redis.call('HGET', KEYS[1], 'max_players') or '0')
        if max == 0 then return -1 end
        if cur >= max then return -1 end
        redis.call('HINCRBY', KEYS[1], 'player_count', 1)
        return cur + 1
    "#;

    let key = format!("server:{server_id}");
    let result: i64 = redis::Script::new(CLAIM_SCRIPT)
        .key(&key)
        .invoke_async(conn).await?;

    Ok(result >= 0)
}

/// Roll back a player_count increment (used if session write fails after claiming).
async fn release_slot(conn: &mut ConnectionManager, server_id: &str) {
    let key = format!("server:{server_id}");
    let _: Result<i64, _> = redis::cmd("HINCRBY")
        .arg(&key).arg("player_count").arg(-1)
        .query_async(conn).await;
}

fn epoch() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

async fn allocate(
    conn: &mut ConnectionManager,
    username: &str,
    session_ttl_secs: usize,
) -> Result<Option<(String, ChosenServer)>> {
    let candidates = list_available_servers(conn).await?;

    for server in candidates {
        let claimed = try_claim_slot(conn, &server.id).await?;
        if !claimed { continue; }  // raced — try next candidate

        let player_id = Uuid::new_v4().to_string();
        if let Err(e) = write_session(conn, &player_id, username, &server.id, session_ttl_secs).await {
            // Roll back the slot we claimed.
            release_slot(conn, &server.id).await;
            return Err(e);
        }

        return Ok(Some((player_id, server)));
    }

    Ok(None)
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
    info!(port = config.port, "starting gatekeeper");

    let client = redis::Client::open(config.redis_url.as_str())?;
    let redis_conn = tokio::time::timeout(
        Duration::from_secs(5),
        ConnectionManager::new(client),
    ).await??;

    let state = Arc::new(AppState {
        config,
        redis: Mutex::new(redis_conn),
    });

    let listener_addr = format!("{}:{}", state.config.listen_ip, state.config.port);

    let app = Router::new()
        .route("/health", get(health))
        .route("/login", post(login))
        .route("/login/stream", get(login_stream))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&listener_addr).await?;
    info!(addr = %listener_addr, "listening");
    axum::serve(listener, app).await?;

    Ok(())
}