use std::collections::HashMap;
use std::env;
use std::net::ToSocketAddrs;
use std::sync::Mutex;
use std::time::Duration;

use bevy::app::ScheduleRunnerPlugin;
use bevy::prelude::*;
use bytes::Bytes;
use game_sockets::protocols::QuicBackend;
use game_sockets::{
    GameConnection, GameNetworkEvent, GamePeer, GameStream, GameStreamReliability,
};
use redis::aio::ConnectionManager;
use shared::{ClientMsg, Heartbeat, ServerMsg};
use tokio::runtime::Runtime;
use tracing::{info, warn};
use uuid::Uuid;

// ─────────────────────────────────────────────────────────────────────────
// Config
// ─────────────────────────────────────────────────────────────────────────

#[derive(Resource)]
struct ServerConfig {
    id: String,
    listen_ip: String,
    port: u16,
    advertise_host: String,
    zone: String,
    max_players: usize,
    orchestrator_host: String,
    orchestrator_port: u16,
    redis_url: String,
}

impl ServerConfig {
    fn from_env() -> Self {
        // DS_ID is set by the orchestrator/agent at spawn time. If unset (standalone `cargo run` for testing), generate one.
        let id = env::var("DS_ID").unwrap_or_else(|_| Uuid::new_v4().to_string());

        Self {
            id,
            listen_ip: env::var("DS_LISTEN_IP").unwrap_or_else(|_| "0.0.0.0".into()),
            port: env::var("DS_PORT").unwrap_or_else(|_| "7001".into()).parse().unwrap(),
            advertise_host: env::var("DS_ADVERTISE_HOST").unwrap_or_else(|_| "localhost".into()),
            zone: env::var("DS_ZONE").unwrap_or_else(|_| "zone_A".into()),
            max_players: env::var("DS_MAX_PLAYERS").unwrap_or_else(|_| "200".into()).parse().unwrap(),
            orchestrator_host: env::var("ORCH_HOST").unwrap_or_else(|_| "localhost".into()),
            orchestrator_port: env::var("ORCH_PORT").unwrap_or_else(|_| "9000".into()).parse().unwrap(),
            redis_url: env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".into()),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Resources
// ─────────────────────────────────────────────────────────────────────────

/// QUIC peer accepting player connections.
#[derive(Resource)]
struct GamePeerRes(Mutex<GamePeer>);

/// QUIC peer connected outbound to the orchestrator for heartbeats.
#[derive(Resource)]
struct OrchPeerRes(Mutex<GamePeer>);

/// State of the outbound connection to the orchestrator.
#[derive(Resource, Default)]
struct OrchLink {
    conn: Option<GameConnection>,
    unreliable_stream: Option<GameStream>,
}

/// Redis client + dedicated tokio runtime for session validation.
#[derive(Resource)]
struct RedisRes {
    runtime: Runtime,
    conn: Mutex<ConnectionManager>,
}

impl RedisRes {
    fn new(url: &str) -> anyhow::Result<Self> {
        let runtime = Runtime::new()?;
        let conn = runtime.block_on(async {
            let client = redis::Client::open(url)?;
            ConnectionManager::new(client).await
        })?;
        Ok(Self { runtime, conn: Mutex::new(conn) })
    }
}

#[derive(Debug)]
struct PlayerInfo {
    player_id: String,
    username: String,
}

#[derive(Resource, Default)]
struct PlayerRegistry {
    players: HashMap<GameConnection, PlayerInfo>,
}

#[derive(Resource)]
struct HeartbeatTimer(Timer);

// ─────────────────────────────────────────────────────────────────────────
// Main
// ─────────────────────────────────────────────────────────────────────────

fn main() {
    let _ = dotenvy::dotenv();

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let config = ServerConfig::from_env();
    info!(
        id = %config.id,
        port = config.port,
        zone = %config.zone,
        advertise = %config.advertise_host,
        "starting dedicated server"
    );

    // Bind QUIC peer for player connections.
    let game_peer = GamePeer::new(QuicBackend::new());
    if let Err(e) = game_peer.listen(&config.listen_ip, config.port) {
        eprintln!("failed to bind game socket: {:?}", e);
        std::process::exit(1);
    }

    // Resolve orchestrator hostname to IP.
    let orch_ip = match format!("{}:{}", config.orchestrator_host, config.orchestrator_port)
        .to_socket_addrs()
        .ok()
        .and_then(|mut iter| iter.next())
    {
        Some(addr) => addr.ip().to_string(),
        None => {
            eprintln!("failed to resolve orchestrator host: {}", config.orchestrator_host);
            std::process::exit(1);
        }
    };

    info!(
        host = %config.orchestrator_host,
        resolved = %orch_ip,
        port = config.orchestrator_port,
        "resolved orchestrator address"
    );

    // Outbound QUIC peer to the orchestrator.
    let orch_peer = GamePeer::new(QuicBackend::new());
    if let Err(e) = orch_peer.connect(&orch_ip, config.orchestrator_port) {
        warn!("orchestrator connect failed (will retry implicitly): {:?}", e);
    }

    // Redis client.
    let redis = match RedisRes::new(&config.redis_url) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("failed to connect to redis: {:?}", e);
            std::process::exit(1);
        }
    };

    App::new()
        .add_plugins(MinimalPlugins.set(
            ScheduleRunnerPlugin::run_loop(Duration::from_millis(16)),
        ))
        .insert_resource(config)
        .insert_resource(GamePeerRes(Mutex::new(game_peer)))
        .insert_resource(OrchPeerRes(Mutex::new(orch_peer)))
        .insert_resource(OrchLink::default())
        .insert_resource(redis)
        .insert_resource(PlayerRegistry::default())
        .insert_resource(HeartbeatTimer(Timer::new(
            Duration::from_secs(5),
            TimerMode::Repeating,
        )))
        .add_systems(Update, (poll_game, poll_orch, send_heartbeat))
        .run();
}

// ─────────────────────────────────────────────────────────────────────────
// Polling: game (player connections)
// ─────────────────────────────────────────────────────────────────────────

fn poll_game(
    net: Res<GamePeerRes>,
    redis: Res<RedisRes>,
    config: Res<ServerConfig>,
    mut registry: ResMut<PlayerRegistry>,
) {
    let mut peer = net.0.lock().unwrap();

    loop {
        let event = match peer.poll() {
            Ok(Some(e)) => e,
            Ok(None) => break,
            Err(e) => { warn!("game poll error: {:?}", e); break; }
        };

        match event {
            GameNetworkEvent::Connected(conn) => {
                info!("client connected: {:?}", conn);
            }
            GameNetworkEvent::Disconnected(conn) => {
                if let Some(p) = registry.players.remove(&conn) {
                    info!("player {} ({}) left", p.username, p.player_id);
                }
            }
            GameNetworkEvent::Message { connection, stream, data } => {
                let Ok(msg) = serde_json::from_slice::<ClientMsg>(&data) else {
                    warn!("invalid msg from {:?}", connection);
                    continue;
                };
                handle_client_msg(&mut peer, &redis, &config, &mut registry, connection, &stream, msg);
            }
            GameNetworkEvent::Error { connection, inner } => {
                warn!("conn {:?} error: {:?}", connection, inner);
            }
            _ => {}
        }
    }
}

fn handle_client_msg(
    peer: &mut GamePeer,
    redis: &Res<RedisRes>,
    config: &Res<ServerConfig>,
    registry: &mut ResMut<PlayerRegistry>,
    connection: GameConnection,
    stream: &GameStream,
    msg: ClientMsg,
) {
    match msg {
        ClientMsg::Join { player_id } => {
            let reply = if registry.players.contains_key(&connection) {
                ServerMsg::Error { reason: "already joined".into() }
            } else if registry.players.len() >= config.max_players {
                ServerMsg::Error { reason: "server full".into() }
            } else {
                match validate_session(redis, &player_id, &config.id) {
                    Err(reason) => ServerMsg::Error { reason: reason.into() },
                    Ok(username) => {
                        info!("player {} ({}) joined", username, player_id);
                        registry.players.insert(connection, PlayerInfo {
                            player_id: player_id.clone(),
                            username: username.clone(),
                        });
                        ServerMsg::Welcome { username }
                    }
                }
            };

            let bytes = Bytes::from(serde_json::to_vec(&reply).unwrap());
            if let Err(e) = peer.send(&connection, stream, bytes) {
                warn!("send failed: {:?}", e);
            }
        }
        ClientMsg::Leave => {
            if let Some(p) = registry.players.remove(&connection) {
                info!("player {} ({}) left gracefully", p.username, p.player_id);
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Session validation against Redis (read → burn → verify binding)
// ─────────────────────────────────────────────────────────────────────────

fn validate_session(
    redis: &Res<RedisRes>,
    player_id: &str,
    server_id: &str,
) -> Result<String, &'static str> {
    redis.runtime.block_on(async {
        let mut conn = redis.conn.lock().unwrap().clone();
        let key = format!("session:{}", player_id);

        // 1. Read.
        let session: HashMap<String, String> = redis::cmd("HGETALL")
            .arg(&key)
            .query_async(&mut conn).await
            .map_err(|_| "redis unavailable")?;

        if session.is_empty() {
            return Err("invalid or expired session");
        }

        // 2. Burn — DEL returns 1 if we won the race, 0 if someone else already burned it.
        let deleted: i64 = redis::cmd("DEL")
            .arg(&key)
            .query_async(&mut conn).await
            .map_err(|_| "redis unavailable")?;

        if deleted == 0 {
            return Err("token already used");
        }

        // 3. Verify binding.
        if session.get("server_id").map(String::as_str) != Some(server_id) {
            return Err("token bound to different server");
        }

        // 4. Extract username.
        session.get("username")
            .cloned()
            .ok_or("malformed session")
    })
}

// ─────────────────────────────────────────────────────────────────────────
// Polling: orchestrator (outbound heartbeat link)
// ─────────────────────────────────────────────────────────────────────────

fn poll_orch(net: Res<OrchPeerRes>, mut link: ResMut<OrchLink>) {
    let mut peer = net.0.lock().unwrap();

    loop {
        let event = match peer.poll() {
            Ok(Some(e)) => e,
            Ok(None) => break,
            Err(e) => { warn!("orch poll error: {:?}", e); break; }
        };

        match event {
            GameNetworkEvent::Connected(conn) => {
                info!("connected to orchestrator: {:?}", conn);
                link.conn = Some(conn);
                if let Err(e) = peer.create_stream(conn, GameStreamReliability::Unreliable) {
                    warn!("create unreliable stream failed: {:?}", e);
                }
            }
            GameNetworkEvent::StreamCreated(_, stream) => {
                if !stream.is_reliable() {
                    link.unreliable_stream = Some(stream);
                }
            }
            GameNetworkEvent::Disconnected(_) => {
                warn!("lost orchestrator connection");
                link.conn = None;
                link.unreliable_stream = None;
            }
            _ => {}
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Heartbeat — every 5s, datagram to orchestrator
// ─────────────────────────────────────────────────────────────────────────

fn send_heartbeat(
    time: Res<Time>,
    mut timer: ResMut<HeartbeatTimer>,
    net: Res<OrchPeerRes>,
    link: Res<OrchLink>,
    config: Res<ServerConfig>,
    registry: Res<PlayerRegistry>,
) {
    timer.0.tick(time.delta());
    if !timer.0.just_finished() {
        return;
    }

    let (Some(conn), Some(stream)) = (link.conn, link.unreliable_stream.clone()) else {
        warn!("orchestrator link not ready, skipping heartbeat");
        return;
    };

    let hb = Heartbeat {
        id: config.id.clone(),
        ip: config.advertise_host.clone(),
        port: config.port,
        zone: config.zone.clone(),
        player_count: registry.players.len(),
        max_players: config.max_players,
    };
    let bytes = Bytes::from(serde_json::to_vec(&hb).unwrap());

    let peer = net.0.lock().unwrap();
    if let Err(e) = peer.send(&conn, &stream, bytes) {
        warn!("heartbeat send failed: {:?}", e);
    }
}