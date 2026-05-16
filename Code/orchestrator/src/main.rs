use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;
use game_sockets::protocols::QuicBackend;
use game_sockets::{GameNetworkEvent, GamePeer};
use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use shared::{AgentDsInfo, AgentStatus, Heartbeat, KillRequest, SpawnRequest, SpawnResponse};
use tokio::sync::Mutex;
use tokio::task;
use tokio::time::interval;
use tracing::{info, warn};
use uuid::Uuid;

// ─────────────────────────────────────────────────────────────────────────
// Configuration
// ─────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SpawnerKind {
    LocalDocker,
    RemoteDocker,
}

impl SpawnerKind {
    fn from_env() -> Self {
        match env::var("SPAWNER").as_deref() {
            Ok("remote-docker") => Self::RemoteDocker,
            _ => Self::LocalDocker,
        }
    }
}

struct Config {
    // Bind / advertise
    orch_listen_ip: String,
    orch_port: u16,
    orch_advertise_host: String,

    // Redis
    redis_url: String,

    // Scaling policy
    hot_capacity_min: usize,
    evict_buffer: usize,
    scaler_interval_secs: u64,
    server_ttl_secs: usize,
    max_spawn_per_tick: usize,
    max_evict_per_tick: usize,

    // DS config (passed to spawned containers)
    ds_zone: String,
    ds_max_players: usize,
    ds_advertise_host: String,
    ds_port_range_min: u16,
    ds_port_range_max: u16,

    // Spawner
    spawner_kind: SpawnerKind,
    ds_docker_image: String,
    ds_docker_network: String,

    // Remote agent
    agent_port: u16,
    agent_token: String,

    // Bootstrap
    ds_hosts: Vec<String>,
    ssh_probe_timeout_secs: u64,
}

impl Config {
    fn from_env() -> Self {
        let parse_port = |k: &str, default: &str| -> u16 {
            env::var(k).unwrap_or_else(|_| default.into()).parse()
                .unwrap_or_else(|_| panic!("invalid {k}"))
        };

        Self {
            orch_listen_ip: env::var("ORCH_LISTEN_IP").unwrap_or_else(|_| "0.0.0.0".into()),
            orch_port: parse_port("ORCH_PORT", "9000"),
            orch_advertise_host: env::var("ORCH_ADVERTISE_HOST").unwrap_or_else(|_| "localhost".into()),

            redis_url: env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".into()),

            hot_capacity_min: env::var("HOT_CAPACITY_MIN")
                .unwrap_or_else(|_| "400".into()).parse().unwrap(),
            evict_buffer: env::var("EVICT_BUFFER")
                .unwrap_or_else(|_| "300".into()).parse().unwrap(),
            max_spawn_per_tick: env::var("MAX_SPAWN_PER_TICK")
                .unwrap_or_else(|_| "3".into()).parse().unwrap(),
            max_evict_per_tick: env::var("MAX_EVICT_PER_TICK")
                .unwrap_or_else(|_| "1".into()).parse().unwrap(),
            scaler_interval_secs: env::var("SCALER_INTERVAL")
                .unwrap_or_else(|_| "5".into()).parse().unwrap(),
            server_ttl_secs: env::var("SERVER_TTL")
                .unwrap_or_else(|_| "15".into()).parse().unwrap(),

            ds_zone: env::var("DS_ZONE").unwrap_or_else(|_| "zone_A".into()),
            ds_max_players: env::var("DS_MAX_PLAYERS")
                .unwrap_or_else(|_| "200".into()).parse().unwrap(),
            ds_advertise_host: env::var("DS_ADVERTISE_HOST").unwrap_or_else(|_| "localhost".into()),
            ds_port_range_min: parse_port("DS_PORT_RANGE_MIN", "7001"),
            ds_port_range_max: parse_port("DS_PORT_RANGE_MAX", "7100"),

            spawner_kind: SpawnerKind::from_env(),
            ds_docker_image: env::var("DS_DOCKER_IMAGE")
                .unwrap_or_else(|_| "mmorpg/dedicated_server:latest".into()),
            ds_docker_network: env::var("DS_DOCKER_NETWORK")
                .unwrap_or_else(|_| "mmorpg".into()),

            agent_port: parse_port("AGENT_PORT", "8090"),
            agent_token: env::var("AGENT_TOKEN").unwrap_or_else(|_| "change-me".into()),

            ds_hosts: env::var("DS_HOSTS").unwrap_or_default()
                .split(',').map(str::trim).filter(|s| !s.is_empty())
                .map(String::from).collect(),
            ssh_probe_timeout_secs: env::var("AGENT_PROBE_TIMEOUT")
                .unwrap_or_else(|_| "2".into()).parse().unwrap(),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Spawner trait — abstracts where/how a DS container runs
// ─────────────────────────────────────────────────────────────────────────

/// Identifies a running DS in a way that survives orchestrator restart.
#[derive(Debug, Clone)]
struct Location {
    host: String,
    container_id: String,
}

/// A DS discovered by `list_all`, with enough info to adopt it.
#[derive(Debug, Clone)]
struct AdoptedDs {
    ds_id: String,
    port: u16,
    location: Location,
}

#[async_trait]
trait Spawner: Send + Sync {
    async fn spawn(&self, ds_id: &str, port: u16, env: &SpawnEnv) -> Result<Location>;
    async fn locate(&self, ds_id: &str) -> Result<Option<Location>>;
    async fn kill(&self, loc: &Location) -> Result<()>;
    async fn list_all(&self) -> Result<Vec<AdoptedDs>>;
    
    async fn kill_all(&self) -> Result<()> {
        let all = self.list_all().await.unwrap_or_default();
        info!(count = all.len(), "kill_all: removing DSes");
        for ds in all {
            if let Err(e) = self.kill(&ds.location).await {
                warn!(ds_id = %ds.ds_id, error = %e, "kill failed");
            }
        }
        Ok(())
    }
}

struct SpawnEnv {
    ds_zone: String,
    ds_max_players: usize,
    ds_advertise_host: String,
    orch_host: String,
    orch_port: u16,
    redis_url: String,
}

// ─────────────────────────────────────────────────────────────────────────
// LocalDockerSpawner — `docker` CLI on this host
// ─────────────────────────────────────────────────────────────────────────

struct LocalDockerSpawner {
    image: String,
    network: String,
}

#[async_trait]
impl Spawner for LocalDockerSpawner {
    async fn spawn(&self, ds_id: &str, port: u16, env: &SpawnEnv) -> Result<Location> {
        let port_map = format!("{port}:{port}/udp");
        let max_p = env.ds_max_players.to_string();
        let orch_p = env.orch_port.to_string();
        let label_role = "mmorpg.role=ds".to_string();
        let label_id = format!("mmorpg.ds_id={ds_id}");
        let label_port = format!("mmorpg.ds_port={port}");
        let env_id = format!("DS_ID={ds_id}");
        let env_port = format!("DS_PORT={port}");
        let env_zone = format!("DS_ZONE={}", env.ds_zone);
        let env_max = format!("DS_MAX_PLAYERS={max_p}");
        let env_adv = format!("DS_ADVERTISE_HOST={}", env.ds_advertise_host);
        let env_orch_h = format!("ORCH_HOST={}", env.orch_host);
        let env_orch_p = format!("ORCH_PORT={orch_p}");
        let env_redis = format!("REDIS_URL={}", env.redis_url);

        let output = tokio::process::Command::new("docker")
            .args([
                "run", "-d", "--rm",
                "--network", &self.network,
                "-p", &port_map,
                "--label", &label_role,
                "--label", &label_id,
                "--label", &label_port,
                "-e", &env_id,
                "-e", &env_port,
                "-e", &env_zone,
                "-e", &env_max,
                "-e", &env_adv,
                "-e", &env_orch_h,
                "-e", &env_orch_p,
                "-e", &env_redis,
                &self.image,
            ])
            .output().await
            .map_err(|e| anyhow::anyhow!("could not invoke docker CLI: {e}"))?;

        if !output.status.success() {
            anyhow::bail!(
                "docker run exit {}: stderr={:?} stdout={:?}",
                output.status,
                String::from_utf8_lossy(&output.stderr).trim(),
                String::from_utf8_lossy(&output.stdout).trim()
            );
        }

        let container_id = String::from_utf8(output.stdout)?
            .trim().to_string();

        Ok(Location { host: "localhost".into(), container_id })
    }

    async fn locate(&self, ds_id: &str) -> Result<Option<Location>> {
        let filter = format!("label=mmorpg.ds_id={ds_id}");
        let output = tokio::process::Command::new("docker")
            .args(["ps", "-q", "--filter", &filter])
            .output().await?;
        let id = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if id.is_empty() {
            Ok(None)
        } else {
            Ok(Some(Location { host: "localhost".into(), container_id: id }))
        }
    }

    async fn kill(&self, loc: &Location) -> Result<()> {
        let _ = tokio::process::Command::new("docker")
            .args(["rm", "-f", &loc.container_id])
            .output().await
            .context("docker rm -f")?;
        Ok(())
    }

    async fn list_all(&self) -> Result<Vec<AdoptedDs>> {
        let output = tokio::process::Command::new("docker")
            .args([
                "ps",
                "--filter", "label=mmorpg.role=ds",
                "--format", "{{.ID}}\t{{.Label \"mmorpg.ds_id\"}}\t{{.Label \"mmorpg.ds_port\"}}",
            ])
            .output().await?;

        if !output.status.success() {
            anyhow::bail!("docker ps failed: {}", String::from_utf8_lossy(&output.stderr));
        }

        let mut out = Vec::new();
        for line in String::from_utf8_lossy(&output.stdout).lines() {
            let parts: Vec<&str> = line.split('\t').collect();
            if parts.len() != 3 { continue; }
            let container_id = parts[0].trim().to_string();
            let ds_id = parts[1].trim().to_string();
            let Ok(port) = parts[2].trim().parse::<u16>() else { continue; };
            if ds_id.is_empty() || container_id.is_empty() { continue; }
            out.push(AdoptedDs {
                ds_id,
                port,
                location: Location { host: "localhost".into(), container_id },
            });
        }
        Ok(out)
    }
}

// ─────────────────────────────────────────────────────────────────────────
// RemoteDockerSpawner — HTTP to per-VM agent
// ─────────────────────────────────────────────────────────────────────────

struct RemoteDockerSpawner {
    agent_port: u16,
    agent_token: String,
    redis: Arc<Mutex<ConnectionManager>>,
    http: reqwest::Client,
}

impl RemoteDockerSpawner {
    fn agent_url(&self, host: &str, path: &str) -> String {
        format!("http://{host}:{port}{path}", host = host, port = self.agent_port)
    }

    /// Pick an idle host from Redis. Returns None if none available.
    async fn pick_idle_host(&self) -> Result<Option<String>> {
        let mut conn = self.redis.lock().await;
        let mut iter = conn.scan_match::<_, String>("host:*").await?;
        let mut hosts = Vec::new();
        while let Some(k) = iter.next_item().await {
            hosts.push(k);
        }
        drop(iter);

        for key in hosts {
            let fields: HashMap<String, String> = conn.hgetall(&key).await?;
            let status = fields.get("status").map(String::as_str);
            if status == Some("idle") {
                if let Some(name) = key.strip_prefix("host:") {
                    return Ok(Some(name.to_string()));
                }
            }
        }
        Ok(None)
    }

    /// Get every known agent hostname from Redis (regardless of status).
    async fn known_hosts(&self) -> Result<Vec<String>> {
        let mut conn = self.redis.lock().await;
        let mut iter = conn.scan_match::<_, String>("host:*").await?;
        let mut hosts = Vec::new();
        while let Some(k) = iter.next_item().await {
            if let Some(name) = k.strip_prefix("host:") {
                hosts.push(name.to_string());
            }
        }
        Ok(hosts)
    }
}

#[async_trait]
impl Spawner for RemoteDockerSpawner {
    async fn spawn(&self, ds_id: &str, port: u16, env: &SpawnEnv) -> Result<Location> {
        let Some(host) = self.pick_idle_host().await? else {
            anyhow::bail!("no idle host available in fleet");
        };

        let req = SpawnRequest {
            ds_id: ds_id.to_string(),
            ds_port: port,
            ds_advertise_host: env.ds_advertise_host.clone(),
            ds_zone: env.ds_zone.clone(),
            ds_max_players: env.ds_max_players,
            orch_host: env.orch_host.clone(),
            orch_port: env.orch_port,
            redis_url: env.redis_url.clone(),
        };

        let resp = self.http
            .post(self.agent_url(&host, "/spawn"))
            .bearer_auth(&self.agent_token)
            .json(&req)
            .send().await
            .context("agent /spawn request")?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("agent /spawn on {host} failed: {status} {body}");
        }

        let spawn_resp: SpawnResponse = resp.json().await?;
        Ok(Location { host, container_id: spawn_resp.container_id })
    }

    async fn locate(&self, ds_id: &str) -> Result<Option<Location>> {
        let mut conn = self.redis.lock().await;
        let mut iter = conn.scan_match::<_, String>("host:*").await?;
        let mut hosts = Vec::new();
        while let Some(k) = iter.next_item().await {
            hosts.push(k);
        }
        drop(iter);

        for key in hosts {
            let fields: HashMap<String, String> = conn.hgetall(&key).await?;
            if fields.get("ds_id").map(String::as_str) != Some(ds_id) {
                continue;
            }
            let Some(container_id) = fields.get("container_id").cloned() else { continue; };
            if container_id.is_empty() { continue; }
            let Some(host) = key.strip_prefix("host:") else { continue; };
            return Ok(Some(Location { host: host.to_string(), container_id }));
        }
        Ok(None)
    }

    async fn kill(&self, loc: &Location) -> Result<()> {
        let req = KillRequest { container_id: loc.container_id.clone() };
        let resp = self.http
            .post(self.agent_url(&loc.host, "/kill"))
            .bearer_auth(&self.agent_token)
            .json(&req)
            .send().await
            .context("agent /kill request")?;

        if !resp.status().is_success() {
            warn!(host = %loc.host, status = %resp.status(), "agent /kill failed");
        }
        Ok(())
    }

    async fn list_all(&self) -> Result<Vec<AdoptedDs>> {
        let hosts = self.known_hosts().await?;
        let mut found = Vec::new();

        for host in hosts {
            let url = self.agent_url(&host, "/list");
            let resp = self.http
                .get(&url)
                .bearer_auth(&self.agent_token)
                .timeout(Duration::from_secs(3))
                .send().await;

            match resp {
                Ok(r) if r.status().is_success() => {
                    let listings: Vec<AgentDsInfo> = match r.json().await {
                        Ok(v) => v,
                        Err(e) => {
                            warn!(%host, error = %e, "agent /list parse failed");
                            continue;
                        }
                    };
                    for info in listings {
                        found.push(AdoptedDs {
                            ds_id: info.ds_id,
                            port: info.port,
                            location: Location {
                                host: host.clone(),
                                container_id: info.container_id,
                            },
                        });
                    }
                }
                Ok(r) => warn!(%host, status = %r.status(), "agent /list non-200"),
                Err(e) => warn!(%host, error = %e, "agent /list failed"),
            }
        }
        Ok(found)
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Bootstrap — probe DS_HOSTS to fill Redis gaps from recent outages
// ─────────────────────────────────────────────────────────────────────────

async fn bootstrap_hosts(
    config: &Config,
    redis: &Arc<Mutex<ConnectionManager>>,
    http: &reqwest::Client,
) -> Result<()> {
    if config.spawner_kind != SpawnerKind::RemoteDocker || config.ds_hosts.is_empty() {
        return Ok(());
    }

    info!("probing {} candidate host(s) from DS_HOSTS", config.ds_hosts.len());

    let known: HashMap<String, ()> = {
        let mut conn = redis.lock().await;
        let mut iter = conn.scan_match::<_, String>("host:*").await?;
        let mut out = HashMap::new();
        while let Some(k) = iter.next_item().await {
            if let Some(name) = k.strip_prefix("host:") {
                out.insert(name.to_string(), ());
            }
        }
        out
    };

    let mut tasks = Vec::new();
    for host in &config.ds_hosts {
        if known.contains_key(host) {
            info!(%host, "already in Redis, skipping probe");
            continue;
        }
        let host = host.clone();
        let port = config.agent_port;
        let token = config.agent_token.clone();
        let timeout = Duration::from_secs(config.ssh_probe_timeout_secs);
        let http = http.clone();

        tasks.push(tokio::spawn(async move {
            let url = format!("http://{host}:{port}/status");
            let result = http.get(&url)
                .bearer_auth(&token)
                .timeout(timeout)
                .send().await;
            (host, result)
        }));
    }

    for task in tasks {
        let (host, result) = task.await?;
        match result {
            Ok(resp) if resp.status().is_success() => {
                match resp.json::<AgentStatus>().await {
                    Ok(s) => {
                        info!(%host, status = ?s.status, "agent alive, registering");
                        let mut conn = redis.lock().await;
                        let key = format!("host:{host}");
                        let _: () = redis::pipe()
                            .atomic()
                            .hset(&key, "status", s.status.as_str())
                            .hset(&key, "container_id", s.container_id.unwrap_or_default())
                            .hset(&key, "ds_id", s.ds_id.unwrap_or_default())
                            .hset(&key, "last_heartbeat", chrono_epoch())
                            .expire(&key, 60)
                            .query_async(&mut *conn).await?;
                    }
                    Err(e) => warn!(%host, error = %e, "agent status parse failed"),
                }
            }
            Ok(resp) => warn!(%host, status = %resp.status(), "agent probe non-200"),
            Err(e) => warn!(%host, error = %e, "agent probe failed"),
        }
    }

    Ok(())
}

fn chrono_epoch() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

// ─────────────────────────────────────────────────────────────────────────
// Adoption — recover state from a previous (potentially crashed) run
// ─────────────────────────────────────────────────────────────────────────

async fn adopt_orphans(
    spawner: &Arc<dyn Spawner>,
    redis: &Arc<Mutex<ConnectionManager>>,
    config: &Config,
) -> Result<()> {
    info!("scanning for orphaned DSes from previous run");
    let orphans = spawner.list_all().await?;

    if orphans.is_empty() {
        info!("no orphans found");
        return Ok(());
    }

    info!(count = orphans.len(), "found candidate orphans");

    for orphan in orphans {
        let key = format!("server:{}", orphan.ds_id);
        let mut conn = redis.lock().await;

        // If Redis already has a fresh entry, the DS is heartbeating fine.
        // Don't overwrite — let the heartbeat path keep it current.
        let exists: bool = redis::cmd("EXISTS")
            .arg(&key)
            .query_async(&mut *conn).await
            .unwrap_or(false);
        if exists {
            info!(ds_id = %orphan.ds_id, "already in Redis, skipping adoption");
            continue;
        }

        // Place a 'starting' marker. The DS is still alive and heartbeating
        // (its ORCH_HOST/port didn't change), so within ~5s the first heartbeat
        // will overwrite this with the real player_count and flip status to available.
        let result: Result<(), _> = redis::pipe()
            .atomic()
            .hset(&key, "port", orphan.port)
            .hset(&key, "host", &orphan.location.host)
            .hset(&key, "container_id", &orphan.location.container_id)
            .hset(&key, "status", "starting")
            .hset(&key, "player_count", 0usize)
            .hset(&key, "max_players", config.ds_max_players)
            .hset(&key, "zone", &config.ds_zone)
            .hset(&key, "ip", &config.ds_advertise_host)
            .expire(&key, config.server_ttl_secs as i64)
            .query_async(&mut *conn).await;

        match result {
            Ok(()) => info!(
                ds_id = %orphan.ds_id,
                port = orphan.port,
                host = %orphan.location.host,
                "adopted orphan"
            ),
            Err(e) => warn!(ds_id = %orphan.ds_id, error = %e, "adoption write failed"),
        }
    }

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────
// Heartbeat listener (DS heartbeats → Redis server:* entries)
// ─────────────────────────────────────────────────────────────────────────

async fn run_heartbeat_listener(
    config: Arc<Config>,
    redis: Arc<Mutex<ConnectionManager>>,
) -> Result<()> {
    let mut peer = GamePeer::new(QuicBackend::new());
    peer.listen(&config.orch_listen_ip, config.orch_port)?;
    info!(
        listen = %config.orch_listen_ip,
        port = config.orch_port,
        "heartbeat listener bound"
    );

    loop {
        match peer.poll()? {
            Some(GameNetworkEvent::Connected(conn)) => {
                info!("DS connected: {:?}", conn);
            }
            Some(GameNetworkEvent::Disconnected(conn)) => {
                info!("DS disconnected: {:?}", conn);
            }
            Some(GameNetworkEvent::Message { connection, data, .. }) => {
                let Ok(hb) = serde_json::from_slice::<Heartbeat>(&data) else {
                    warn!("invalid heartbeat from {:?}", connection);
                    continue;
                };
                let mut conn_g = redis.lock().await;
                if let Err(e) = write_heartbeat(&mut conn_g, &hb, config.server_ttl_secs).await {
                    warn!("redis write failed: {:?}", e);
                }
            }
            Some(_) => {}
            None => tokio::time::sleep(Duration::from_millis(10)).await,
        }
    }
}

async fn write_heartbeat(
    conn: &mut ConnectionManager,
    hb: &Heartbeat,
    ttl: usize,
) -> Result<()> {
    let key = format!("server:{}", hb.id);
    let status = hb.status();

    let _: () = redis::pipe()
        .atomic()
        .hset(&key, "ip", &hb.ip)
        .hset(&key, "port", hb.port)
        .hset(&key, "zone", &hb.zone)
        .hset(&key, "status", status.as_str())
        .hset(&key, "player_count", hb.player_count)
        .hset(&key, "max_players", hb.max_players)
        .expire(&key, ttl as i64)
        .query_async(conn).await?;
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────
// Scaler — capacity-based spawn + stateless eviction
// ─────────────────────────────────────────────────────────────────────────

#[derive(Debug)]
struct ServerSnapshot {
    id: String,
    status: String,
    player_count: usize,
    max_players: usize,
}

async fn read_servers(conn: &mut ConnectionManager) -> Result<Vec<ServerSnapshot>> {
    let mut iter = conn.scan_match::<_, String>("server:*").await?;
    let mut keys = Vec::new();
    while let Some(k) = iter.next_item().await { keys.push(k); }
    drop(iter);

    let mut out = Vec::with_capacity(keys.len());
    for key in keys {
        let fields: HashMap<String, String> = conn.hgetall(&key).await?;
        let Some(id) = key.strip_prefix("server:").map(String::from) else { continue; };
        out.push(ServerSnapshot {
            id,
            status: fields.get("status").cloned().unwrap_or_default(),
            player_count: fields.get("player_count").and_then(|s| s.parse().ok()).unwrap_or(0),
            max_players: fields.get("max_players").and_then(|s| s.parse().ok()).unwrap_or(0),
        });
    }
    Ok(out)
}

fn total_free_capacity(servers: &[ServerSnapshot]) -> usize {
    servers.iter()
        .filter(|s| s.status == "available")
        .map(|s| s.max_players.saturating_sub(s.player_count))
        .sum()
}

async fn pick_free_port(config: &Config, redis: &Arc<Mutex<ConnectionManager>>) -> Result<u16> {
    let mut conn = redis.lock().await;

    let mut iter = conn.scan_match::<_, String>("server:*").await?;
    let mut keys = Vec::new();
    while let Some(k) = iter.next_item().await {
        keys.push(k);
    }
    drop(iter);

    let mut used = std::collections::HashSet::new();
    for key in keys {
        let port: Option<u16> = conn.hget(&key, "port").await.ok();
        if let Some(p) = port {
            used.insert(p);
        }
    }
    drop(conn);

    for port in config.ds_port_range_min..=config.ds_port_range_max {
        if !used.contains(&port) {
            return Ok(port);
        }
    }
    anyhow::bail!(
        "no free port in range {}..{}",
        config.ds_port_range_min,
        config.ds_port_range_max
    );
}

async fn run_scaler(
    config: Arc<Config>,
    redis: Arc<Mutex<ConnectionManager>>,
    spawner: Arc<dyn Spawner>,
) -> Result<()> {
    let mut ticker = interval(Duration::from_secs(config.scaler_interval_secs));

    loop {
        ticker.tick().await;

        let servers = {
            let mut conn = redis.lock().await;
            match read_servers(&mut conn).await {
                Ok(s) => s,
                Err(e) => { warn!("read_servers failed: {:?}", e); continue; }
            }
        };

        let free = total_free_capacity(&servers);
        info!(
            free_slots = free,
            target = config.hot_capacity_min,
            buffer = config.evict_buffer,
            "fleet capacity"
        );

        // 1. Scale up if below floor.
        if free < config.hot_capacity_min {
            let deficit = config.hot_capacity_min - free;
            let needed = deficit.div_ceil(config.ds_max_players.max(1));
            let to_spawn = needed.min(config.max_spawn_per_tick);

            if needed > to_spawn {
                info!(
                    needed,
                    spawning = to_spawn,
                    "deficit exceeds per-tick spawn limit; spreading across ticks"
                );
            }

            for _ in 0..to_spawn {
                let ds_id = Uuid::new_v4().to_string();
                let port = match pick_free_port(&config, &redis).await {
                    Ok(p) => p,
                    Err(e) => { warn!("pick_free_port: {:?}", e); break; }
                };
                let env = SpawnEnv {
                    ds_zone: config.ds_zone.clone(),
                    ds_max_players: config.ds_max_players,
                    ds_advertise_host: config.ds_advertise_host.clone(),
                    orch_host: config.orch_advertise_host.clone(),
                    orch_port: config.orch_port,
                    redis_url: config.redis_url.clone(),
                };
                match spawner.spawn(&ds_id, port, &env).await {
                    Ok(loc) => {
                        info!(%ds_id, port, host = %loc.host, "spawned");

                        let key = format!("server:{ds_id}");
                        let mut conn = redis.lock().await;
                        let result: Result<(), _> = redis::pipe()
                            .atomic()
                            .hset(&key, "port", port)
                            .hset(&key, "host", &loc.host)
                            .hset(&key, "container_id", &loc.container_id)
                            .hset(&key, "status", "starting")
                            .hset(&key, "player_count", 0usize)
                            .hset(&key, "max_players", config.ds_max_players)
                            .hset(&key, "zone", &config.ds_zone)
                            .hset(&key, "ip", &config.ds_advertise_host)
                            .expire(&key, config.server_ttl_secs as i64)
                            .query_async(&mut *conn).await;
                        if let Err(e) = result {
                            warn!(error = %e, "pre-register write failed");
                        }
                    }
                    Err(e) => { warn!(error = %e, "spawn failed"); break; }
                }
            }
        }

        // 2. Evict empties if doing so leaves us with MIN+BUFFER free.
        let mut current_free = free;
        let mut evicted_this_tick = 0;
        let empties: Vec<_> = servers.iter()
            .filter(|s| s.status == "available" && s.player_count == 0)
            .collect();

        for s in empties {
            if evicted_this_tick >= config.max_evict_per_tick { break; }

            let after = current_free.saturating_sub(s.max_players);
            if after < config.hot_capacity_min + config.evict_buffer { break; }

            match spawner.locate(&s.id).await {
                Ok(Some(loc)) => {
                    info!(ds_id = %s.id, "evicting empty DS");
                    if let Err(e) = spawner.kill(&loc).await {
                        warn!(ds_id = %s.id, error = %e, "kill failed");
                    } else {
                        let key = format!("server:{}", s.id);
                        let mut conn = redis.lock().await;
                        let _: Result<i64, _> = redis::cmd("DEL").arg(&key).query_async(&mut *conn).await;
                        drop(conn);
                        current_free = after;
                        evicted_this_tick += 1;
                    }
                }
                Ok(None) => {
                    warn!(ds_id = %s.id, "could not locate DS for eviction");
                    let key = format!("server:{}", s.id);
                    let mut conn = redis.lock().await;
                    let _: Result<i64, _> = redis::cmd("DEL").arg(&key).query_async(&mut *conn).await;
                }
                Err(e) => warn!(ds_id = %s.id, error = %e, "locate failed"),
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Cleanup helpers used at shutdown
// ─────────────────────────────────────────────────────────────────────────

async fn cleanup_server_keys(redis: &Arc<Mutex<ConnectionManager>>) {
    let mut conn = redis.lock().await;
    let Ok(mut iter) = conn.scan_match::<_, String>("server:*").await else { return; };
    let mut keys = Vec::new();
    while let Some(k) = iter.next_item().await { keys.push(k); }
    drop(iter);
    for k in keys {
        let _: Result<i64, _> = redis::cmd("DEL").arg(&k).query_async(&mut *conn).await;
    }
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

    let config = Arc::new(Config::from_env());
    info!(
        listen = %config.orch_listen_ip,
        port = config.orch_port,
        advertise = %config.orch_advertise_host,
        spawner = ?config.spawner_kind,
        "starting orchestrator"
    );

    let client = redis::Client::open(config.redis_url.as_str())?;
    let redis_conn = ConnectionManager::new(client).await?;
    let redis = Arc::new(Mutex::new(redis_conn));

    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;

    // Bootstrap probe (remote-docker only).
    if let Err(e) = bootstrap_hosts(&config, &redis, &http).await {
        warn!(error = %e, "bootstrap probe failed");
    }

    // Build spawner.
    let spawner: Arc<dyn Spawner> = match config.spawner_kind {
        SpawnerKind::LocalDocker => Arc::new(LocalDockerSpawner {
            image: config.ds_docker_image.clone(),
            network: config.ds_docker_network.clone(),
        }),
        SpawnerKind::RemoteDocker => Arc::new(RemoteDockerSpawner {
            agent_port: config.agent_port,
            agent_token: config.agent_token.clone(),
            redis: redis.clone(),
            http: http.clone(),
        }),
    };

    // Adopt any orphaned DSes from a previous (potentially crashed) run.
    if let Err(e) = adopt_orphans(&spawner, &redis, &config).await {
        warn!(error = %e, "adoption failed");
    }

    // Launch background tasks.
    let listener_handle = task::spawn(run_heartbeat_listener(config.clone(), redis.clone()));
    let scaler_handle = task::spawn(run_scaler(config.clone(), redis.clone(), spawner.clone()));

    #[cfg(unix)]
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;

    #[cfg(unix)]
    tokio::select! {
        _ = tokio::signal::ctrl_c() => info!("received SIGINT, shutting down"),
        _ = sigterm.recv() => info!("received SIGTERM, shutting down"),
        _ = listener_handle => warn!("heartbeat listener exited unexpectedly"),
        _ = scaler_handle => warn!("scaler exited unexpectedly"),
    }

    #[cfg(not(unix))]
    tokio::select! {
        _ = tokio::signal::ctrl_c() => info!("received Ctrl+C, shutting down"),
        _ = listener_handle => warn!("heartbeat listener exited unexpectedly"),
        _ = scaler_handle => warn!("scaler exited unexpectedly"),
    }

    // Graceful cleanup: kill all DSes we manage, then clear Redis state.
    info!("cleaning up managed DSes");
    if let Err(e) = spawner.kill_all().await {
        warn!(error = %e, "kill_all failed during shutdown");
    }

    info!("cleaning up Redis server:* keys");
    cleanup_server_keys(&redis).await;

    info!("orchestrator exited cleanly");
    Ok(())
}