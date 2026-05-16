# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A Rust MMORPG infrastructure lab — orchestrator + gatekeeper + dedicated servers + per-host agent, glued together by Redis and a vendored QUIC networking library. There is no gameplay code; the focus is the control plane (autoscaling, session brokering, fleet management) and how it deploys across one or many VMs.

## Repository has two Cargo projects, not one

- **`Code/`** is a Cargo workspace (`Code/Cargo.toml`) with members: `shared`, `orchestrator`, `dedicated_server`, `gatekeeper`, `test_client`.
- **`VMCode/Host_Agent/`** is a **separate** Cargo project (its own `Cargo.toml`, own `Cargo.lock`, own `target/`). It is intentionally outside the workspace because it ships as a **native binary** in the `mmorpg-fleet-ds` .deb (the other services ship as Docker images). It depends on `../../Code/shared` by path.
- **`External_Dependencies/game_sockets/`** is a vendored networking library (`game_sockets` crate, QUIC/UDP/TCP backends). Both workspace members and the host agent consume it via path dependency with `default-features = false, features = ["quic"]`.

Run cargo commands from the right root: `cd Code && cargo <cmd>` for service crates, `cd VMCode/Host_Agent && cargo <cmd>` for the agent. `cargo build` at repo root won't find a manifest.

## Build / run

```bash
# Workspace (services)
cd Code && cargo build --release
cargo build --release -p orchestrator      # one crate
cargo run -p test_client                   # interactive CLI

# Host agent (separate project)
cd VMCode/Host_Agent && cargo build --release

# Full local stack (Redis + orch + gk; DS containers spawned dynamically)
docker compose up --build                  # uses docker-compose.yaml at repo root

# Build all six .deb packages — requires Linux + dpkg-dev + fakeroot
VERSION=0.1.0 IMAGE_NAMESPACE=lordnns \
HOST_AGENT_BIN=/path/to/host_agent \
./debian/build-debs.sh                     # outputs to dist/*.deb
```

No `cargo test` suite exists. There are no unit or integration tests in `Code/` or `VMCode/`. Don't claim a feature is verified by "running tests" — exercise it through `test_client` (REPL with `spawn`, `list`, `quit`, `crash`) against a running stack.

Release pipeline: pushing a `v*` tag triggers `.github/workflows/release.yml`, which builds three Docker images (pushed to GHCR), the host_agent binary, all six .debs, and a GitHub Release.

## Architecture

### Data flow

```
client → gatekeeper:3000  (HTTP POST /login or SSE GET /login/stream)
            │
            └── reads `server:*` from Redis, claims a slot via Lua,
                writes `session:<player_id>`, returns DS ip:port

client → DS:7001+        (QUIC, game traffic; DS validates session via Redis)

DS → orchestrator:9000   (QUIC heartbeats every 5s, JSON Heartbeat struct)
            │
            └── orchestrator writes `server:<ds_id>` to Redis with TTL=15s

orchestrator scaler (every 5s):
    reads `server:*`, computes free capacity, calls Spawner trait
    (LocalDockerSpawner | RemoteDockerSpawner) to spawn/evict DSes
```

In `remote-docker` mode the orchestrator HTTP-POSTs `/spawn` and `/kill` to a host_agent (`AGENT_PORT=8090`, `Authorization: Bearer <AGENT_TOKEN>`). Host agents heartbeat `host:<hostname>` into Redis; the orchestrator's `pick_idle_host` picks any host whose hash has `status=idle`.

### The Spawner trait is the deployment-mode switch

`Code/orchestrator/src/main.rs` defines `trait Spawner { spawn, locate, kill, list_all, kill_all }`. Two impls:

- **`LocalDockerSpawner`** — shells out to the local `docker` CLI. Used when `SPAWNER=local-docker`. The orchestrator container mounts the host's `/var/run/docker.sock` to make this work in compose.
- **`RemoteDockerSpawner`** — picks an idle host from Redis (`host:*` keys), HTTP-POSTs to that agent. Used when `SPAWNER=remote-docker`.

When adding behavior that touches container lifecycle (labels, env vars, port mapping), update both impls **and** the corresponding `docker_run` in `VMCode/Host_Agent/src/main.rs`. DS containers are labeled `mmorpg.role=ds`, `mmorpg.ds_id=<uuid>`, `mmorpg.ds_port=<port>`; orphan adoption (`adopt_orphans`) and listing (`list_all`) both rely on these labels.

### Redis is the only source of truth shared across processes

Three key namespaces:

- `server:<ds_id>` — hash of `{ip, port, zone, status, player_count, max_players, host, container_id}` with TTL=`SERVER_TTL` (default 15s). Written by orchestrator on heartbeat, read by scaler and gatekeeper. **TTL is the liveness signal** — if a DS stops heartbeating its key vanishes and the scaler treats it as gone.
- `host:<hostname>` — hash of `{status, container_id, ds_id, last_heartbeat}` with TTL=60s. Written by host_agent, read by orchestrator's `pick_idle_host` and bootstrap probe.
- `session:<player_id>` — hash of `{username, server_id, issued_at}` with TTL=`SESSION_TTL` (default 30s). Written by gatekeeper on successful login, validated by DS on join.

Slot allocation in `gatekeeper/src/main.rs` (`try_claim_slot`) uses a Lua script for atomic check-and-increment on `player_count` — preserve this when modifying allocation; non-atomic reads will let two players race onto the last slot.

### Orchestrator startup recovery (`adopt_orphans`)

On boot, the orchestrator calls `Spawner::list_all()` to find DSes already running (from a previous orch process). For each, if Redis has no fresh `server:<id>` key, it writes a `status=starting` placeholder. The DS is still heartbeating to the same `ORCH_ADVERTISE_HOST:ORCH_PORT`, so its next 5s heartbeat overwrites the placeholder with real state. **Do not change `ORCH_ADVERTISE_HOST` between restarts** — orphan adoption breaks because the DSes will be heartbeating to the wrong address. On clean shutdown the orchestrator instead calls `kill_all` + clears `server:*`.

### Configuration is all env vars

Every service reads its config from environment variables at startup (via `dotenvy` + `env::var` with defaults). There are no config files. The canonical reference for env var names and meanings is `Code/.env.exemple`. When in monolith deployment, `/etc/default/mmorpg-monolith` is the env file that `docker compose` consumes via the systemd unit.

## Deployment topologies

The `debian/` directory packages the same code in three shapes — see `README.md` for the install commands:

1. **Monolith** (`mmorpg-monolith`) — one VM, docker compose runs redis+orch+gk, orchestrator uses `local-docker` spawner to start DS containers on the same daemon.
2. **Central + fleet** (`mmorpg-central-fleet` + `mmorpg-fleet-ds`) — control-plane VM runs redis+orch+gk; each fleet-ds VM runs the host_agent natively (not in a container, because it needs to call `docker`). Orchestrator uses `remote-docker` spawner.
3. **Fully distributed** — one component per VM (`mmorpg-redis`, `mmorpg-gatekeeper`, `mmorpg-orchestrator`, `mmorpg-fleet-ds`).

If you're modifying anything that affects deployment (env var names, ports, container labels, Redis schema), you almost certainly need to update files in all three corresponding `debian/<pkg>/` trees — the .deb-staged `docker-compose.yaml`, the systemd unit, and `/etc/default/mmorpg-<name>` defaults all have copies. `debian/build-debs.sh` substitutes `__VERSION__`, `__REGISTRY__`, `__NAMESPACE__` placeholders at build time.
