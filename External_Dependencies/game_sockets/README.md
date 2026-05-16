# Game Network Protocol Benchmark

A high-performance, asynchronous benchmarking suite written in Rust designed to evaluate transport protocols for real-time multiplayer games.

This tool simulates realistic game traffic patterns—mixing high-frequency unreliable data (e.g., player movement) with lower-frequency reliable data (e.g., game state)—to measure latency, jitter, and packet loss under various network conditions.

> **Note:** For a deep dive into the methodology, performance analysis, and results, please refer to the accompanying scientific paper: *[Insert Paper Title Here]* (Link pending).

## Supported Protocols

| Protocol | Description | Library |
|----------|-------------|---------|
| **UDP** | Raw datagrams (baseline, no reliability) | `std::net::UdpSocket` |
| **TCP** | Reliable stream (baseline comparison) | `std::net::TcpStream` |
| **QUIC** | Modern encrypted transport (IETF QUIC) | [`quinn`](https://github.com/quinn-rs/quinn) |
| **GNS** | Valve's GameNetworkingSockets (*Counter-Strike 2*, *Dota 2*) | [`game-networking-sockets-sys`](https://crates.io/crates/game-networking-sockets-sys) |

## Protocol Tweaks

### TCP

Nagle's algorithm is disabled via `TCP_NODELAY` to prevent send-side coalescing, ensuring each game tick produces an immediate packet.

### GNS

Nagle's algorithm is disabled to measure the latency of the underlying transport layer without artificial batching delay.

### QUIC

Several optimizations are applied to bring QUIC closer to a gaming-friendly configuration:

1. **BBR Congestion Control** — BBR models the network bottleneck bandwidth and RTT to keep in-flight data just below buffer capacity, reducing bufferbloat-induced latency spikes compared to the default Cubic controller.
2. **Datagram Pacing Disabled** — Standard QUIC smooths packet transmission over time. We disable pacing for immediate "fire-and-forget" dispatch, matching what a real game server would do.
3. **Aggressive Loss Detection** — The default initial RTT estimate (333ms) is replaced with a realistic gaming value (15ms), allowing QUIC to declare packets lost and trigger retransmission significantly faster at connection startup.

> Old results (before these tweaks) are available in the `benchmark_results_standard` folder for comparison.

## Traffic Pattern

Unlike standard bandwidth tools (like `iperf3`), this benchmark simulates a real game loop with two concurrent data streams:

| Stream | Frequency | Reliability | Simulates |
|--------|-----------|-------------|-----------|
| Movement/Physics | 60 Hz | Unreliable (fire-and-forget) | Player position, velocity, input state |
| Game State/RPC | 20 Hz | Reliable (ordered delivery) | Damage events, spawns, authoritative state |

## Benchmarking Methodology

### Hardware & OS

All measurements in the paper were performed on bare-metal hardware with OS-level core isolation:

| Item | Value |
|------|-------|
| CPU | AMD Ryzen 7 3800X, 8C/8T (SMT disabled), 3.6 GHz fixed (CPB disabled) |
| RAM | 64 GB DDR4 |
| OS | Debian 13.4 "Trixie" (kernel 6.x) |
| Rust | Stable toolchain (see `rust-toolchain.toml` or `rustc --version`) |
| Isolation | `isolcpus=2,3,4,5` — 4 cores reserved (2 per process) |

### Core Isolation

To minimize measurement noise, the server and client processes are pinned to **isolated CPU cores on the same CCX** (Core Complex) of the Ryzen 3800X. This eliminates scheduler interference and avoids inter-chiplet Infinity Fabric latency.

**Kernel boot parameters:**

```
isolcpus=2,3,4,5 nohz_full=2,3,4,5 rcu_nocbs=2,3,4,5 processor.max_cstate=0 nosoftlockup amd_pstate=disable
```

| Parameter | Purpose |
|-----------|---------|
| `isolcpus=2,3,4,5` | Removes cores 2–5 from the general scheduler |
| `nohz_full=2,3,4,5` | Disables the scheduler tick on isolated cores |
| `rcu_nocbs=2,3,4,5` | Offloads RCU callbacks to non-isolated cores |
| `processor.max_cstate=0` | Disables CPU sleep states (eliminates wake-up latency) |

**Process launch pattern:**

Each process is allocated **two isolated cores**: one for the application's game loop (main thread) and one for the asynchronous I/O runtime (Tokio). This prevents real-time scheduling starvation between the two threads and reflects realistic multi-threaded deployment.

```
Core 0    → OS, IRQs, housekeeping
Core 1    → Unused (buffer)
Cores 2,4 → Server (game loop + Tokio runtime)
Cores 3,5 → Client (game loop + Tokio runtime)
Cores 6,7 → Unused
```

```bash
# Server — pinned to cores 2,4, real-time FIFO priority
ip netns exec ns_server taskset -c 2,4 chrt -f 50 ./target/release/server ...

# Client — pinned to cores 3,5, real-time FIFO priority
ip netns exec ns_client taskset -c 3,5 chrt -f 50 ./target/release/client ...
```

### Network Emulation

Instead of applying `tc` on loopback (which bypasses much of the kernel networking stack), we use **Linux network namespaces connected by a veth pair**. This gives `tc`/`netem` a realistic Layer 2/3 path to operate on.

```
┌─────────────────┐          veth pair          ┌─────────────────┐
│   ns_server      │◄──────────────────────────►│   ns_client      │
│   10.0.0.1       │      netem (symmetric)      │   10.0.0.2       │
│   cores 2,4      │                             │   cores 3,5      │
└─────────────────┘                             └─────────────────┘
```

**Setup:**

```bash
# Create namespaces and veth pair
ip netns add ns_server && ip netns add ns_client
ip link add veth-srv type veth peer name veth-cli
ip link set veth-srv netns ns_server
ip link set veth-cli netns ns_client

# Assign addresses
ip netns exec ns_server ip addr add 10.0.0.1/24 dev veth-srv
ip netns exec ns_server ip link set veth-srv up
ip netns exec ns_client ip addr add 10.0.0.2/24 dev veth-cli
ip netns exec ns_client ip link set veth-cli up

# Disable offloading for accurate tc behavior
ip netns exec ns_server ethtool -K veth-srv tx off rx off tso off gso off gro off
ip netns exec ns_client ethtool -K veth-cli tx off rx off tso off gso off gro off
```

**Applying conditions (symmetric):**

```bash
# Example: 30ms ±5ms delay each way, 0.1% loss
ip netns exec ns_client tc qdisc add dev veth-cli root netem delay 30ms 5ms distribution normal loss 0.1%
ip netns exec ns_server tc qdisc add dev veth-srv root netem delay 30ms 5ms distribution normal loss 0.1%
```

### Test Scenarios

The benchmark suite (`run_full_suite.sh`) runs all four protocols through 10 network scenarios:

| # | Scenario | Delay (each way) | Jitter | Loss | Simulates |
|---|----------|-------------------|--------|------|-----------|
| 01 | Baseline | 0 | 0 | 0% | Direct veth (raw transport overhead) |
| 02 | Low latency | 10ms | ±1ms | 0% | LAN / same-region datacenter |
| 03 | Medium latency | 30ms | ±5ms | 0% | Cross-country |
| 04 | High latency | 50ms | ±10ms | 0% | Transatlantic |
| 05 | Low loss | 0 | 0 | 1% | Minor congestion |
| 06 | Medium loss | 0 | 0 | 2% | Congested link |
| 07 | High loss | 0 | 0 | 5% | Severe congestion / bad WiFi |
| 08 | Fiber | 5ms | ±1ms | 0% | Home fiber connection |
| 09 | Good connection | 30ms | ±5ms | 0.1% | Typical cable/DSL |
| 10 | Bad connection | 80ms | ±20ms | 2% | Unstable WiFi / mobile 4G |

Each configuration runs **10 times × 60 seconds** (plus 10s warmup discarded) for statistical validity.

### Statistical Reporting

Results are reported as percentile distributions (p50, p95, p99), not just means, since tail latency matters more than average latency for game networking. Each scenario produces per-protocol CSV files that can be processed with the included `stats` tool.

## Quick Start

### 1. Prerequisites

**Rust:** Stable toolchain (1.70+).

**System dependencies (for GNS):**

```bash
# Debian/Ubuntu
sudo apt install cmake clang libclang-dev libssl-dev libprotobuf-dev protobuf-compiler pkg-config libfontconfig1-dev

# macOS
brew install cmake protobuf llvm
```

### 2. Build

```bash
RUSTFLAGS="-C target-cpu=native" cargo build --release
```

> The `target-cpu=native` flag enables CPU-specific optimizations. Omit it if building for a different machine.

### 3. Run Manually

**Server** (in one terminal):

```bash
# Options: udp, tcp, quic, gns
./target/release/server --protocol gns --port 8080
```

**Client** (in another terminal or machine):

```bash
./target/release/client --protocol gns --ip 127.0.0.1 --port 8080 --duration 30 --results gns_test.csv
```

### 4. Run the Full Benchmark Suite

The automated suite handles namespace setup, core pinning, netem configuration, and multiple runs:

```bash
# Build first (as your normal user)
RUSTFLAGS="-C target-cpu=native" cargo build --release

# Run the suite (requires root for tc, namespaces, chrt)
sudo ./run_full_suite.sh
```

Options:

```bash
sudo ./run_full_suite.sh --runs 10 --duration 60 --skip-internet
```

### 5. Analyze Results

```bash
./target/release/stats gns_test.csv
```

Example output:

```
STATS FOR: STREAM 1 [Unreliable]
--------------------------------------------------
Reliability:
  Packet Loss:        0 pkts (0.00%)

Latency (RTT):
  Avg:                12 ms
  Jitter:             0.45 ms
  P99:                14 ms
```

## Reproducing the Paper Results

For full reproducibility of the results presented in the paper:

1. Use bare-metal hardware (not a VM) — virtualization adds 10–50µs of unpredictable jitter
2. Disable SMT and CPU boost in BIOS
3. Apply kernel boot parameters listed above (`isolcpus=2,3,4,5 ...`), then reboot
4. Run the preparation script before each benchmark session:
   ```bash
   sudo ./prepare_bench.sh
   ```
5. Run the full suite:
   ```bash
   sudo ./run_full_suite.sh
   ```
6. Results are saved in `benchmark_results/<timestamp>/` with system info captured automatically

## Project Structure

```
.
├── src/
│   ├── server.rs          # Multi-protocol server
│   ├── client.rs          # Benchmark client with CSV output
│   └── stats.rs           # Statistical analysis tool
├── scripts/               # Bash scripts
|     ├── run_full_suite.sh      # Automated benchmark suite
|     ├── prepare_bench.sh       # System tuning for benchmarking
|     ├── setup_netns.sh         # Network namespace setup
|     ├── cleanup.sh             # Restore system to normal
├── benchmark_results/     # Raw results (per-run CSVs)
└── benchmark_results_standard/  # Pre-tweak QUIC results for comparison
```

## License

MIT