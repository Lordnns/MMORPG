use std::collections::HashMap;
use std::net::ToSocketAddrs;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use bytes::Bytes;
use clap::Parser;
use eventsource_stream::Eventsource;
use futures_util::stream::StreamExt;
use game_sockets::protocols::QuicBackend;
use game_sockets::{GameConnection, GameNetworkEvent, GamePeer, GameStream, GameStreamReliability};
use shared::{ClientMsg, LoginEvent, LoginResponse, ServerMsg};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::{mpsc, Mutex};

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "http://localhost:3000")]
    gatekeeper: String,
    /// Default username base when `spawn N` is invoked without a name.
    #[arg(long, default_value = "alice")]
    username: String,
    #[arg(long, default_value = "1234")]
    password: String,
}

#[derive(Debug, Clone, Copy)]
enum ClientCmd {
    Leave,
    Crash,
}

#[derive(Debug)]
enum ClientEvent {
    Waiting { id: usize, username: String, message: String, elapsed_ms: u64 },
    Joined { id: usize, username: String, server_ip: String, server_port: u16 },
    LeftGracefully { id: usize },
    Crashed { id: usize },
    Errored { id: usize, err: String },
}

struct ClientHandle {
    id: usize,
    username: String,
    server_ip: String,
    server_port: u16,
    cmd_tx: mpsc::UnboundedSender<ClientCmd>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let args = Arc::new(Args::parse());

    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<ClientEvent>();
    let clients: Arc<Mutex<HashMap<usize, ClientHandle>>> = Arc::new(Mutex::new(HashMap::new()));
    let next_id = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    print_banner();
    print_help(&args.username);

    // Event-printer task.
    let clients_print = clients.clone();
    tokio::spawn(async move {
        while let Some(ev) = event_rx.recv().await {
            match ev {
                ClientEvent::Waiting { id, username, message, elapsed_ms } => {
                    println!(
                        "[#{}] {} waiting... {} (elapsed {}ms)",
                        id, username, message, elapsed_ms
                    );
                }
                ClientEvent::Joined { id, username, server_ip, server_port } => {
                    println!("[#{}] {} joined → {}:{}", id, username, server_ip, server_port);
                }
                ClientEvent::LeftGracefully { id } => {
                    println!("[#{}] left gracefully", id);
                    clients_print.lock().await.remove(&id);
                }
                ClientEvent::Crashed { id } => {
                    println!("[#{}] crashed", id);
                    clients_print.lock().await.remove(&id);
                }
                ClientEvent::Errored { id, err } => {
                    println!("[#{}] ERROR: {}", id, err);
                    clients_print.lock().await.remove(&id);
                }
            }
        }
    });

    // REPL.
    let stdin = tokio::io::stdin();
    let mut reader = BufReader::new(stdin).lines();
    print_prompt();

    while let Ok(Some(line)) = reader.next_line().await {
        let parts: Vec<&str> = line.trim().split_whitespace().collect();
        if parts.is_empty() {
            print_prompt();
            continue;
        }

        match parts[0] {
            "spawn" => {
                let n: usize = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(1);
                let base = parts.get(2).copied().unwrap_or(args.username.as_str()).to_string();

                for _ in 0..n {
                    let id = next_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let username = if n == 1 && parts.get(2).is_none() {
                        base.clone()
                    } else {
                        format!("{}_{}", base, id)
                    };
                    spawn_client(
                        id,
                        username,
                        args.clone(),
                        clients.clone(),
                        event_tx.clone(),
                    ).await;
                }
            }
            "list" => {
                let map = clients.lock().await;
                if map.is_empty() {
                    println!("(no active clients)");
                } else {
                    println!("active clients:");
                    let mut ids: Vec<&usize> = map.keys().collect();
                    ids.sort();
                    for id in ids {
                        let c = &map[id];
                        println!("  #{:<3} {:<20} → {}:{}", c.id, c.username, c.server_ip, c.server_port);
                    }
                }
            }
            "quit" => {
                let target = parts.get(1).copied().unwrap_or("");
                disconnect(target, ClientCmd::Leave, &clients).await;
            }
            "crash" => {
                let target = parts.get(1).copied().unwrap_or("");
                disconnect(target, ClientCmd::Crash, &clients).await;
            }
            "help" | "?" => print_help(&args.username),
            "exit" => {
                println!("disconnecting all and exiting...");
                disconnect("all", ClientCmd::Leave, &clients).await;
                tokio::time::sleep(Duration::from_millis(500)).await;
                break;
            }
            _ => {
                println!("unknown command. type 'help' for commands.");
            }
        }

        print_prompt();
    }

    Ok(())
}

async fn disconnect(
    target: &str,
    cmd: ClientCmd,
    clients: &Arc<Mutex<HashMap<usize, ClientHandle>>>,
) {
    let map = clients.lock().await;

    if target == "all" {
        for (id, c) in map.iter() {
            if c.cmd_tx.send(cmd).is_err() {
                println!("[#{}] could not send (already gone)", id);
            }
        }
        println!("(sent {:?} to all)", cmd);
    } else if let Ok(id) = target.parse::<usize>() {
        if let Some(c) = map.get(&id) {
            if c.cmd_tx.send(cmd).is_err() {
                println!("[#{}] could not send (already gone)", id);
            } else {
                println!("(sent {:?} to #{})", cmd, id);
            }
        } else {
            println!("no client with id #{}", id);
        }
    } else if target.is_empty() {
        let kw = match cmd {
            ClientCmd::Leave => "quit",
            ClientCmd::Crash => "crash",
        };
        println!("usage: {} <id|all>", kw);
    } else {
        println!("invalid target: {}", target);
    }
}

async fn spawn_client(
    id: usize,
    username: String,
    args: Arc<Args>,
    clients: Arc<Mutex<HashMap<usize, ClientHandle>>>,
    event_tx: mpsc::UnboundedSender<ClientEvent>,
) {
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<ClientCmd>();

    // Login via SSE stream — handles waiting + ready in one persistent connection.
    let http = match reqwest::Client::builder()
        .timeout(Duration::from_secs(120))
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            let _ = event_tx.send(ClientEvent::Errored {
                id, err: format!("http client: {e}"),
            });
            return;
        }
    };

    let url = format!(
        "{}/login/stream?username={}&password={}",
        args.gatekeeper,
        urlencoding::encode(&username),
        urlencoding::encode(&args.password),
    );

    let resp = match http.get(&url).send().await {
        Ok(r) => r,
        Err(e) => {
            let _ = event_tx.send(ClientEvent::Errored { id, err: format!("stream connect: {e}") });
            return;
        }
    };

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        let _ = event_tx.send(ClientEvent::Errored {
            id,
            err: format!("stream {status}: {body}"),
        });
        return;
    }

    let mut sse = resp.bytes_stream().eventsource();
    let mut saw_first_waiting = false;

    let login: LoginResponse = loop {
        let item = match sse.next().await {
            Some(it) => it,
            None => {
                let _ = event_tx.send(ClientEvent::Errored {
                    id, err: "stream ended without ready event".into(),
                });
                return;
            }
        };

        let event = match item {
            Ok(e) => e,
            Err(e) => {
                let _ = event_tx.send(ClientEvent::Errored {
                    id, err: format!("sse parse: {e}"),
                });
                return;
            }
        };

        let parsed: Result<LoginEvent, _> = serde_json::from_str(&event.data);
        let ev = match parsed {
            Ok(v) => v,
            Err(_) => continue, // unknown event payload, ignore
        };

        match ev {
            LoginEvent::Waiting { message, elapsed_ms } => {
                // Suppress the initial "looking..." event with elapsed_ms=0 — too noisy
                // for the burst case where most clients are served immediately.
                if elapsed_ms > 0 || saw_first_waiting {
                    let _ = event_tx.send(ClientEvent::Waiting {
                        id,
                        username: username.clone(),
                        message,
                        elapsed_ms,
                    });
                }
                saw_first_waiting = true;
            }
            LoginEvent::Ready(login_resp) => break login_resp,
            LoginEvent::Error { error } => {
                let _ = event_tx.send(ClientEvent::Errored {
                    id, err: format!("gatekeeper: {error}"),
                });
                return;
            }
        }
    };

    {
        let mut map = clients.lock().await;
        map.insert(id, ClientHandle {
            id,
            username: username.clone(),
            server_ip: login.server.ip.clone(),
            server_port: login.server.port,
            cmd_tx,
        });
    }

    tokio::spawn(client_loop(id, username, login, cmd_rx, event_tx));
}

async fn client_loop(
    id: usize,
    username: String,
    login: LoginResponse,
    mut cmd_rx: mpsc::UnboundedReceiver<ClientCmd>,
    event_tx: mpsc::UnboundedSender<ClientEvent>,
) {
    let ip = match format!("{}:{}", login.server.ip, login.server.port)
        .to_socket_addrs()
        .ok()
        .and_then(|mut i| i.next())
    {
        Some(addr) => addr.ip().to_string(),
        None => {
            let _ = event_tx.send(ClientEvent::Errored {
                id,
                err: format!("could not resolve {}", login.server.ip),
            });
            return;
        }
    };

    let mut peer = GamePeer::new(QuicBackend::new());
    if let Err(e) = peer.connect(&ip, login.server.port) {
        let _ = event_tx.send(ClientEvent::Errored {
            id,
            err: format!("QUIC connect: {e:?}"),
        });
        return;
    }

    let mut ds_conn: Option<GameConnection> = None;
    let mut reliable_stream: Option<GameStream> = None;
    let mut joined = false;

    loop {
        tokio::select! {
            biased;

            Some(cmd) = cmd_rx.recv() => {
                match cmd {
                    ClientCmd::Leave => {
                        if joined {
                            if let (Some(c), Some(s)) = (ds_conn, &reliable_stream) {
                                let leave = serde_json::to_vec(&ClientMsg::Leave).unwrap();
                                let _ = peer.send(&c, s, Bytes::from(leave));
                                tokio::time::sleep(Duration::from_millis(300)).await;
                            }
                        }
                        let _ = event_tx.send(ClientEvent::LeftGracefully { id });
                        return;
                    }
                    ClientCmd::Crash => {
                        let _ = event_tx.send(ClientEvent::Crashed { id });
                        return;
                    }
                }
            }

            _ = tokio::time::sleep(Duration::from_millis(20)) => {
                loop {
                    let event = match peer.poll() {
                        Ok(Some(e)) => e,
                        Ok(None) => break,
                        Err(e) => {
                            let _ = event_tx.send(ClientEvent::Errored {
                                id,
                                err: format!("poll: {e:?}"),
                            });
                            return;
                        }
                    };

                    match event {
                        GameNetworkEvent::Connected(c) => {
                            ds_conn = Some(c);
                            if let Err(e) = peer.create_stream(c, GameStreamReliability::Reliable) {
                                let _ = event_tx.send(ClientEvent::Errored {
                                    id,
                                    err: format!("create_stream: {e:?}"),
                                });
                                return;
                            }
                        }
                        GameNetworkEvent::StreamCreated(_, stream) if stream.is_reliable() => {
                            reliable_stream = Some(stream.clone());
                            let join = ClientMsg::Join { player_id: login.player_id.clone() };
                            let bytes = Bytes::from(serde_json::to_vec(&join).unwrap());
                            if let Some(c) = ds_conn {
                                let _ = peer.send(&c, &stream, bytes);
                            }
                        }
                        GameNetworkEvent::Message { data, .. } => {
                            match serde_json::from_slice::<ServerMsg>(&data) {
                                Ok(ServerMsg::Welcome { .. }) => {
                                    joined = true;
                                    let _ = event_tx.send(ClientEvent::Joined {
                                        id,
                                        username: username.clone(),
                                        server_ip: login.server.ip.clone(),
                                        server_port: login.server.port,
                                    });
                                }
                                Ok(ServerMsg::Error { reason }) => {
                                    let _ = event_tx.send(ClientEvent::Errored {
                                        id,
                                        err: format!("DS rejected JOIN: {reason}"),
                                    });
                                    return;
                                }
                                Err(e) => {
                                    let _ = event_tx.send(ClientEvent::Errored {
                                        id,
                                        err: format!("bad message: {e}"),
                                    });
                                    return;
                                }
                            }
                        }
                        GameNetworkEvent::Disconnected(_) => {
                            let _ = event_tx.send(ClientEvent::Errored {
                                id,
                                err: "DS dropped us".into(),
                            });
                            return;
                        }
                        _ => {}
                    }
                }
            }
        }
    }
}

fn print_prompt() {
    use std::io::Write;
    print!("> ");
    std::io::stdout().flush().ok();
}

fn print_banner() {
    println!();
    println!("┌─ test_client harness ───────────────────────────────────┐");
    println!("│  multi-client interactive tester (SSE login)            │");
    println!("└─────────────────────────────────────────────────────────┘");
}

fn print_help(default_base: &str) {
    println!();
    println!("commands:");
    println!("  spawn N [base]   spawn N clients (default 1)");
    println!("                   names: base_<id> (or just `base` when N==1 and no base given)");
    println!("                   default base when N>1: \"{}\"", default_base);
    println!("  list             show active clients");
    println!("  quit ID          graceful disconnect of client ID (sends Leave)");
    println!("  quit all         graceful disconnect of every client");
    println!("  crash ID         ungraceful disconnect of client ID");
    println!("  crash all        ungraceful disconnect of every client");
    println!("  help             this message");
    println!("  exit             graceful quit all, then exit");
    println!();
}