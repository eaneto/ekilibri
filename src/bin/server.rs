use serde::Deserialize;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::RwLock,
    time::timeout,
};

use tracing::{debug, info, trace, warn};
use uuid::Uuid;

use clap::Parser;

#[derive(Debug, Deserialize, Clone)]
enum Strategies {
    /// Distribute requests randomly across the healthy servers.
    RoundRobin,
    /// Distribute requests to the healthy server with less
    /// connections at the moment.
    LeastConnections,
}

#[derive(Debug, Deserialize, Clone)]
struct Config {
    /// List of the server addresses
    servers: Vec<String>,
    /// Load balancing stragies.
    strategy: Strategies,
    /// Maximum number of failed requests to stop balancing to.
    max_fails: u64,
    /// Timeout to consider a connection as failed.
    connection_timeout: u32,
    /// Timeout writing the data to the server.
    write_timeout: u32,
    /// Timeout reading the data to the server.
    read_timeout: u32,
    /// Health check path
    health_check_path: String,
}

#[derive(Parser, Debug)]
struct Args {
    /// Path to configuration file
    #[arg(short, long, default_value = "ekilibri.toml")]
    file: String,
}

type HealthyServers = Arc<RwLock<HashMap<u8, String>>>;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    let config_content = match fs::read_to_string(&args.file).await {
        Ok(content) => content,
        Err(e) => panic!("Unable to read {} configuration file. {e}", args.file),
    };
    let config: Config = match toml::from_str(&config_content) {
        Ok(config) => config,
        Err(e) => panic!("Unable to parse configuration file {}. {e}", args.file),
    };
    debug!("Starting ekilibri with {:?}", config);

    let connections_counters = Arc::new(RwLock::new(Vec::with_capacity(config.servers.len())));
    for _ in &config.servers {
        connections_counters.write().await.push(AtomicU64::new(0));
    }

    let listener = match TcpListener::bind("127.0.0.1:8080").await {
        Ok(listener) => listener,
        Err(e) => panic!("Unable to start ekilibri on port 8080. Error = {:?}", e),
    };

    info!("Ekilibri listening at port 8080");

    let healthy_servers = HealthyServers::new(RwLock::new(HashMap::new()));
    for (id, server) in config.servers.iter().enumerate() {
        healthy_servers
            .write()
            .await
            .insert(id as u8, server.clone());
    }

    let healthy_servers_clone = Arc::clone(&healthy_servers);
    let config_clone = config.clone();
    tokio::spawn(async move {
        check_servers_health(config_clone, healthy_servers_clone).await;
    });

    accept_and_handle_connections(listener, connections_counters, config, healthy_servers).await;
}

async fn accept_and_handle_connections(
    listener: TcpListener,
    connections_counters: Arc<RwLock<Vec<AtomicU64>>>,
    config: Config,
    healthy_servers: HealthyServers,
) {
    loop {
        match listener.accept().await {
            Ok((mut ekilibri_stream, _)) => {
                let healthy_servers = Arc::clone(&healthy_servers);
                let connections_counters = Arc::clone(&connections_counters);
                let config = config.clone();
                tokio::spawn(async move {
                    handle_connection(
                        config,
                        healthy_servers,
                        connections_counters,
                        &mut ekilibri_stream,
                    )
                    .await;
                });
            }
            Err(e) => warn!("Error listening to socket. {e}"),
        }
    }
}

async fn handle_connection(
    config: Config,
    healthy_servers: HealthyServers,
    connections_counters: Arc<RwLock<Vec<AtomicU64>>>,
    ekilibri_stream: &mut TcpStream,
) {
    let request_id = Uuid::new_v4();
    let server_id = match config.strategy {
        Strategies::RoundRobin => choose_server_round_robin(healthy_servers).await,
        Strategies::LeastConnections => {
            choose_server_least_connections(healthy_servers, Arc::clone(&connections_counters))
                .await
        }
    };

    let counters = connections_counters.read().await;
    let counter = counters
        .get(server_id)
        .expect("The counters should be initialized with every possible server_id at this point");
    counter.fetch_add(1, Ordering::Relaxed);

    // TODO: Remove expect and handle case where the server got unhealthy.
    match TcpStream::connect(
        config.servers.get(server_id).expect(
            "The counters should be initialized with every possible server_id at this point",
        ),
    )
    .await
    {
        Ok(mut server_stream) => {
            if server_stream.set_nodelay(true).is_ok() {
                info!("Connected to server, server_id={server_id}, request_id={request_id}");
                process_request(request_id, ekilibri_stream, &mut server_stream).await;
            }
        }
        Err(e) => {
            warn!("Can't connect to server, server_id={server_id}, request_id={request_id}. {e}")
        }
    }

    counter.fetch_sub(1, Ordering::Relaxed);
}

async fn process_request(
    request_id: Uuid,
    mut ekilibri_stream: &mut TcpStream,
    mut server_stream: &mut TcpStream,
) {
    // Read data from client and send it to the server
    let mut buf_reader = BufReader::new(&mut ekilibri_stream);
    let mut buf = [0_u8; 1024];
    match buf_reader.read(&mut buf).await {
        Ok(size) => {
            trace!(
                "Successfully read data from client stream, size={size}, request_id={request_id}"
            )
        }
        Err(_) => {
            trace!("Unable to read data from client stream, request_id={request_id}");
            return;
        }
    }
    match server_stream.write_all(&buf).await {
        Ok(()) => {
            trace!("Successfully sent client data to server, request_id={request_id}")
        }
        Err(_) => {
            trace!("Unable to send client data to server, request_id={request_id}");
            return;
        }
    }

    // Reply client with same response from server
    let mut buf_reader = BufReader::new(&mut server_stream);
    let mut buf = [0_u8; 1024];
    match buf_reader.read(&mut buf).await {
        Ok(size) => {
            trace!(
                "Successfully read data from server stream, size={size}, request_id={request_id}"
            )
        }
        Err(_) => {
            trace!("Unable to read data from server stream, request_id={request_id}");
            return;
        }
    }
    match ekilibri_stream.write_all(&buf).await {
        Ok(()) => {
            trace!("Successfully sent server data to client, request_id={request_id}")
        }
        Err(_) => {
            trace!("Unable to send server data to client, request_id={request_id}");
        }
    }
}

async fn choose_server_round_robin(servers: HealthyServers) -> usize {
    let healthy_servers = servers.read().await;
    let possible_servers: Vec<u8> = healthy_servers.keys().copied().collect();
    let idx = rand::random::<usize>() % possible_servers.len();
    possible_servers[idx] as usize
}

async fn choose_server_least_connections(
    servers: HealthyServers,
    connections_counters: Arc<RwLock<Vec<AtomicU64>>>,
) -> usize {
    let mut chosen_server = 0;
    let counters = connections_counters.read().await;
    for server_id in servers.read().await.keys().copied() {
        let chosen_server_connections = counters
            .get(chosen_server)
            .expect(
                "The counters should be initialized with every possible server_id at this point",
            )
            .load(Ordering::Relaxed);
        let current_server_connections = counters
            .get(server_id as usize)
            .expect(
                "The counters should be initialized with every possible server_id at this point",
            )
            .load(Ordering::Relaxed);
        if chosen_server_connections > current_server_connections {
            chosen_server = server_id as usize;
        }
    }
    chosen_server
}

async fn check_servers_health(config: Config, healthy_servers: HealthyServers) {
    let servers = config.servers;

    // TODO: Maybe timeouts should have a TTL.
    let mut timeouts = Vec::with_capacity(servers.len());
    for _ in &servers {
        timeouts.push(AtomicU64::new(0));
    }

    loop {
        for (id, server) in servers.iter().enumerate() {
            let idx = id as u8;
            // Has reached maximum quantity fails allowed and is in
            // the list of healthy servers?
            if timeouts[id].load(Ordering::SeqCst) >= config.max_fails
                && healthy_servers.read().await.contains_key(&idx)
            {
                let idx = id as u8;
                healthy_servers.write().await.remove(&idx);
                warn!("Server {server} is unhealthy, removing it from the list of healthy servers");
                continue;
            }

            let mut stream = match timeout(
                Duration::from_millis(config.connection_timeout as u64),
                TcpStream::connect(server),
            )
            .await
            {
                Ok(result) => match result {
                    Ok(stream) => stream,
                    Err(_) => {
                        timeouts[id].fetch_add(1, Ordering::Relaxed);
                        warn!("Server {server} might be down");
                        continue;
                    }
                },
                Err(_) => {
                    timeouts[id].fetch_add(1, Ordering::Relaxed);
                    warn!("Timeout, server {server} might be down");
                    continue;
                }
            };
            // TODO: Timeout cancels the future, but write_all is
            // not cancellation safe. Will this be a problem?
            let request = format!("GET {} HTTP/1.1\r\n", config.health_check_path);
            match timeout(
                Duration::from_millis(config.write_timeout as u64),
                stream.write_all(request.as_bytes()),
            )
            .await
            {
                Ok(result) => {
                    if result.is_err() {
                        timeouts[id].fetch_add(1, Ordering::Relaxed);
                        warn!("Server {server} might be down");
                        continue;
                    }
                }
                Err(_) => {
                    timeouts[id].fetch_add(1, Ordering::Relaxed);
                    warn!("Timeout, server {server} might be down");
                    continue;
                }
            };
            let mut buf = [0_u8; 128];
            match timeout(
                Duration::from_millis(config.read_timeout as u64),
                stream.read(&mut buf),
            )
            .await
            {
                Ok(result) => {
                    if result.is_err() {
                        timeouts[id].fetch_add(1, Ordering::Relaxed);
                        warn!("Server {server} might be down");
                        continue;
                    }
                }
                Err(_) => {
                    timeouts[id].fetch_add(1, Ordering::Relaxed);
                    warn!("Timeout, server {server} might be down");
                    continue;
                }
            };
            let response = String::from_utf8_lossy(&buf);
            let ok_response = "HTTP/1.1 200";
            if response.starts_with(ok_response) {
                debug!("Everything is ok at {server}");
            } else {
                timeouts[id].fetch_add(1, Ordering::Relaxed);
                warn!("Server {server} might be down");
            }

            let idx = id as u8;
            // If the server id is not in the healthy_servers list,
            // than it has a non-zero value.
            // FIXME
            if !healthy_servers.read().await.contains_key(&idx) {
                let value = timeouts[id].fetch_sub(1, Ordering::Relaxed);
                if value < config.max_fails {
                    healthy_servers.write().await.insert(idx, server.clone());
                    info!("Everything seems to be fine with server {server} now, re-added to the list of healthy servers");
                }
            }
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn choosing_server_with_least_connections() {
        let mut servers = Vec::new();
        servers.push("127.0.0.1:8080".to_string());
        servers.push("127.0.0.1:8081".to_string());
        servers.push("127.0.0.1:8082".to_string());
        let config = Config {
            servers,
            strategy: Strategies::LeastConnections,
            max_fails: 0,
            connection_timeout: 1000,
            write_timeout: 1000,
            read_timeout: 1000,
            health_check_path: "/health".to_string(),
        };

        let healthy_servers = HealthyServers::new(RwLock::new(HashMap::new()));
        for (id, server) in config.servers.iter().enumerate() {
            healthy_servers
                .write()
                .await
                .insert(id as u8, server.clone());
        }

        let mut connections = Vec::new();
        connections.push(AtomicU64::new(15));
        connections.push(AtomicU64::new(12));
        connections.push(AtomicU64::new(6));
        let connections_counters = Arc::new(RwLock::new(connections));

        let chosen_server =
            choose_server_least_connections(healthy_servers, connections_counters).await;

        assert_eq!(chosen_server, 2);
    }
}
