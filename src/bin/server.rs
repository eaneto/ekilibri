use ekilibri::http::{parse_request, ParseErrorKind, CRLF};
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
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::RwLock,
    time::{timeout, Instant},
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
    /// Load balancing strategy.
    strategy: Strategies,
    /// Maximum number of failed requests(non 200 responses and
    /// timeouts) allowed before a server is taken from the healthy
    /// servers list. A server that is not on this list won't receive
    /// any requests. The health check process runs once every 500ms.
    max_fails: u64,
    /// The time window to consider the [Config::max_fails] and remove
    /// a server from the healthy servers list, the time windows is
    /// relevant so that "old" requests don't interfere in the
    /// decision (in seconds).
    fail_window: u16,
    /// Timeout to establish a connection to one of the servers (in
    /// milliseconds).
    connection_timeout: u32,
    /// Timeout writing the data to the server (in milliseconds).
    write_timeout: u32,
    /// Timeout reading the data to the server (in milliseconds).
    read_timeout: u32,
    /// The path to check the server's health. Ex.: "/health".
    health_check_path: String,
}

#[derive(Parser, Debug)]
struct Args {
    /// Path to configuration file
    #[arg(short, long, default_value = "ekilibri.toml")]
    file: String,
}

/// Map of servers that ekilibri will use to consider to balance requests, each
/// mapped by id.
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

    let config = Arc::new(config);

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
    let config_clone = Arc::clone(&config);
    tokio::spawn(async move {
        check_servers_health(config_clone, healthy_servers_clone).await;
    });

    accept_and_handle_connections(listener, connections_counters, config, healthy_servers).await;
}

async fn accept_and_handle_connections(
    listener: TcpListener,
    connections_counters: Arc<RwLock<Vec<AtomicU64>>>,
    config: Arc<Config>,
    healthy_servers: HealthyServers,
) {
    loop {
        match listener.accept().await {
            Ok((mut ekilibri_stream, _)) => {
                let healthy_servers = Arc::clone(&healthy_servers);
                let connections_counters = Arc::clone(&connections_counters);
                let config = Arc::clone(&config);
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
    config: Arc<Config>,
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

    match TcpStream::connect(
        config
            .servers
            .get(server_id)
            .expect("The strategy functions should return a possible server_id at this point"),
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

// TODO: Consider configuration timeouts.
async fn process_request(
    request_id: Uuid,
    ekilibri_stream: &mut TcpStream,
    server_stream: &mut TcpStream,
) {
    // Read data from client and send it to the server
    let request = match parse_request(&request_id, ekilibri_stream).await {
        Ok((_, raw_request)) => raw_request,
        Err(e) => {
            let status = match e {
                ParseErrorKind::MissingContentLength => "411",
                _ => "400",
            };
            let response = format!("HTTP/1.1 {status}{CRLF}{CRLF}");
            if let Err(e) = ekilibri_stream.write_all(response.as_bytes()).await {
                debug!("Unable to send response to the client {e}");
            }
            return;
        }
    };

    match server_stream.write_all(&request).await {
        Ok(()) => {
            trace!("Successfully sent client data to server, request_id={request_id}")
        }
        Err(_) => {
            trace!("Unable to send client data to server, request_id={request_id}");
            return;
        }
    }

    // Reply client with same response from server
    let mut cursor = 0;
    let mut buf = vec![0_u8; 4096];
    loop {
        if buf.len() == cursor {
            buf.resize(cursor * 2, 0);
        }

        let bytes_read = match server_stream.read(&mut buf[cursor..]).await {
            Ok(size) => {
                trace!(
                "Successfully read data from server stream, size={size}, request_id={request_id}"
                );
                size
            }
            Err(_) => {
                trace!("Unable to read data from server stream, request_id={request_id}");
                0
            }
        };

        cursor += bytes_read;

        if bytes_read == 0 || cursor < buf.len() {
            break;
        }
    }

    match ekilibri_stream.write_all(&buf[..cursor]).await {
        Ok(()) => {
            trace!("Successfully sent server data to client, request_id={request_id}")
        }
        Err(_) => {
            trace!("Unable to send server data to client, request_id={request_id}");
        }
    }
}

// FIXME: Returns error if all servers are down
async fn choose_server_round_robin(servers: HealthyServers) -> usize {
    let healthy_servers = servers.read().await;
    let possible_servers: Vec<u8> = healthy_servers.keys().copied().collect();
    let idx = rand::random::<usize>() % possible_servers.len();
    possible_servers[idx] as usize
}

// FIXME: Returns error if all servers are down
async fn choose_server_least_connections(
    healthy_servers: HealthyServers,
    connections_counters: Arc<RwLock<Vec<AtomicU64>>>,
) -> usize {
    let counters = connections_counters.read().await;
    let healthy_servers = healthy_servers.read().await;
    let possible_servers: Vec<u8> = healthy_servers.keys().copied().collect();
    let mut chosen_server = *possible_servers
        .first()
        .expect("FIXME At least one server should be up (NOT TRUE)")
        as usize;
    for server_id in possible_servers {
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

/// Checks if all the servers in the configuration([Config::servers]) are healthy.
/// If a server times out or answers the health endpoint([Config::health_check_path])
/// with something different than a 200, than the server is considered unhealthy
/// and a timestamp is saved recording when the error happened. This data is used
/// when considering to drop a server from the healthy servers list([HealthyServers]).
/// This process considers the time window defined in the configuration([Config::fail_window])
/// counting all errors recorded inside this time window. When the number of errors
/// in this window is lesser than the configured Config::max_fails, the server is
/// added to the list of healthy servers again.
/// A background process runs cleaning the list of errors, retaining only the errors
/// that happened inside the time window, this job runs every 10 seconds.
///
/// # Problems
///
/// The main problem of this solution is the need to run a "garbage collection" job
/// to remove the invalid entries from the errors list. If the user configures a big
/// time window, the list may grow too much if there are multiple errors. This job
/// also locks the error list, so the health check process can't insert an error
/// while the GC job is running.
async fn check_servers_health(config: Arc<Config>, healthy_servers: HealthyServers) {
    let timeouts = Arc::new(RwLock::new(Vec::with_capacity(config.servers.len())));
    for _ in &config.servers {
        timeouts
            .write()
            .await
            .push(RwLock::new(Vec::<Instant>::new()));
    }

    // background job to remove dead timeouts.
    let timeout_list_clone = Arc::clone(&timeouts);
    let server_count = config.servers.len();
    let config_clone = Arc::clone(&config);
    tokio::spawn(async move {
        let config = config_clone;
        loop {
            debug!("Looking for timeouts to clean");
            for id in 0..server_count {
                timeout_list_clone
                    .read()
                    .await
                    .get(id)
                    .expect("The timeout list should be initialized with each server id")
                    .write()
                    .await
                    .retain(|timeout| {
                        // If the timeout is inside the window.
                        timeout.elapsed().as_secs() < config.fail_window as u64
                    });
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
        }
    });

    loop {
        for (id, server) in config.servers.iter().enumerate() {
            let idx = id as u8;
            let timeout_count = count_server_timeouts(&timeouts, id, config.fail_window).await;

            // Has reached maximum quantity fails allowed and is in
            // the list of healthy servers?
            if timeout_count >= config.max_fails && healthy_servers.read().await.contains_key(&idx)
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
                        timeouts.read().await[id].write().await.push(Instant::now());
                        warn!("Server {server} might be down");
                        continue;
                    }
                },
                Err(_) => {
                    timeouts.read().await[id].write().await.push(Instant::now());
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
                        timeouts.read().await[id].write().await.push(Instant::now());
                        warn!("Server {server} might be down");
                        continue;
                    }
                }
                Err(_) => {
                    timeouts.read().await[id].write().await.push(Instant::now());
                    warn!("Timeout, server {server} might be down");
                    continue;
                }
            };
            let mut buf = [0_u8; 12];
            match timeout(
                Duration::from_millis(config.read_timeout as u64),
                stream.read(&mut buf),
            )
            .await
            {
                Ok(result) => {
                    if result.is_err() {
                        timeouts.read().await[id].write().await.push(Instant::now());
                        warn!("Server {server} might be down");
                        continue;
                    }
                }
                Err(_) => {
                    timeouts.read().await[id].write().await.push(Instant::now());
                    warn!("Timeout, server {server} might be down");
                    continue;
                }
            };
            let response = String::from_utf8_lossy(&buf);
            let ok_response = "HTTP/1.1 200";
            if response.starts_with(ok_response) {
                debug!("Everything is ok at {server}");
            } else {
                timeouts.read().await[id].write().await.push(Instant::now());
                warn!("Server {server} might be down");
            }

            let idx = id as u8;
            if !healthy_servers.read().await.contains_key(&idx) {
                let timeout_count = count_server_timeouts(&timeouts, id, config.fail_window).await;
                if timeout_count < config.max_fails {
                    healthy_servers.write().await.insert(idx, server.clone());
                    info!("Everything seems to be fine with server {server} now, re-added to the list of healthy servers");
                }
            }
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    async fn count_server_timeouts(
        timeouts: &Arc<RwLock<Vec<RwLock<Vec<Instant>>>>>,
        server_id: usize,
        fail_window: u16,
    ) -> u64 {
        let timeouts = timeouts.read().await;
        let server_timeouts = timeouts
            .get(server_id)
            .expect("The timeout list should be initialized with each server id")
            .read()
            .await;

        let mut counter = 0;
        for timeout in server_timeouts.iter() {
            if timeout.elapsed().as_secs() < fail_window as u64 {
                counter += 1;
            }
        }
        counter
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
            fail_window: 1,
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
