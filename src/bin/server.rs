use ekilibri::http::{self, parse_request, parse_response, ParsingError, CRLF, HTTP_VERSION};
use serde::Deserialize;
use std::{
    collections::HashMap,
    io,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use thiserror::Error;
use tokio::{
    fs,
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
    sync::{mpsc, RwLock},
    time::{timeout, Instant},
};

use tracing::{debug, error, info, trace, warn};
use uuid::Uuid;

use ekilibri::pool::{Connection, ConnectionPool};

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
    connection_timeout: u64,
    /// Timeout writing the data to the server (in milliseconds).
    write_timeout: u32,
    /// Timeout reading the data to the server (in milliseconds).
    read_timeout: u64,
    /// The path to check the server's health. Ex.: "/health".
    health_check_path: String,
    /// The maximum number of connections to have in the connection pool.
    pool_size: usize,
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

    let connection_pools = Arc::new(RwLock::new(Vec::with_capacity(config.servers.capacity())));
    for (id, server) in config.servers.iter().enumerate() {
        let (sender, receiver) = mpsc::channel::<Connection>(config.pool_size);
        connection_pools.write().await.push(ConnectionPool::new(
            id,
            config.pool_size,
            config.connection_timeout,
            server,
            sender,
            receiver,
        ));
        connection_pools.read().await[id]
            .establish_connections()
            .await;
    }

    let healthy_servers_clone = Arc::clone(&healthy_servers);
    let config_clone = Arc::clone(&config);
    let pools_clone = Arc::clone(&connection_pools);
    tokio::spawn(async move {
        check_servers_health(config_clone, healthy_servers_clone, pools_clone).await;
    });

    accept_and_handle_connections(
        listener,
        connections_counters,
        config,
        healthy_servers,
        connection_pools,
    )
    .await;
}

async fn accept_and_handle_connections(
    listener: TcpListener,
    connections_counters: Arc<RwLock<Vec<AtomicU64>>>,
    config: Arc<Config>,
    healthy_servers: HealthyServers,
    connection_pools: Arc<RwLock<Vec<ConnectionPool>>>,
) {
    loop {
        match listener.accept().await {
            Ok((mut ekilibri_stream, _)) => {
                let healthy_servers = Arc::clone(&healthy_servers);
                let connections_counters = Arc::clone(&connections_counters);
                let pools_clone = Arc::clone(&connection_pools);
                let config = Arc::clone(&config);
                tokio::spawn(async move {
                    handle_connection(
                        config,
                        healthy_servers,
                        connections_counters,
                        &mut ekilibri_stream,
                        pools_clone,
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
    pools: Arc<RwLock<Vec<ConnectionPool>>>,
) {
    let request_id = Uuid::new_v4();
    let request = match parse_request(&request_id, ekilibri_stream).await {
        Ok(request) => request,
        Err(e) => {
            warn!(
                "There was an error while parsing the request, request_id={request_id}, error={e}"
            );
            let (status, reason) = match e {
                ParsingError::MissingContentLength => (411, "Length Required"),
                ParsingError::HTTPVersionNotSupported => (505, "HTTP Version not supported"),
                _ => (400, "Bad Request"),
            };
            let response = format!("{HTTP_VERSION} {status} {reason}{CRLF}{CRLF}");
            if let Err(e) = ekilibri_stream.write_all(response.as_bytes()).await {
                debug!("Unable to send response to the client {e}");
            }
            return;
        }
    };

    let result = match config.strategy {
        Strategies::RoundRobin => choose_server_round_robin(healthy_servers).await,
        Strategies::LeastConnections => {
            choose_server_least_connections(healthy_servers, Arc::clone(&connections_counters))
                .await
        }
    };

    let server_id = match result {
        Ok(id) => id,
        Err(e) => {
            let response = match e {
                RequestError::NoHealthyServer => http::Response::new(502),
            };
            warn!(
                "Error choosing a possible server to route request, request_id={request_id}, error={e}"
            );
            if let Err(e) = ekilibri_stream.write_all(&response.as_bytes()).await {
                error!("Error sending response to client, request_id={request_id}, {e}")
            }
            return;
        }
    };

    debug!("Server {server_id} chosen for routing");

    let counters = connections_counters.read().await;
    let counter = counters
        .get(server_id)
        .expect("The counters should be initialized with every possible server_id at this point");
    counter.fetch_add(1, Ordering::Relaxed);

    let pools_lock = pools.read().await;
    let pool = pools_lock
        .get(server_id)
        .expect("There should be a pool of connections initialized for each server");

    let response = match pool.get_connection().await {
        Ok(mut connection) => {
            info!("Connected to server, server_id={server_id}, request_id={request_id}");

            match process_request(request_id, request, &mut connection.stream, config).await {
                Ok(response) => {
                    let connection_header = response.headers.get("connection");
                    let connection_header = match connection_header {
                        Some(connection_header) => match connection_header.as_str() {
                            "keep-alive" => http::ConnectionHeader::KeepAlive,
                            "close" => http::ConnectionHeader::Close,
                            _ => http::ConnectionHeader::Close,
                        },
                        None => http::ConnectionHeader::KeepAlive,
                    };

                    match connection_header {
                        http::ConnectionHeader::KeepAlive => {
                            pool.return_connection(connection).await
                        }
                        http::ConnectionHeader::Close => {
                            let pools_clone = Arc::clone(&pools);
                            reconnect_in_background(connection, server_id, pools_clone).await;
                        }
                    }

                    response
                }
                Err(e) => {
                    warn!("Error processing the request, server_id={server_id}, request_id={request_id}. {e}");
                    match e {
                        // If there was a timeout error the response will still be sent to the connection in the pool,
                        // but the client won't ever receive it, if nothing is done the next request will have to
                        // read the response from the previous connection before sending the request. To make sure this
                        // is not needed, given that the next request may arrive before the response of the initial
                        // request, the connection is closed and a new one is opened, this is done in background so
                        // that the client won't have to wait for the new connection to be established.
                        ProcessingError::WriteTimeout => {
                            let pools_clone = Arc::clone(&pools);
                            reconnect_in_background(connection, server_id, pools_clone).await;
                            http::Response::new(504)
                        }
                        ProcessingError::UnableToSendRequest(_) => {
                            pool.return_connection(connection).await;
                            http::Response::new(502)
                        }
                        ProcessingError::ParsingError(why) => match why {
                            ParsingError::UnableToRead => http::Response::new(502),
                            ParsingError::ReadTimeout => {
                                let pools_clone = Arc::clone(&pools);
                                reconnect_in_background(connection, server_id, pools_clone).await;
                                http::Response::new(504)
                            }
                            _ => {
                                warn!("Unwrapped error, server_id={server_id}, request_id={request_id}. {why}");
                                http::Response::new(400)
                            }
                        },
                    }
                }
            }
        }
        Err(e) => match e.kind() {
            io::ErrorKind::TimedOut => {
                warn!("Can't connect to server, connection timed out, server_id={server_id}, request_id={request_id}.");
                http::Response::new(504)
            }
            _ => {
                warn!(
                    "Can't connect to server, server_id={server_id}, request_id={request_id}. {e}"
                );
                http::Response::new(502)
            }
        },
    };

    if let Err(e) = ekilibri_stream.write_all(&response.as_bytes()).await {
        error!("Error sending response to client, request_id={request_id}, {e}")
    }

    counter.fetch_sub(1, Ordering::Relaxed);
}

async fn reconnect_in_background(
    connection: Connection,
    server_id: usize,
    pools: Arc<RwLock<Vec<ConnectionPool>>>,
) {
    tokio::spawn(async move {
        drop(connection);
        let pools_lock = pools.read().await;
        let pool = pools_lock
            .get(server_id)
            .expect("There should be a pool of connections initialized for each server");
        match pool.create_connection(pool.is_reconnecting()).await {
            Ok(connection) => pool.send_connection(connection).await,
            Err(e) => {
                warn!(
                    "Failed to create a new connection in the pool during reconnection. server_id={server_id}, error={e}"
                );
            }
        }
    });
}

#[derive(Error, Debug)]
enum ProcessingError {
    #[error("Request timed out while sending the request to the server")]
    WriteTimeout,
    #[error("Unable to send the request to the server")]
    UnableToSendRequest(#[from] io::Error),
    #[error("Parsing error: {0}")]
    ParsingError(#[from] http::ParsingError),
}

/// Takes the [request] data and send it to the chosen server.
async fn process_request(
    request_id: Uuid,
    request: http::Request,
    server_stream: &mut TcpStream,
    config: Arc<Config>,
) -> Result<http::Response, ProcessingError> {
    match timeout(
        Duration::from_millis(config.write_timeout as u64),
        server_stream.write_all(&request.as_bytes()),
    )
    .await
    {
        Ok(result) => match result {
            Ok(()) => trace!("Successfully sent request o server, request_id={request_id}"),
            Err(e) => {
                error!("Unable to send request to server, request_id={request_id}, {e}");
                return Err(ProcessingError::UnableToSendRequest(e));
            }
        },
        Err(e) => {
            error!("Timeout sending request to server, request_id={request_id}, {e}");
            return Err(ProcessingError::WriteTimeout);
        }
    }

    let response = parse_response(server_stream, config.read_timeout).await?;

    Ok(response)
}

#[derive(Error, Debug)]
enum RequestError {
    #[error("None of the servers configured are healthy at this moment")]
    NoHealthyServer,
}

async fn choose_server_round_robin(servers: HealthyServers) -> Result<usize, RequestError> {
    let healthy_servers = servers.read().await;
    if healthy_servers.is_empty() {
        return Err(RequestError::NoHealthyServer);
    }
    let possible_servers: Vec<u8> = healthy_servers.keys().copied().collect();
    let idx = rand::random::<usize>() % possible_servers.len();
    Ok(possible_servers[idx] as usize)
}

async fn choose_server_least_connections(
    healthy_servers: HealthyServers,
    connections_counters: Arc<RwLock<Vec<AtomicU64>>>,
) -> Result<usize, RequestError> {
    let healthy_servers = healthy_servers.read().await;
    if healthy_servers.is_empty() {
        return Err(RequestError::NoHealthyServer);
    }
    let counters = connections_counters.read().await;
    let possible_servers: Vec<u8> = healthy_servers.keys().copied().collect();
    let mut chosen_server = *possible_servers
        .first()
        .expect("There should be at least one server in the list at this point")
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
    Ok(chosen_server)
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
async fn check_servers_health(
    config: Arc<Config>,
    healthy_servers: HealthyServers,
    pools: Arc<RwLock<Vec<ConnectionPool>>>,
) {
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

    let mut request_template = format!("GET {} {HTTP_VERSION}{CRLF}", config.health_check_path);
    request_template.push_str(&format!("Connection: keep-alive{CRLF}"));
    request_template.push_str(&format!("Host: [server]{CRLF}"));
    request_template.push_str(CRLF);

    loop {
        for (id, server) in config.servers.iter().enumerate() {
            let pool = &pools.read().await[id];
            let idx = id as u8;
            let timeout_count = count_server_timeouts(&timeouts, id, config.fail_window).await;

            // Has reached maximum quantity fails allowed and is in
            // the list of healthy servers?
            if timeout_count >= config.max_fails && healthy_servers.read().await.contains_key(&idx)
            {
                let idx = id as u8;
                healthy_servers.write().await.remove(&idx);
                // If the server is unhealthy, all connections should be dropped and the pool should start creating new
                // connections for each request.
                pool.set_reconnect(true);
                pool.drop_connections().await;
                warn!("Server {server} is unhealthy, removing it from the list of healthy servers");
                continue;
            }

            let mut stream = match timeout(
                Duration::from_millis(config.connection_timeout),
                TcpStream::connect(&server),
            )
            .await
            {
                Ok(result) => match result {
                    Ok(stream) => stream,
                    Err(e) => {
                        warn!("Error while connecting to the server, {id} might be down, {e}",);
                        timeouts.read().await[id].write().await.push(Instant::now());
                        continue;
                    }
                },
                Err(_) => {
                    warn!("Connection timeout, server {id} might be down",);
                    timeouts.read().await[id].write().await.push(Instant::now());
                    continue;
                }
            };

            let request = request_template.replace("[server]", server);
            // TODO: Timeout cancels the future, but write_all is
            // not cancellation safe. Will this be a problem?
            match timeout(
                Duration::from_millis(config.write_timeout as u64),
                stream.write_all(request.as_bytes()),
            )
            .await
            {
                Ok(result) => {
                    if let Err(e) = result {
                        warn!("Error sending request to server, {server} might be down, {e}");
                        timeouts.read().await[id].write().await.push(Instant::now());
                        continue;
                    }
                }
                Err(_) => {
                    timeouts.read().await[id].write().await.push(Instant::now());
                    warn!("Write timeout, server {server} might be down");
                    continue;
                }
            };
            let response = match parse_response(&mut stream, config.read_timeout).await {
                Ok(response) => response,
                Err(e) => {
                    timeouts.read().await[id].write().await.push(Instant::now());
                    warn!("Error reading response from server, {server} might be down, {e}");
                    continue;
                }
            };

            if response.status == 200 {
                trace!("Everything is ok at {server}");
            } else {
                timeouts.read().await[id].write().await.push(Instant::now());
                warn!("Server {server} is not healthy");
                debug!("server_response={:?}", response.body);
            }

            let idx = id as u8;
            if !healthy_servers.read().await.contains_key(&idx) {
                let timeout_count = count_server_timeouts(&timeouts, id, config.fail_window).await;
                if timeout_count < config.max_fails {
                    healthy_servers.write().await.insert(idx, server.clone());
                    // We assume that if there are any connections in the channel they are not valid
                    // connections and should be dropped.
                    pool.drop_connections().await;
                    pool.establish_connections().await;
                    pool.set_reconnect(false);
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
    async fn choosing_server_round_robin_without_healthy_servers() {
        let healthy_servers = HealthyServers::new(RwLock::new(HashMap::new()));

        let chosen_server = choose_server_round_robin(healthy_servers).await;

        assert_eq!(chosen_server.is_err(), true);
    }

    #[tokio::test]
    async fn choosing_server_with_least_connections() {
        let mut servers = Vec::new();
        servers.push("127.0.0.1:8080".to_string());
        servers.push("127.0.0.1:8081".to_string());
        servers.push("127.0.0.1:8082".to_string());

        let healthy_servers = HealthyServers::new(RwLock::new(HashMap::new()));
        {
            let mut servers = healthy_servers.write().await;
            servers.insert(0, "127.0.0.1:8080".to_string());
            servers.insert(1, "127.0.0.1:8081".to_string());
            servers.insert(2, "127.0.0.1:8082".to_string());
        }

        let mut connections = Vec::new();
        connections.push(AtomicU64::new(15));
        connections.push(AtomicU64::new(12));
        connections.push(AtomicU64::new(6));
        let connections_counters = Arc::new(RwLock::new(connections));

        let chosen_server =
            choose_server_least_connections(healthy_servers, connections_counters).await;

        assert_eq!(chosen_server.is_ok(), true);
        assert_eq!(chosen_server.unwrap(), 2);
    }

    #[tokio::test]
    async fn choosing_server_with_least_connections_without_healthy_servers() {
        let healthy_servers = HealthyServers::new(RwLock::new(HashMap::new()));
        let mut connections = Vec::new();
        connections.push(AtomicU64::new(0));
        connections.push(AtomicU64::new(0));
        connections.push(AtomicU64::new(0));
        let connections_counters = Arc::new(RwLock::new(connections));

        let chosen_server =
            choose_server_least_connections(healthy_servers, connections_counters).await;

        assert_eq!(chosen_server.is_err(), true);
    }
}
