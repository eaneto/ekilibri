use serde::Deserialize;
use std::{
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
};

use tracing::{debug, info, trace, warn};
use uuid::Uuid;

use clap::Parser;

#[derive(Debug, Deserialize, Clone)]
enum Strategies {
    RoundRobin,
    LeastConnections,
}

#[derive(Debug, Deserialize, Clone)]
struct Config {
    servers: Vec<String>,
    strategy: Strategies,
}

#[derive(Parser, Debug)]
struct Args {
    /// Path to configuration file
    #[arg(short, long, default_value = "ekilibri.toml")]
    file: String,
}

type Servers = Arc<RwLock<Vec<String>>>;

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

    let servers = Servers::new(RwLock::new(config.servers.clone()));

    let servers_clone = servers.clone();
    tokio::spawn(async move {
        let servers = servers_clone.read().await;
        info!("Initiating health checks {:?}", servers);
        loop {
            for server in servers.iter() {
                // TODO: call health
            }

            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    });

    accept_and_handle_connections(listener, connections_counters, config, servers).await;
}

async fn accept_and_handle_connections(
    listener: TcpListener,
    connections_counters: Arc<RwLock<Vec<AtomicU64>>>,
    config: Config,
    servers: Servers,
) {
    match listener.accept().await {
        Ok((mut ekilibri_stream, _)) => {
            let connections_counters = Arc::clone(&connections_counters);
            let config = config.clone();
            tokio::spawn(async move {
                handle_connection(
                    config,
                    servers.clone(),
                    connections_counters,
                    &mut ekilibri_stream,
                )
                .await;
            });
        }
        Err(e) => warn!("Error listening to socket. {e}"),
    }
}

async fn handle_connection(
    config: Config,
    servers: Servers,
    connections_counters: Arc<RwLock<Vec<AtomicU64>>>,
    ekilibri_stream: &mut TcpStream,
) {
    let request_id = Uuid::new_v4();
    let server_id = match config.strategy {
        Strategies::RoundRobin => choose_server_round_robin(servers).await,
        Strategies::LeastConnections => {
            choose_server_least_connections(servers, Arc::clone(&connections_counters)).await
        }
    };

    let counters = connections_counters.read().await;
    let counter = counters
        .get(server_id)
        .expect("The counters should be initialized with every possible server_id at this point");
    counter.fetch_add(1, Ordering::Relaxed);

    match TcpStream::connect(
        config.servers.get(server_id).expect(
            "The counters should be initialized with every possible server_id at this point",
        ),
    )
    .await
    {
        Ok(mut server_stream) => {
            info!("Connected to server, server_id={server_id}, request_id={request_id}");
            process_request(request_id, ekilibri_stream, &mut server_stream).await;
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

async fn choose_server_round_robin(servers: Servers) -> usize {
    rand::random::<usize>() % servers.read().await.len()
}

async fn choose_server_least_connections(
    servers: Servers,
    connections_counters: Arc<RwLock<Vec<AtomicU64>>>,
) -> usize {
    let mut chosen_server = 0;
    let counters = connections_counters.read().await;
    for server_id in 1..servers.read().await.len() {
        let chosen_server_connections = counters
            .get(chosen_server)
            .expect(
                "The counters should be initialized with every possible server_id at this point",
            )
            .load(Ordering::Relaxed);
        let current_server_connections = counters
            .get(server_id)
            .expect(
                "The counters should be initialized with every possible server_id at this point",
            )
            .load(Ordering::Relaxed);
        if chosen_server_connections > current_server_connections {
            chosen_server = server_id;
        }
    }
    chosen_server
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
        };

        let mut connections = Vec::new();
        connections.push(AtomicU64::new(15));
        connections.push(AtomicU64::new(12));
        connections.push(AtomicU64::new(6));
        let connections_counters = Arc::new(RwLock::new(connections));

        let chosen_server = choose_server_least_connections(&config, connections_counters).await;

        assert_eq!(chosen_server, 2);
    }
}
