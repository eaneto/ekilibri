use serde::Deserialize;
use tokio::{fs, io::AsyncReadExt, io::AsyncWriteExt, net::TcpListener, net::TcpStream};
use toml;

use tracing::{debug, info, trace, warn};
use uuid::Uuid;

#[derive(Debug, Deserialize)]
struct Config {
    servers: Vec<String>,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    // TODO: Make file path configurable
    let configuration_file = "ekilibri.toml";
    let config_content = match fs::read_to_string(&configuration_file).await {
        Ok(content) => content,
        Err(_) => panic!("Unable to read {configuration_file} configuration file"),
    };
    let config: Config = match toml::from_str(&config_content) {
        Ok(config) => config,
        Err(_) => panic!("Unable to parse configuration file {configuration_file}"),
    };
    debug!("Starting ekilibri with {:?}", config);

    let listener = match TcpListener::bind("127.0.0.1:8080").await {
        Ok(listener) => listener,
        Err(e) => panic!("Unable to start ekilibri on port 8080. Error = {:?}", e),
    };

    info!("Ekilibri listening at port 8080");

    loop {
        match listener.accept().await {
            Ok((mut ekilibri_stream, _)) => {
                let request_id = Uuid::new_v4();
                let server_id = rand::random::<usize>() % config.servers.len();
                match TcpStream::connect(config.servers.get(server_id).unwrap()).await {
                    Ok(mut server_stream) => {
                        info!("Connected to server, server_id={server_id}, request_id={request_id}");

                        process_request(request_id, &mut ekilibri_stream, &mut server_stream).await;
                    }
                    Err(e) => warn!("Can't connect to server, server_id={server_id}, request_id={request_id}, {e}"),
                }
            }
            Err(e) => warn!("Error listening to socket, {e}"),
        }
    }
}

async fn process_request(
    request_id: Uuid,
    ekilibri_stream: &mut TcpStream,
    server_stream: &mut TcpStream,
) {
    // Read data from client and send it to the server
    let mut buf = [0_u8; 1024];
    match ekilibri_stream.read(&mut buf).await {
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
    let mut buf = [0_u8; 1024];
    match server_stream.read(&mut buf).await {
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
            return;
        }
    }
}
