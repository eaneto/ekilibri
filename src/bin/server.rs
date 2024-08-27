use serde::Deserialize;
use tokio::{fs, io::AsyncReadExt, io::AsyncWriteExt, net::TcpListener, net::TcpStream};
use toml;

use tracing::info;

#[derive(Debug, Deserialize)]
struct Config {
    servers: Vec<String>,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let config_content = fs::read_to_string("ekilibri.toml").await.unwrap();
    let config: Config = toml::from_str(&config_content).unwrap();
    println!("{:?}", config);

    let listener = match TcpListener::bind("127.0.0.1:8080").await {
        Ok(listener) => listener,
        Err(e) => panic!("Unable to start ekilibri on port 8080. Error = {:?}", e),
    };

    info!("Ekilibri listening at port 8080");

    loop {
        // Get connection
        match listener.accept().await {
            Ok((mut stream, _)) => {
                // Send to server
                let server_id = rand::random::<usize>() % config.servers.len();
                let mut server_stream =
                    match TcpStream::connect(config.servers.get(server_id).unwrap()).await {
                        Ok(server_stream) => server_stream,
                        Err(_) => panic!("Can't connect to server {server_id}"),
                    };

                info!("Connected to {server_id}");

                let mut buf = [0_u8; 1024];
                stream.read(&mut buf).await.unwrap();
                server_stream.write_all(&buf).await.unwrap();

                // Reply with same response
                let mut buf = [0_u8; 1024];
                server_stream.read(&mut buf).await.unwrap();
                stream.write_all(&buf).await.unwrap();
            }
            Err(_) => eprintln!("Error listening to socket"),
        }
    }
}
