use clap::Parser;
use tokio::{io::AsyncReadExt, io::AsyncWriteExt, net::TcpListener};

use tracing::info;

use uuid::Uuid;

#[derive(Debug, Parser)]
struct Args {
    #[clap(long, short)]
    port: u32,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    let port = args.port;
    let listener = match TcpListener::bind(format!("127.0.0.1:{port}")).await {
        Ok(listener) => listener,
        Err(e) => panic!(
            "Unable to start echo server on port {port}. Error = {:?}",
            e
        ),
    };

    loop {
        match listener.accept().await {
            Ok((mut stream, _)) => {
                tokio::spawn(async move {
                    let request_id = Uuid::new_v4();
                    info!("Received message, request_id={request_id}");
                    let mut buf = [0_u8; 1024];
                    let _ = stream.read(&mut buf).await.unwrap();
                    stream.write_all(&buf).await.unwrap();
                    info!("Replied message, request_id={request_id}");
                });
            }
            Err(_) => eprintln!("Error listening to socket"),
        }
    }
}
