use clap::Parser;
use tokio::{io::AsyncWriteExt, net::TcpListener};

#[derive(Debug, Parser)]
struct Args {
    #[clap(long, short)]
    port: u32,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let port = args.port;
    let listener = match TcpListener::bind(format!("127.0.0.1:{port}")).await {
        Ok(listener) => listener,
        Err(e) => panic!("Unable to start ekilibri on port {port}. Error = {:?}", e),
    };

    loop {
        match listener.accept().await {
            Ok((mut stream, _)) => {
                println!("Received message");
                let message = "OK\n".as_bytes();
                stream.write_all(&message).await.unwrap();
            }
            Err(_) => eprintln!("Error listening to socket"),
        }
    }
}
