use std::time::Duration;

use clap::Parser;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
    time,
};

use tracing::{debug, info};

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

                    let buf_reader = BufReader::new(&mut stream);
                    let mut request_lines = buf_reader.lines();
                    if let Ok(line) = request_lines.next_line().await {
                        let line = line.expect("Line should be some at this point");
                        debug!("line={}", line);
                        match &line[..] {
                            "POST /echo HTTP/1.1" => {
                                // TODO: Format corret response
                                stream
                                    .write_all("HTTP/1.1 200 OK\r\n\r\n".as_bytes())
                                    .await
                                    .unwrap();
                                info!("Replied message, request_id={request_id}");
                            }
                            "GET /sleep HTTP/1.1" => {
                                time::sleep(Duration::from_millis(2000)).await;
                                let response = "HTTP/1.1 200 OK\r\n\r\n";
                                stream.write_all(response.as_bytes()).await.unwrap();
                                info!("Replied message, request_id={request_id}");
                            }
                            _ => debug!("Nothing"),
                        }
                    }
                });
            }
            Err(_) => eprintln!("Error listening to socket"),
        }
    }
}
