use std::time::Duration;

use clap::Parser;
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
    time,
};

use tracing::{debug, info, warn};

use uuid::Uuid;

use ekilibri::http::{parse_request, Method, ParsingError, CRLF};

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
        accept_and_handle_connection(&listener).await;
    }
}

async fn accept_and_handle_connection(listener: &TcpListener) {
    match listener.accept().await {
        Ok((stream, _)) => {
            tokio::spawn(async move {
                process_request(stream).await;
            });
        }
        Err(_) => eprintln!("Error listening to socket"),
    }
}

async fn process_request(mut stream: TcpStream) {
    let request_id = Uuid::new_v4();
    let request = match parse_request(&request_id, &mut stream).await {
        Ok((request, _)) => request,
        Err(e) => {
            let status = match e {
                ParsingError::MissingContentLength => "411",
                ParsingError::HTTPVersionNotSupported => "505",
                _ => "400",
            };
            let response = format!("HTTP/1.1 {status}{CRLF}{CRLF}");
            if let Err(e) = stream.write_all(response.as_bytes()).await {
                debug!("Unable to send response to the client {e}");
            }
            return;
        }
    };

    let response = match request.method {
        Method::Get => match request.path.as_str() {
            "/sleep" => {
                info!("Received request for /sleep, request_id={request_id}");
                time::sleep(Duration::from_millis(2000)).await;
                format!("HTTP/1.1 200{CRLF}{CRLF}")
            }
            "/health" => {
                info!("Received request for /health, request_id={request_id}");
                format!("HTTP/1.1 200{CRLF}{CRLF}")
            }
            _ => {
                info!("Received request for unmapped path, request_id={request_id}");
                format!("HTTP/1.1 404{CRLF}{CRLF}")
            }
        },
        Method::Post => match request.path.as_str() {
            "/echo" => {
                info!("Received request for /echo, request_id={request_id}");
                let length = match request.headers.get("Content-Length") {
                    Some(value) => value,
                    None => "0",
                };
                let content_length = format!("Content-Length: {length}");
                let content_type = match request.headers.get("Content-Type") {
                    Some(value) => value,
                    None => "text/plain",
                };
                let content_type = format!("Content-Type: {content_type}");
                let body = request.body.unwrap_or_default();
                format!("HTTP/1.1 200{CRLF}{content_length}{CRLF}{content_type}{CRLF}{CRLF}{body}")
            }
            _ => {
                info!("Received request for unmapped path, request_id={request_id}");
                format!("HTTP/1.1 404{CRLF}{CRLF}")
            }
        },
        Method::Unknown => {
            warn!("Received request for unmapped path, request_id={request_id}");
            format!("HTTP/1.1 404{CRLF}{CRLF}")
        }
    };

    if let Err(e) = stream.write_all(response.as_bytes()).await {
        debug!("Unable to send response to the client {e}");
    }
}
