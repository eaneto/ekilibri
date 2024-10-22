use std::time::Duration;

use axum::{
    body::Bytes,
    http::{header, HeaderMap, StatusCode},
    routing::{get, post},
    Router,
};
use clap::Parser;
use tokio::{net::TcpListener, time};

use tracing::info;

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

    let app = Router::new()
        .route("/health", get(health))
        .route("/sleep", get(sleep))
        .route("/echo", post(echo));

    let listener = match TcpListener::bind(format!("127.0.0.1:{port}")).await {
        Ok(listener) => listener,
        Err(e) => panic!(
            "Unable to start echo server on port {port}. Error = {:?}",
            e
        ),
    };

    axum::serve(listener, app).await.unwrap();
}

async fn health() -> StatusCode {
    info!("Received request for /health");
    StatusCode::OK
}

async fn sleep() -> StatusCode {
    info!("Received request for /sleep");
    time::sleep(Duration::from_millis(2000)).await;
    StatusCode::OK
}

async fn echo(headers: HeaderMap, body: Bytes) -> Result<(HeaderMap, String), StatusCode> {
    info!("Received request for /echo");
    let content_type = match headers.get("content-type") {
        Some(value) => value.to_str().unwrap(),
        None => "text/plain",
    };
    let mut headers = HeaderMap::new();
    headers.insert(header::CONTENT_TYPE, content_type.parse().unwrap());
    match String::from_utf8(body.to_vec()) {
        Ok(body) => Ok((headers, body)),
        Err(_) => Err(StatusCode::BAD_REQUEST),
    }
}
