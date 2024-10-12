use std::{collections::HashMap, time::Duration};

use clap::Parser;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time,
};

use tracing::{debug, info};

use uuid::Uuid;

#[derive(Debug, Parser)]
struct Args {
    #[clap(long, short)]
    port: u32,
}

const CRLF: &str = "\r\n";
const CR: u8 = 13;
const LF: u8 = 10;

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

                    let request = parse_request(&request_id, &mut stream).await;

                    match request.method {
                        Method::Get => match request.path.as_str() {
                            "/sleep" => {
                                debug!("Received request for /sleep");
                                time::sleep(Duration::from_millis(2000)).await;
                                let response = format!("HTTP/1.1 200{CRLF}{CRLF}");
                                stream.write_all(response.as_bytes()).await.unwrap();
                            }
                            "/health" => {
                                debug!("Received request for /health");
                                let response = format!("HTTP/1.1 200{CRLF}{CRLF}");
                                stream.write_all(response.as_bytes()).await.unwrap();
                            }
                            _ => {
                                debug!("Received request for unmapped path");
                                let response = format!("HTTP/1.1 404{CRLF}{CRLF}");
                                stream.write_all(response.as_bytes()).await.unwrap();
                            }
                        },
                        Method::Post => match request.path.as_str() {
                            "/echo" => {
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
                                let response = format!("HTTP/1.1 200{CRLF}{content_length}{CRLF}{content_type}{CRLF}{CRLF}{body}");
                                stream.write_all(response.as_bytes()).await.unwrap();
                            }
                            _ => {
                                debug!("Received request for unmapped path");
                                let response = format!("HTTP/1.1 404{CRLF}{CRLF}");
                                stream.write_all(response.as_bytes()).await.unwrap();
                            }
                        },
                        Method::Unknown => {
                            debug!("Received request for unknown method");
                            let response = format!("HTTP/1.1 404{CRLF}{CRLF}");
                            stream.write_all(response.as_bytes()).await.unwrap();
                        }
                    }
                });
            }
            Err(_) => eprintln!("Error listening to socket"),
        }
    }
}

enum Method {
    Get,
    Post,
    Unknown,
}

struct Request {
    method: Method,
    path: String,
    headers: HashMap<String, String>,
    body: Option<String>,
}

async fn parse_request(request_id: &uuid::Uuid, stream: &mut TcpStream) -> Request {
    let mut method = String::new();
    let mut path = String::new();
    let mut protocol = String::new();
    let mut headers = HashMap::<String, String>::new();
    let mut body: Option<String> = None;

    let mut buf = vec![0u8; 4096];
    let bytes_read = match stream.read(&mut buf).await {
        Ok(size) => size,
        Err(e) => {
            debug!("Error reading TCP stream to parse command, request_id={request_id}, error={e}");
            0
        }
    };

    if bytes_read == 0 {
        panic!("HELP");
    }

    debug!("{}", String::from_utf8_lossy(&buf));

    // Parse request line
    let mut initial_position = 0;
    for i in 0..(buf.len() - 1) {
        if buf[i] == CR && buf[i + 1] == LF {
            let request_line = String::from_utf8_lossy(&buf[initial_position..i]);
            let mut request_line = request_line.split_whitespace();
            method = request_line
                .next()
                .expect("Where's the method?")
                .to_string();
            path = request_line.next().expect("Where's the path?").to_string();
            protocol = request_line
                .next()
                .expect("Where's the protocol?")
                .to_string();
            initial_position = i + 2;
            break;
        }
    }

    debug!("Initial position: {initial_position}");
    debug!("Method: {method}, Path: {path}, Protocol: {protocol}");

    // Parse headers
    let mut header_position = initial_position;
    for i in initial_position..(buf.len() - 3) {
        if buf[i] == CR && buf[i + 1] == LF {
            // Parse header line
            let header_line = String::from_utf8_lossy(&buf[header_position..i]);
            let mut header_line = header_line.split(":");
            let key = header_line.next().expect("Header key missing").to_string();
            let value = header_line
                .next()
                .expect("Header value missing")
                .trim_start()
                .to_string();
            headers.insert(key, value);
            header_position = i + 2;

            // This means \r\n\r\n, which is the end of the headers
            // and the beginning of the body(or the end of the
            // request).
            if buf[i + 2] == CR && buf[i + 3] == LF {
                initial_position = i + 4;
                break;
            }
        }
    }

    debug!("Initial position: {initial_position}");

    debug!("Headers:");
    for (key, value) in headers.iter() {
        debug!("Key: {key}, Value: {value}");
    }

    if initial_position >= bytes_read {
        debug!("This is the end");
    }

    // content-length should be required if method is post:
    if method == "POST" {
        // FIXME: lowercase headers keys
        let content_length = headers
            .get("Content-Length")
            .expect("Content length should have been sent in the request");

        let content_length = content_length
            .parse::<u32>()
            .expect("Content length should fit in a u32");

        if bytes_read - initial_position >= content_length as usize {
            debug!("I have read enough from the socket!");
        } else {
            debug!("I need to read more from the socket!");
        }

        let mut cursor = bytes_read;
        let mut bytes_read = bytes_read;
        while bytes_read - initial_position < content_length as usize {
            debug!("Reading more data from the socket");
            if buf.len() == cursor {
                buf.resize(cursor * 2, 0);
            }

            // FIXME: I think there might be a bug here, in case
            // there's an error, or there's nothing to read from the
            // socket.
            let current_bytes_read = match stream.read(&mut buf[cursor..]).await {
                Ok(size) => size,
                Err(e) => {
                    debug!("Error reading TCP stream to parse command, request_id={request_id}, error={e}");
                    0
                }
            };

            if bytes_read == 0 {
                panic!("HELP");
            }

            cursor += current_bytes_read;
            bytes_read += current_bytes_read;
        }

        debug!("I read everything that i needed, ready to parse request body.");

        body = Some(String::from_utf8_lossy(&buf[initial_position..]).to_string());
    }

    let method = match method.as_str() {
        "GET" => Method::Get,
        "POST" => Method::Post,
        _ => Method::Unknown,
    };

    Request {
        method,
        path,
        headers,
        body,
    }
}
