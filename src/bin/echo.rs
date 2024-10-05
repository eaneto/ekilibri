use std::time::Duration;

use clap::Parser;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
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

                    let mut cursor = 0;
                    let mut buf = vec![0u8; 4096];

                    let CR = '\r';
                    let LF = '\n';

                    // TODO: https://www.rfc-editor.org/rfc/rfc2616#section-2
                    // First line: VERB PATH PROTOCOL
                    // Second line: Headers
                    // Other line: Body
                    loop {
                        if buf.len() == cursor {
                            buf.resize(cursor * 2, 0);
                        }

                        let bytes_read = match stream.read(&mut buf[cursor..]).await {
                            Ok(size) => size,
                            Err(e) => {
                                debug!("Error reading TCP stream to parse command, request_id={request_id}, error={e}");
                                break;
                            }
                        };

                        if bytes_read == 0 {
                            break;
                        }

                        cursor += bytes_read;

                        // if reach_end(\r\n) {
                        //}
                    }

                    debug!("{}", String::from_utf8_lossy(&buf));

                    //let buf_reader = BufReader::new(&mut stream);
                    //let mut request_lines = buf_reader.lines();
                    //if let Ok(line) = request_lines.next_line().await {
                    //    let line = line.unwrap();
                    //    match &line[..] {
                    //        "POST /echo HTTP/1.1" => {
                    //            // TODO: parse headers
                    //            // TODO: parse request body -> CRLF + CRLF
                    //            let mut response_body = String::new();

                    //            // TODO: Read content-type
                    //            // TODO: Skip headers, read only the request body.
                    //            while let Ok(new_line) = request_lines.next_line().await {
                    //                match new_line {
                    //                    Some(content) => {
                    //                        debug!("content={content}");
                    //                        response_body.push_str(&content)
                    //                    }
                    //                    None => break,
                    //                }
                    //            }

                    //            debug!("Read the entire request, response={response_body}");
                    //            let content_length = response_body.len();
                    //            stream
                    //                .write_all(
                    //                    format!(
                    //                        "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {content_length}\r\n\r\n{response_body}\r\n\r\n")
                    //                        .as_bytes(),
                    //                )
                    //                .await
                    //                .unwrap();
                    //            info!("Replied message, request_id={request_id}");
                    //        }
                    //        "GET /sleep HTTP/1.1" => {
                    //            time::sleep(Duration::from_millis(2000)).await;
                    //            let response = "HTTP/1.1 200 OK\r\n\r\n";
                    //            stream.write_all(response.as_bytes()).await.unwrap();
                    //            info!("Replied message, request_id={request_id}");
                    //        }
                    //        _ => debug!("Nothing"),
                    //    }
                    //}
                });
            }
            Err(_) => eprintln!("Error listening to socket"),
        }
    }
}
