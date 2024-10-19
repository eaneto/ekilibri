use thiserror::Error;

use std::collections::HashMap;
use tokio::{io::AsyncReadExt, net::TcpStream};

use tracing::debug;

const CR: u8 = 13;
const LF: u8 = 10;
pub const HTTP_VERSION: &str = "HTTP/1.1";
pub const CRLF: &str = "\r\n";

pub enum Method {
    Get,
    Post,
    Unknown,
}

pub struct Request {
    pub method: Method,
    pub path: String,
    pub headers: HashMap<String, String>,
    pub body: Option<String>,
}

#[derive(Error, Debug)]
pub enum ParsingError {
    #[error("There was no data to be read from the socket")]
    UnableToRead,
    #[error("The request line was impossible to parse, missing information")]
    MalformedRequest,
    #[error("The header is not parseable")]
    MalformedHeader,
    #[error("The request was not sent with the content-length header")]
    MissingContentLength,
    #[error("The request was sent with an invalid content-length value")]
    InvalidContentLength,
    #[error("The request was sent for an unsupported HTTP version")]
    HTTPVersionNotSupported,
}

pub async fn parse_request(
    request_id: &uuid::Uuid,
    stream: &mut TcpStream,
) -> Result<(Request, Vec<u8>), ParsingError> {
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
        return Err(ParsingError::UnableToRead);
    }

    // Parse request line
    let mut initial_position = 0;
    for i in 0..(buf.len() - 1) {
        if buf[i] == CR && buf[i + 1] == LF {
            let request_line = String::from_utf8_lossy(&buf[initial_position..i]);
            let mut request_line = request_line.split_whitespace();
            method = match request_line.next() {
                Some(method) => method.to_string(),
                None => return Err(ParsingError::MalformedRequest),
            };
            path = match request_line.next() {
                Some(path) => path.to_string(),
                None => return Err(ParsingError::MalformedRequest),
            };
            protocol = match request_line.next() {
                Some(protocol) => protocol.to_string(),
                None => return Err(ParsingError::MalformedRequest),
            };
            initial_position = i + 2;
            break;
        }
    }

    if protocol != HTTP_VERSION {
        return Err(ParsingError::HTTPVersionNotSupported);
    }

    // Parse headers
    let mut header_position = initial_position;
    for i in initial_position..(buf.len() - 3) {
        if buf[i] == CR && buf[i + 1] == LF {
            // Parse header line
            let header_line = String::from_utf8_lossy(&buf[header_position..i]);
            let mut header_line = header_line.split(":");
            let key = match header_line.next() {
                Some(key) => key.to_string(),
                None => return Err(ParsingError::MalformedHeader),
            };
            let value = match header_line.next() {
                Some(value) => value.trim_start().to_string(),
                None => return Err(ParsingError::MalformedHeader),
            };
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

    // content-length should be required if method is post:
    if method == "POST" {
        // FIXME: lowercase headers keys
        let content_length = match headers.get("Content-Length") {
            Some(length) => length,
            None => return Err(ParsingError::MissingContentLength),
        };

        let content_length = match content_length.parse::<u32>() {
            Ok(length) => length,
            Err(_) => return Err(ParsingError::InvalidContentLength),
        };

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

    Ok((
        Request {
            method,
            path,
            headers,
            body,
        },
        buf,
    ))
}
