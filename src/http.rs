use bytes::Bytes;
use thiserror::Error;

use std::{collections::HashMap, time::Duration};
use tokio::{io::AsyncReadExt, net::TcpStream, time::timeout};

use tracing::debug;

const CR: u8 = 13;
const LF: u8 = 10;
pub const HTTP_VERSION: &str = "HTTP/1.1";
pub const CRLF: &str = "\r\n";

#[derive(PartialEq)]
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
    bytes: Bytes,
}

impl Request {
    pub fn as_bytes(&self) -> Bytes {
        Bytes::clone(&self.bytes)
    }
}

pub struct Response {
    pub status: u16,
    _reason: String,
    pub headers: HashMap<String, String>,
    pub body: Option<String>,
    bytes: Bytes,
}

impl Response {
    #[must_use]
    pub fn new(
        status: u16,
        reason: String,
        headers: HashMap<String, String>,
        body: Option<String>,
        bytes: Bytes,
    ) -> Response {
        Response {
            status,
            _reason: reason,
            headers,
            body,
            bytes,
        }
    }

    #[must_use]
    pub fn from_status(status: u16) -> Response {
        let reason = match status {
            400 => "Bad Request",
            411 => "Length Required",
            502 => "Bad Gateway",
            504 => "Gateway Time-out",
            505 => "HTTP Version not supported",
            _ => "Unsupported status",
        };

        let status_line = format!("{HTTP_VERSION} {status} {reason}{CRLF}{CRLF}");
        let bytes = Bytes::from(status_line);

        Response::new(status, reason.to_string(), HashMap::new(), None, bytes)
    }

    pub fn as_bytes(&self) -> Bytes {
        Bytes::clone(&self.bytes)
    }
}

#[derive(PartialEq)]
pub enum ConnectionHeader {
    KeepAlive,
    Close,
}

#[derive(Error, Debug)]
pub enum ParsingError {
    #[error("There was no data to be read from the socket")]
    UnableToRead,
    #[error("Request timed out while waiting for the server response")]
    ReadTimeout,
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
    // Response error
    #[error("The request was sent with an invalid HTTP status")]
    InvalidStatus,
    #[error("The status line was impossible to parse, missing information")]
    MalformedResponse,
}

/// Parses a HTTP request from a given connection.
///
/// # Errors
///
/// There are multiple errors that could happen while parsing the request, they
/// are all documented under [`ParsingError`].
pub async fn parse_request(
    request_id: &uuid::Uuid,
    stream: &mut TcpStream,
) -> Result<Request, ParsingError> {
    let mut method = String::new();
    let mut path = String::new();
    let mut protocol = String::new();
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

    let mut cursor_position = 0;
    for i in 0..(buf.len() - 1) {
        if buf[i] == CR && buf[i + 1] == LF {
            let request_line = String::from_utf8_lossy(&buf[cursor_position..i]);
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
            cursor_position = i + 2;
            break;
        }
    }

    if protocol != HTTP_VERSION {
        return Err(ParsingError::HTTPVersionNotSupported);
    }

    let method = match method.as_str() {
        "GET" => Method::Get,
        "POST" => Method::Post,
        _ => Method::Unknown,
    };

    let (headers, position) = parse_headers(&buf, cursor_position)?;
    cursor_position = position;

    // content-length should be required if method is post:
    let final_cursor_position = if method == Method::Post {
        let Some(content_length) = headers.get("content-length") else {
            return Err(ParsingError::MissingContentLength);
        };

        let Ok(content_length) = content_length.parse::<u32>() else {
            return Err(ParsingError::InvalidContentLength);
        };

        if bytes_read - cursor_position >= content_length as usize {
            debug!("I have read enough from the socket!");
        } else {
            debug!("I need to read more from the socket!");
        }

        let mut body_cursor = bytes_read;
        let mut bytes_read = bytes_read;
        while bytes_read - cursor_position < content_length as usize {
            debug!("Reading more data from the socket");
            if buf.len() == body_cursor {
                buf.resize(body_cursor * 2, 0);
            }

            let current_bytes_read = match stream.read(&mut buf[body_cursor..]).await {
                Ok(size) => size,
                Err(e) => {
                    debug!("Error reading TCP stream to parse command, request_id={request_id}, error={e}");
                    0
                }
            };

            if bytes_read == 0 {
                break;
            }

            body_cursor += current_bytes_read;
            bytes_read += current_bytes_read;
        }

        debug!("I read everything that i needed, ready to parse request body.");

        body = Some(String::from_utf8_lossy(&buf[cursor_position..]).to_string());

        cursor_position + content_length as usize
    } else {
        cursor_position
    };

    Ok(Request {
        method,
        path,
        headers,
        body,
        bytes: Bytes::copy_from_slice(&buf[..final_cursor_position]),
    })
}

/// Parses a HTTP response from a given connection.
///
/// # Errors
///
/// There are multiple errors that could happen while parsing the response, they
/// are all documented under [`ParsingError`].
pub async fn parse_response(
    stream: &mut TcpStream,
    read_timeout_ms: u64,
) -> Result<Response, ParsingError> {
    let mut protocol = String::new();
    let mut status = 0_u16;
    let mut reason = String::new();
    let mut body: Option<String> = None;

    let mut buf = vec![0u8; 4096];
    let bytes_read = if let Ok(result) = timeout(
        Duration::from_millis(read_timeout_ms),
        stream.read(&mut buf),
    )
    .await
    {
        match result {
            Ok(size) => size,
            Err(e) => {
                debug!("Error reading TCP stream to parse command, error={e}");
                0
            }
        }
    } else {
        debug!("Time out reading response");
        return Err(ParsingError::ReadTimeout);
    };

    if bytes_read == 0 {
        return Err(ParsingError::UnableToRead);
    }

    let mut cursor_position = 0;
    for i in 0..(buf.len() - 1) {
        if buf[i] == CR && buf[i + 1] == LF {
            let request_line = String::from_utf8_lossy(&buf[cursor_position..i]);
            let mut request_line = request_line.split_whitespace();
            protocol = match request_line.next() {
                Some(protocol) => protocol.to_string(),
                None => return Err(ParsingError::MalformedRequest),
            };
            // TODO: Make sure status is valid
            status = match request_line.next() {
                Some(status) => match status.parse::<u16>() {
                    Ok(status) => status,
                    Err(_) => return Err(ParsingError::InvalidStatus),
                },
                None => return Err(ParsingError::MalformedRequest),
            };
            for response_reason in request_line {
                reason.push_str(&format!("{response_reason} "));
            }
            reason = reason.trim_end().to_string();
            cursor_position = i + 2;
            break;
        }
    }

    if protocol != HTTP_VERSION {
        return Err(ParsingError::HTTPVersionNotSupported);
    }

    let (headers, position) = parse_headers(&buf, cursor_position)?;
    cursor_position = position;

    let Some(content_length) = headers.get("content-length") else {
        return Ok(Response::new(
            status,
            reason,
            headers,
            body,
            Bytes::copy_from_slice(&buf),
        ));
    };

    let Ok(content_length) = content_length.parse::<u32>() else {
        return Err(ParsingError::InvalidContentLength);
    };

    let mut body_cursor = bytes_read;
    let mut bytes_read = bytes_read;
    while bytes_read - cursor_position < content_length as usize {
        debug!("Reading more data from the socket");
        if buf.len() == body_cursor {
            buf.resize(body_cursor * 2, 0);
        }

        let current_bytes_read = if let Ok(result) = timeout(
            Duration::from_millis(read_timeout_ms),
            stream.read(&mut buf[body_cursor..]),
        )
        .await
        {
            match result {
                Ok(size) => size,
                Err(e) => {
                    debug!("Error reading TCP stream to parse command, error={e}");
                    0
                }
            }
        } else {
            debug!("Time out reading response");
            return Err(ParsingError::ReadTimeout);
        };

        if bytes_read == 0 {
            break;
        }

        body_cursor += current_bytes_read;
        bytes_read += current_bytes_read;
    }

    body = Some(String::from_utf8_lossy(&buf[cursor_position..]).to_string());

    Ok(Response::new(
        status,
        reason,
        headers,
        body,
        Bytes::copy_from_slice(&buf),
    ))
}

fn parse_headers(
    buf: &[u8],
    mut initial_position: usize,
) -> Result<(HashMap<String, String>, usize), ParsingError> {
    let mut headers = HashMap::<String, String>::new();
    let mut header_position = initial_position;
    for i in initial_position..(buf.len() - 3) {
        if buf[i] == CR && buf[i + 1] == LF {
            let header_line = String::from_utf8_lossy(&buf[header_position..i]);
            if header_line.is_empty() {
                break;
            }
            let header_line = header_line.split_once(':');
            let (key, value) = match header_line {
                Some((key, value)) => {
                    if key.is_empty() || value.is_empty() {
                        return Err(ParsingError::MalformedHeader);
                    }
                    (key.to_string(), value.trim_start().to_string())
                }
                None => return Err(ParsingError::MalformedHeader),
            };

            headers.insert(key.to_ascii_lowercase(), value);
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
    Ok((headers, initial_position))
}
