use std::{
    io,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    net::TcpStream,
    sync::{
        mpsc::{Receiver, Sender},
        Mutex,
    },
    time::timeout,
};

use tracing::{debug, trace, warn};

pub struct Connection {
    pub stream: TcpStream,
    drop: bool,
}

impl Connection {
    pub fn new(stream: TcpStream, drop: bool) -> Connection {
        Connection { stream, drop }
    }

    fn should_drop(&self) -> bool {
        self.drop
    }
}

pub struct ConnectionPool {
    server_id: usize,
    size: usize,
    timeout: u64,
    server_address: String,
    sender: Sender<Connection>,
    reconnect: Arc<AtomicBool>,
    receiver: Arc<Mutex<Receiver<Connection>>>,
}

impl ConnectionPool {
    pub fn new(
        id: usize,
        size: usize,
        timeout: u64,
        server_address: &str,
        sender: Sender<Connection>,
        receiver: Receiver<Connection>,
    ) -> ConnectionPool {
        ConnectionPool {
            server_id: id,
            size,
            timeout,
            server_address: server_address.to_string(),
            sender,
            reconnect: Arc::new(AtomicBool::new(false)),
            receiver: Arc::new(Mutex::new(receiver)),
        }
    }

    pub fn set_reconnect(&self, reconnect: bool) {
        self.reconnect.store(reconnect, Ordering::Relaxed);
    }

    pub fn is_reconnecting(&self) -> bool {
        self.reconnect.load(Ordering::Relaxed)
    }

    pub async fn establish_connections(&self) {
        for _ in 0..self.size {
            match self.create_connection(false).await {
                Ok(connection) => self.send_connection(connection).await,
                Err(e) => {
                    warn!("Error establishing connection in the pool, error={e}")
                }
            }
        }
    }

    /// Locks the receiver and consumes every connection in the
    /// channel until it is empty.
    pub async fn drop_connections(&self) {
        let mut receiver = self.receiver.lock().await;
        while !receiver.is_empty() {
            let _ = receiver.recv().await;
        }
    }

    pub async fn get_connection(&self) -> Result<Connection, io::Error> {
        if self.is_reconnecting() {
            debug!(
                "Creating a new connection with the server {}",
                self.server_id
            );
            return self.create_connection(true).await;
        }

        match timeout(
            Duration::from_millis(self.timeout),
            self.receiver.lock().await.recv(),
        )
        .await
        {
            Ok(option) => match option {
                Some(connection) => Ok(connection),
                None => Err(io::Error::from(io::ErrorKind::TimedOut)),
            },
            Err(e) => Err(io::Error::new(io::ErrorKind::TimedOut, e)),
        }
    }

    /// Returns the connection to the channel or drops the
    /// connection if the pool is at the reconnect state.
    pub async fn return_connection(&self, connection: Connection) {
        if !connection.should_drop() {
            trace!("Returning connection to the pool");
            self.sender
                .send(connection)
                .await
                .expect("Channel should be open");
        }
    }

    pub async fn create_connection(&self, reconnecting: bool) -> Result<Connection, io::Error> {
        match timeout(
            Duration::from_millis(self.timeout),
            TcpStream::connect(&self.server_address),
        )
        .await
        {
            Ok(result) => match result {
                Ok(stream) => {
                    if let Err(e) = stream.set_nodelay(true) {
                        warn!("Error setting nodelay on stream, data will be buffered. error={e}");
                    }
                    Ok(Connection::new(stream, reconnecting))
                }
                Err(e) => {
                    warn!(
                        "Error while connecting to the server, {} might be down, {e}",
                        self.server_id
                    );
                    Err(e)
                }
            },
            Err(e) => {
                warn!(
                    "Connection timeout, server {} might be down",
                    self.server_id
                );
                Err(io::Error::new(io::ErrorKind::TimedOut, e))
            }
        }
    }

    pub async fn send_connection(&self, connection: Connection) {
        self.sender
            .send(connection)
            .await
            .expect("Channel should be open");
    }
}
