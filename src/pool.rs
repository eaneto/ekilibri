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

/// A TCP connection pool.
///
/// The pool uses a fixed sized bounded mpsc channel to store and access the
/// connections. The pool stores the server's id and address, its size and a
/// timeout for establishing new connections.
pub struct ConnectionPool {
    /// The server id in Ekilibri's context.
    server_id: usize,
    /// Number of connections established in the pool.
    size: usize,
    /// A timeout in milliseconds for getting/establishing a new connection.
    timeout: Duration,
    /// Address of the server, used to establish new connections.
    server_address: String,
    /// An atomic value to define if the pool is at the reconnect state.
    reconnect: Arc<AtomicBool>,
    /// The sender used to send the connections to the channel.
    sender: Sender<Connection>,
    /// A receiver shared between all threads behind a [Mutex], the mutex is
    /// need to share the [Receiver] but the lock acquired should not be hold
    /// for long.
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
            timeout: Duration::from_millis(timeout),
            server_address: server_address.to_string(),
            sender,
            reconnect: Arc::new(AtomicBool::new(false)),
            receiver: Arc::new(Mutex::new(receiver)),
        }
    }

    /// Activates the reconnect state at the pool.
    pub async fn active_reconnection(&self) {
        self.set_reconnect(true);
        self.drop_connections().await;
    }

    /// Deactivates the reconnect state of the pool, dropping every possible
    /// connection in the channel and establishing new ones.
    pub async fn deactive_reconnection(&self) {
        // We assume that if there are any connections in the channel they are
        // not valid connections and should be dropped.
        self.drop_connections().await;
        self.establish_connections().await;
        self.set_reconnect(false);
    }

    /// Establishes every possible new connection in the pool, given the pool
    /// size.
    pub async fn establish_connections(&self) {
        for _ in 0..self.size {
            match self.establish_connection(false).await {
                Ok(connection) => self.send_connection(connection).await,
                Err(e) => {
                    warn!("Error establishing connection in the pool, error={e}")
                }
            }
        }
    }

    /// Gets a connection from the pool, creating a new one in cases where the
    /// pool is at the reconnect state.
    ///
    /// # Errors
    ///
    /// Any I/O error may be returned when the pool is at the reconnect state,
    /// but when it's not the only possible error is a timeout.
    pub async fn get_connection(&self) -> Result<Connection, io::Error> {
        if self.is_reconnecting() {
            debug!(
                "Creating a new connection with the server {}",
                self.server_id
            );
            return self.establish_connection(true).await;
        }

        match timeout(self.timeout, self.receiver.lock().await.recv()).await {
            Ok(option) => match option {
                Some(connection) => Ok(connection),
                None => Err(io::Error::from(io::ErrorKind::TimedOut)),
            },
            Err(e) => Err(io::Error::new(io::ErrorKind::TimedOut, e)),
        }
    }

    /// Returns the connection to the pool or drops the connection if the pool
    /// was at the reconnect state when the connection was acquired.
    pub async fn return_connection(&self, connection: Connection) {
        if !connection.should_drop() {
            trace!("Returning connection to the pool");
            self.sender
                .send(connection)
                .await
                .expect("Channel should be open");
        }
    }

    /// Creates a new connection in the pool.
    ///
    /// Established a new connection with the servers and sends it in the pool
    /// channel.
    pub async fn create_connection(&self) {
        match self.establish_connection(self.is_reconnecting()).await {
            Ok(connection) => self.send_connection(connection).await,
            Err(e) => {
                warn!(
                    "Failed to create a new connection in the pool during reconnection. server_id={}, error={e}", self.server_id
                );
            }
        }
    }

    /// Sends a new [connection] to the internal channel.
    async fn send_connection(&self, connection: Connection) {
        self.sender
            .send(connection)
            .await
            .expect("Channel should be open");
    }

    /// Establishes a new TCP connection with the server.
    ///
    /// When the connection is successfully established, the function tries to
    /// set "no delay" to true, if there's an error it is ignored and logged.
    ///
    /// # Errors
    ///
    /// Any I/O error that might happen in the [TcpStream::connect] call is
    /// returned, and timeout from [timeout].
    async fn establish_connection(&self, reconnecting: bool) -> Result<Connection, io::Error> {
        match timeout(self.timeout, TcpStream::connect(&self.server_address)).await {
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

    /// Locks the receiver and consumes every connection in the channel until it
    /// is empty.
    async fn drop_connections(&self) {
        let mut receiver = self.receiver.lock().await;
        while !receiver.is_empty() {
            let _ = receiver.recv().await;
        }
    }

    fn set_reconnect(&self, reconnect: bool) {
        self.reconnect.store(reconnect, Ordering::Relaxed);
    }

    fn is_reconnecting(&self) -> bool {
        self.reconnect.load(Ordering::Relaxed)
    }
}
