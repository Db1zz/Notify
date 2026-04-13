use std::sync::Arc;

use dashmap::DashMap;
use futures::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use serde::Deserialize;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::Mutex,
};
use tokio_tungstenite::{
    accept_async,
    tungstenite::{Bytes, Error, Message, Utf8Bytes},
    WebSocketStream,
};
use tracing::{error, info, instrument, warn};
use uuid::Uuid;

type WsSink = SplitSink<WebSocketStream<TcpStream>, Message>;
type SharedSink = Arc<Mutex<WsSink>>;
type ClientsMap = DashMap<Uuid, SharedSink>;
type SharedClientsMap = Arc<ClientsMap>;

#[derive(Deserialize)]
struct ConnectionData {
    userid: Uuid,
}

struct ConnectedClient {
    userid: Uuid,
    read: SplitStream<WebSocketStream<TcpStream>>,
}

pub struct ClientsManager {
    listener: Arc<TcpListener>,
    connected_clients: SharedClientsMap,
    addr: String,
}

impl ClientsManager {
    pub async fn new(addr: String) -> Self {
        Self {
            listener: Arc::new(TcpListener::bind(addr.clone()).await.unwrap()),
            connected_clients: Arc::new(DashMap::new()),
            addr,
        }
    }

    async fn connect_client_task(
        connected_clients: SharedClientsMap,
        write: SplitSink<WebSocketStream<TcpStream>, Message>,
        mut read: SplitStream<WebSocketStream<TcpStream>>,
    ) -> Result<ConnectedClient, ConnectionError> {
        let msg = match read.next().await {
            Some(Ok(m)) => m,
            Some(Err(e)) => {
                return Err(ConnectionError::from(ReadError::Websocket(e)));
            }
            None => {
                return Err(ConnectionError::from(ReadError::UnknownError));
            }
        };

        let buf: String = if msg.is_text() {
            msg.to_string()
        } else {
            return Err(ConnectionError::UnknownMessage);
        };

        let connection_data = serde_json::from_slice::<ConnectionData>(buf.as_bytes())?;
        if connected_clients.contains_key(&connection_data.userid) {
            return Err(ConnectionError::AlreadyConnected(connection_data.userid));
        }

        connected_clients.insert(connection_data.userid, Arc::new(Mutex::new(write)));

        Ok(ConnectedClient {
            userid: connection_data.userid,
            read,
        })
    }

    #[instrument(skip(connected_clients, client))]
    async fn watch_client_disconnect(
        connected_clients: SharedClientsMap,
        mut client: ConnectedClient,
    ) {
        let mut buf = String::new();

        loop {
            buf.clear();
            let msg = match client.read.next().await {
                Some(Ok(m)) => m,
                Some(Err(e)) => {
                    error!(error = ?e, "This is weird bugggg, should happen anyways...");
                    break;
                }
                None => {
                    break;
                }
            };

            if msg.is_close() {
                break;
            }
        }

        if connected_clients.remove(&client.userid).is_none() {
            error!(
				userid = client.userid.to_string(),
				"Client doesn't exists in a dashmap, if you see this message, it means there's a BUG...")
        }
    }

    #[instrument(skip(self))]
    pub async fn listen(&self) {
        loop {
            let (tcp_socket, client_addr) = self.listener.accept().await.unwrap();
            let ws_stream = accept_async(tcp_socket).await.expect("Handshake failed");
            let cloned_connected_clients = self.connected_clients.clone();

            tokio::spawn(async move {
                let (write, read) = ws_stream.split();
                let result =
                    Self::connect_client_task(cloned_connected_clients.clone(), write, read).await;
                match result {
                    Ok(client) => {
                        info!(
                            cl_addr = client_addr.to_string(),
                            "A new client connected to the server"
                        );
                        Self::watch_client_disconnect(cloned_connected_clients, client).await;
                    }
                    Err(e) => {
                        warn!(error = ?e, "Failed to establish connection with a client");
                    }
                }
            });
        }
    }

    fn get_client(&self, id: Uuid) -> Result<SharedSink, SendMessageError> {
        self.connected_clients
            .get(&id)
            .map(|ref_guard| ref_guard.clone())
            .ok_or(SendMessageError::ClientNotFound)
    }

    pub fn get_clients_count(&self) -> usize {
        self.connected_clients.len()
    }

    pub async fn is_client_connected(&self, id: Uuid) -> bool {
        if let Ok(client) = self.get_client(id) {
            let msg = Message::Ping(Bytes::from("ping"));
            return client.lock().await.send(msg).await.is_ok();
        }
        false
    }

    pub async fn send_to_client(&self, id: Uuid, data: String) -> Result<(), SendMessageError> {
        let client = self.get_client(id)?;
        let msg = Message::Text(Utf8Bytes::from(data));

        client.lock().await.send(msg).await?;

        Ok(())
    }

    pub fn destroy_client(&self, client_id: Uuid) {
        self.connected_clients.remove(&client_id);
    }

    pub fn get_addr(&self) -> &String {
        &self.addr
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ReadError {
    #[error(transparent)]
    Websocket(#[from] Error),

    #[error("Unknown read error")]
    UnknownError,
}

#[derive(thiserror::Error, Debug)]
pub enum ConnectionError {
    #[error(transparent)]
    Serialize(#[from] serde_json::Error),

    #[error("User with id {0} is already connected")]
    AlreadyConnected(Uuid),

    #[error("Received unknown message through a websocket")]
    UnknownMessage,

    #[error(transparent)]
    ReadError(#[from] ReadError),
}

#[derive(thiserror::Error, Debug)]
pub enum SendMessageError {
    #[error("Client not found")]
    ClientNotFound,

    #[error(transparent)]
    Websocket(#[from] Error),
}
