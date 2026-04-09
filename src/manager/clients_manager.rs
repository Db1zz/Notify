use std::sync::Arc;

use dashmap::DashMap;
use serde::Deserialize;
use tokio::{io::AsyncReadExt, net::{TcpListener, tcp::{OwnedReadHalf, OwnedWriteHalf}}, sync::Mutex};
use tracing::{instrument, warn, error, info};
use uuid::Uuid;

#[derive(Deserialize)]
struct ConnectionData {
	userid: Uuid
}

struct ConnectedClient {
	userid: Uuid,
	reader: OwnedReadHalf
}

pub struct ClientsManager {
	listener: Arc<TcpListener>,
	connected_clients: Arc<DashMap<Uuid, Arc<Mutex<OwnedWriteHalf>>>>,
	addr: String,
}

impl ClientsManager {
	pub async fn new(addr: String) -> Self {
		Self {
			listener: Arc::new(TcpListener::bind(addr.clone()).await.unwrap()),
			connected_clients: Arc::new(DashMap::new()),
			addr: addr
		}
	}

	async fn connect_client_task(
		connected_clients: Arc<DashMap<Uuid, Arc<Mutex<OwnedWriteHalf>>>>,
		mut reader: OwnedReadHalf,
		writer: OwnedWriteHalf) -> Result<ConnectedClient, ConnectionError>
	{
		let mut buf = [0; 1024];

		let rb = reader.read(&mut buf).await.unwrap();
		let connection_data = serde_json::from_slice::<ConnectionData>(&buf[..rb])?;

		if connected_clients.contains_key(&connection_data.userid) {
			return Err(ConnectionError::AlreadyConnected(connection_data.userid));
		}

		connected_clients.insert(connection_data.userid.clone(), Arc::new(Mutex::new(writer)));

		Ok(ConnectedClient {
			userid: connection_data.userid,
			reader
		})
	}

	#[instrument(skip(connected_clients, client))]
	async fn watch_client_disconnect(
		connected_clients: Arc<DashMap<Uuid, Arc<Mutex<OwnedWriteHalf>>>>,
		mut client: ConnectedClient)
	{
		let mut buf = [0; 1024];

		loop {
			match client.reader.read(&mut buf).await {
				Ok(0) => {
					info!(
						userid = client.userid.to_string(), // Tbh casting UUID to a string is kinda inefficient in a loaded system... TODO
						"A user has been disconnected");
					break;
				}
				Ok(_) => continue,
				Err(e) => {
					error!(error = ?e, "TCP read() error");
                	break;
				}
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
			let (socket, client_addr) = self.listener.accept().await.unwrap();
			let cloned_connected_clients = self.connected_clients.clone();

			// Note: info span for logs is too heavy, and shouldn't be created for each connection...
			tokio::spawn(async move {
				let (reader, writer) = socket.into_split();
				let result = Self::connect_client_task(cloned_connected_clients.clone(), reader, writer).await;
				match result {
					Ok(client) => {
						info!(cl_addr = client_addr.to_string(), "A new client connected to the server");
						Self::watch_client_disconnect(cloned_connected_clients, client).await;
					}
					Err(e) => {
						warn!(error = ?e, "Failed to establish connection with a client");
					}
				}
			});
		}
	}

	pub fn get_clients_count(&self) -> usize {
		self.connected_clients.len()
	}

	pub fn get_client(&self, id: Uuid) -> Option<Arc<Mutex<OwnedWriteHalf>>> {
		self.connected_clients
			.get(&id)
			.map(|ref_guard| ref_guard.clone())
	}

	pub fn destroy_client(&self, client_id: Uuid) {
		self.connected_clients.remove(&client_id);
	}

	pub fn get_addr(&self) -> &String {
		return &self.addr;
	}
}

#[derive(thiserror::Error, Debug)]
pub enum ConnectionError {
    #[error(transparent)]
    Serialize(#[from] serde_json::Error),

    #[error("User with id {0} is already connected")]
    AlreadyConnected(Uuid),
}