use std::{str::FromStr, sync::Arc};

use dashmap::DashMap;
use futures::lock::Mutex;
use tokio::{io::AsyncReadExt, net::{TcpListener, tcp::{OwnedReadHalf, OwnedWriteHalf}}};
use uuid::Uuid;

pub struct ClientsManager {
	listener: TcpListener,
	connected_clients: Arc<DashMap<Uuid, Arc<Mutex<OwnedWriteHalf>>>>,
}

impl ClientsManager {
	pub async fn new(addr: String) -> Self {
		Self {
			listener: TcpListener::bind(addr).await.unwrap(),
			connected_clients: Arc::new(DashMap::new())
		}
	}

	async fn connect_client_task(connected_clients: Arc<DashMap<Uuid, Arc<Mutex<OwnedWriteHalf>>>>, mut reader: OwnedReadHalf, writer: OwnedWriteHalf) {
		let mut buf = [0; 1024];

		let rb = reader.read(&mut buf).await.unwrap();
		let payload = String::from_utf8_lossy(&buf[..rb]);
		let json: serde_json::Value = match serde_json::from_str(&payload) {
			Ok(v) => v,
			Err(err) => {
				println!("todo 1 {}", err);
				return;
			}
		};

		let user_id: String = match json.get("user_id").and_then(|v| v.as_str()) {
			Some(s) => s.to_string(),
			None => {
				println!("Error! Expected JSON \"user_id\":\"123\"");
				return ;
			}
		};

		let uuid: Uuid = match Uuid::from_str(&user_id) {
			Ok(u) => u, 
			Err(err) => {
				println!("todo 3 {}", err);
				return;
			}
		};

		connected_clients.insert(uuid, Arc::new(Mutex::new(writer)));
	}

	pub async fn listen(&self) {
		loop {
			let (socket, client_addr) = self.listener.accept().await.unwrap();
			let (reader, writer) = socket.into_split();

			let cloned_connected_clients = self.connected_clients.clone();
			tokio::spawn(async move {
				Self::connect_client_task(cloned_connected_clients, reader, writer).await;
				println!("A new client connected to the server {}", client_addr);
			});
		}
	}

	pub fn get_client(&self, id: Uuid) -> Option<Arc<Mutex<OwnedWriteHalf>>> {
		self.connected_clients
			.get(&id)
			.map(|ref_guard| ref_guard.clone())
	}

	pub fn destroy_client(&self, client_id: Uuid) {
		self.connected_clients.remove(&client_id);
	}
}