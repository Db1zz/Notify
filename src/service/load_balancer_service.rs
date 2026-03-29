/*
    Purpose of the service:
        Load balancing (LB) services distribute
        the load between consumers and producers
        by selecting the least-loaded node.
        Distribution is carried out by sending node addresses
        to the client.

    Technical requirements:
        1. The load balancer MUST be aware of (consumers and producers)
        2. The load balancer is SINGLE-THREADED (Why? For simplicity. The service itself is not used as often as others;
            more notifications pass through the notification manager than new connections coming to this service.)
        3. Single connection for clients. The task is only to send addresses. If a user wants to receive
            new addresses, they can do so by connecting to the load balancer again.
        4. The load balancer must receive information about node load
*/

use std::{sync::Arc, thread::sleep, time::Duration};
use std::net::SocketAddr;
use async_trait::async_trait;
use tokio::net::{TcpListener, tcp::{OwnedReadHalf, OwnedWriteHalf}};

use crate::manager::task_manager::{TaskManager, TaskManagerTask};

struct LoadBalancerTask {
	client_addr: SocketAddr,
	reader: OwnedReadHalf,
	writer: OwnedWriteHalf
}

impl LoadBalancerTask {
	pub fn new(client_addr: SocketAddr, reader: OwnedReadHalf, writer: OwnedWriteHalf) -> Self {
		Self {
			client_addr,
			reader,
			writer
		}
	}
}

#[async_trait]
impl TaskManagerTask for LoadBalancerTask
{
	async fn handle(self) {
		println!("TODO LoadBalancerTask");
	}
}

pub struct LoadBalancer {
	// consumers: Arc<Vec<String>>,
	// producers: Arc<Vec<String>>,

	listener: Arc<TcpListener>,
	task_manager: TaskManager<LoadBalancerTask>,
}

impl LoadBalancer {
	pub async fn new(addr: String) -> Self {
		Self {
			// consumers: Arc::new(Vec::new()),
			// producers: Arc::new(Vec::new()),
			listener: Arc::new(TcpListener::bind(addr).await.unwrap()),

			task_manager: TaskManager::new(5),
		}
	}

	// async fn update_load_status(&self) {

	// }

	// async fn get_least_loaded_nodes(&self) {
	// }

	// async fn handle_new_connection(&self) {

	// }

	fn start_load_updater(&self) {
		// let lb_clone = self.clone();
		tokio::spawn(async move {
			loop {
				// TODO
				// lb_clone.update_load_status().await;
				sleep(Duration::from_secs(1));
			}
		});
	}

	pub async fn start(&mut self) {
		self.task_manager.start();
		self.start_load_updater();

		loop {
			let (socket, client_addr) = self.listener.accept().await.unwrap(); 
			let (reader, writer) = socket.into_split();

			let task = LoadBalancerTask::new(client_addr, reader, writer);
			self.task_manager.submit(task).await;
		}
	}
}