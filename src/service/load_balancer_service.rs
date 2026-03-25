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
use tokio::net::{TcpListener, tcp::{OwnedReadHalf, OwnedWriteHalf}};
use crossbeam_deque::{Injector, Stealer, Worker};

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

#[derive(Clone)]
pub struct LoadBalancer {
	consumers: Arc<Vec<String>>,
	producers: Arc<Vec<String>>,

	listener: Arc<TcpListener>,

    injector: Arc<Injector<LoadBalancerTask>>,
    stealers: Arc<Vec<Stealer<LoadBalancerTask>>>,
}

impl LoadBalancer {
	pub async fn new(addr: String) -> Self {
		Self {
			consumers: Arc::new(Vec::new()),
			producers: Arc::new(Vec::new()),
			listener: Arc::new(TcpListener::bind(addr).await.unwrap()),

			injector: Arc::new(Injector::new()),
			stealers: Arc::new(Vec::new())
		}
	}

	async fn update_load_status(&self) {

	}

	async fn get_least_loaded_nodes(&self) {
		
	}

	// async fn handle_new_connection(&self) {

	// }

	fn start_load_updater(&self) {
		let lb_clone = self.clone();
		tokio::spawn(async move {
			loop {
				lb_clone.update_load_status().await;
				sleep(Duration::from_secs(1));
			}
		});
	}

	fn handle_task(&self, task: LoadBalancerTask) {
		//let nodes = self.get_least_loaded_nodes();
		// write addresses to a client...
		// close socket and safely finish task...
	}

	fn spawn_workers(&self) {
		let amount_of_workers = 5;

		let mut workers = Vec::new();
		let mut stealers = Vec::new();
	
		for _ in 0..amount_of_workers {
			let worker = Worker::<LoadBalancerTask>::new_fifo();
			stealers.push(worker.stealer());
			workers.push(worker);
		}

		for worker in workers {
			let lb_clone = self.clone();
			let stealers = self.stealers.clone();
			let injector = self.injector.clone();

			std::thread::spawn(move || {
				loop {
					if let Some(task) = worker.pop() {
						lb_clone.handle_task(task);
						continue;
					}

					if let Some(task) = injector.steal().success() {
						lb_clone.handle_task(task);
						continue;
					}

					for stealer in &*stealers {
						if let Some(task) = stealer.steal().success() {
							lb_clone.handle_task(task);
							break;
						}
					}
				}
			});
		}
	}

	pub async fn start(&self) {
		self.start_load_updater();
		self.spawn_workers();

		loop {
			// TODO: I think it should be handled if we exceed the maximum number of fds
			let (socket, client_addr) = self.listener.accept().await.unwrap(); 
			let (reader, writer) = socket.into_split();

			let task = LoadBalancerTask::new(client_addr, reader, writer);
			self.injector.push(task);
		}
	}
}