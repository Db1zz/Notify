use std::sync::Arc;
use async_trait::async_trait;
use rdkafka::message::ToBytes;
use tokio::{io::AsyncWriteExt, net::{TcpListener, tcp::OwnedWriteHalf}, sync::Mutex};

use crate::manager::task_manager::{TaskManager, TaskManagerTask};
use crate::metrics::metrics_receiver::MetricsReceiver;

struct LoadBalancerTask {
	writer: Arc<Mutex<OwnedWriteHalf>>,
	metrics: Arc<MetricsReceiver>
}

impl LoadBalancerTask {
	pub fn new(writer: Arc<Mutex<OwnedWriteHalf>>, metrics: Arc<MetricsReceiver>) -> Self {
		Self {
			writer,
			metrics
		}
	}
}

#[async_trait]
impl TaskManagerTask for LoadBalancerTask {
	async fn handle(&self) {
		let result = self.metrics.get_least_loaded_consumer_node().await;
		match result {
			Ok(addr) => {
				let _ = self.writer.lock().await.write_all(&addr.to_string().to_bytes()).await;
			}
			Err(e) => {
				eprintln!("Error: {}", e);
			}
		}

	}
}

pub struct LoadBalancer {
	listener: Arc<TcpListener>,
	task_manager: TaskManager<LoadBalancerTask>,
	metrics_receiver: Arc<MetricsReceiver>,
}

impl LoadBalancer {
	pub async fn new(addr: String, metrics_receiver_addr: String) -> Self {
		Self {
			listener: Arc::new(TcpListener::bind(addr).await.unwrap()),

			task_manager: TaskManager::new(5),
			metrics_receiver: Arc::new(MetricsReceiver::new(metrics_receiver_addr).await)
		}
	}

	pub async fn start(&mut self) {
		self.task_manager.start();
		let metrics_receiver = self.metrics_receiver.clone();

		tokio::spawn(async move {
			metrics_receiver.start().await;
		});

		loop {
			let (socket, _) = self.listener.accept().await.unwrap(); 
			let (_, writer) = socket.into_split();
			let protected_writer = Arc::new(Mutex::new(writer));

			let task = LoadBalancerTask::new(protected_writer, self.metrics_receiver.clone());
			self.task_manager.submit(task).await;
		}
	}
}