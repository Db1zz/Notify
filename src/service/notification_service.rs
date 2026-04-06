use std::sync::Arc;
use std::time::{Duration, Instant};
use async_trait::async_trait;
use tokio::io::AsyncWriteExt;
use tokio::time::sleep;

use crate::manager::task_manager::{TaskManager, TaskManagerTask};
use crate::metrics::metrics_sender::MetricsSender;
use crate::models::notification::Notification;
use crate::consumer::notification_stream_consumer::NotificationStreamConsumer;
use crate::repository::repository::{Repository, RepositoryError};
use crate::manager::ClientsManager;
use crate::metrics::{LoadMetrics, Message, Metrics, NodeRole, Register};

pub struct NotificationServiceTask<NotifsToSendRepo, BlockedNotifsRepo> {
	notification: Notification,
	repo_notifs_to_send: Arc<NotifsToSendRepo>,
	repo_blocked_notifs: Arc<BlockedNotifsRepo>,
	clients_manager: Arc<ClientsManager>,
	metrics: Arc<LoadMetrics>
}

impl<NotifsToSendRepo, BlockedNotifsRepo> NotificationServiceTask<NotifsToSendRepo, BlockedNotifsRepo> 
where
	NotifsToSendRepo: Repository<Item = Notification> + 'static,
	BlockedNotifsRepo: Repository<Item = Notification> + 'static,
{
	pub fn new(
		notification: Notification,
		repo_notifs_to_send: Arc<NotifsToSendRepo>,
		repo_blocked_notifs: Arc<BlockedNotifsRepo>,
		clients_manager: Arc<ClientsManager>,
		metrics: Arc<LoadMetrics>) -> Self
	{
		Self {
			notification,
			repo_notifs_to_send,
			repo_blocked_notifs,
			clients_manager,
			metrics
		}
	}

	async fn send_notification(&self, notification: &Notification) -> bool {
		match self.clients_manager.get_client(notification.userid) {
			Some(client) => {
				// TODO: some error handling
				let mut writer = client.lock().await;
				writer.write_all(notification.sourceid.as_bytes()).await;
				return true;
			},
			None => {
				return false;
			}
		}
	}
}

#[async_trait]
impl<NotifsToSendRepo, BlockedNotifsRepo> TaskManagerTask for NotificationServiceTask<NotifsToSendRepo, BlockedNotifsRepo>
where
	NotifsToSendRepo: Repository<Item = Notification> + 'static,
	BlockedNotifsRepo: Repository<Item = Notification> + 'static,
{
	async fn handle(&self) {
		let started_at = Instant::now();
		let is_notif_blocked: bool = match self.repo_blocked_notifs.get(&self.notification).await {
			Ok(_) => true,
			Err(RepositoryError::NotFound(_)) => false,
			Err(e) => {
				println!("DB Error: {:?}", e);
				return;
			}
		};

		if is_notif_blocked {
			return;
		}

		let is_sent = self.send_notification(&self.notification).await;
		if !is_sent {
			println!("Failed to send notification to a client, pushing to the database");
			let _ = self.repo_notifs_to_send.post(&self.notification).await;
		}

		self.metrics.record_latency(started_at.elapsed());
		self.metrics.dec_queue();
	}
}

pub struct NotificationService<NotifsToSendRepo, BlockedNotifsRepo, Consumer>
where
	NotifsToSendRepo: Repository<Item = Notification> + 'static,
	BlockedNotifsRepo: Repository<Item = Notification> + 'static,
	Consumer: NotificationStreamConsumer
{
	repo_notifs_to_send: Arc<NotifsToSendRepo>,
	repo_blocked_notifs: Arc<BlockedNotifsRepo>,
	consumer: Arc<Consumer>,
	clients_manager: Arc<ClientsManager>,
	task_manager: TaskManager<NotificationServiceTask<NotifsToSendRepo, BlockedNotifsRepo>>,
	metrics: Arc<LoadMetrics>,
	receiver_addr: String
}

impl<NotifsToSendRepo, BlockedNotifsRepo, Consumer>
    NotificationService<NotifsToSendRepo, BlockedNotifsRepo, Consumer>
where
    NotifsToSendRepo: Repository<Item = Notification> + 'static,
    BlockedNotifsRepo: Repository<Item = Notification> + 'static,
    Consumer: NotificationStreamConsumer,
{
	pub fn new(
		repo_notifs_to_send: Arc<NotifsToSendRepo>,
		repo_blocked_notifs: Arc<BlockedNotifsRepo>,
		consumer: Arc<Consumer>,
		clients_manager: Arc<ClientsManager>,
		task_manager: TaskManager<NotificationServiceTask<NotifsToSendRepo, BlockedNotifsRepo>>,
		receiver_addr: String
	) -> Self {
		Self {
			repo_notifs_to_send,
			repo_blocked_notifs,
			consumer,
			clients_manager,
			task_manager,
			metrics: Arc::new(LoadMetrics::new()),
			receiver_addr
		}
	}
	
	async fn connect_and_register(receiver_addr: String, public_addr: String) -> MetricsSender {
		let mut sender = MetricsSender::new(receiver_addr.clone());

		let register = Register {
			public_addr: public_addr.clone(),
			role: NodeRole::NotificationService
		};

		while let Err(e) = sender.register(register.clone()).await {
			eprintln!("Error: Failed to register the metrics sender {} with receiver {}\nInfo: {:?}", public_addr, receiver_addr, e);
			sleep(Duration::from_secs(2)).await;
		}

		sender
	}

	async fn spawn_metrics_reporter(&self) {
		let metrics = self.metrics.clone();
		let public_addr = self.clients_manager.get_addr().clone();

		let receiver_addr = self.receiver_addr.clone();
		tokio::spawn(async move {
			loop {
				let mut sender = Self::connect_and_register(
						receiver_addr.clone(),
						public_addr.clone())
					.await;

				println!("Connected to the metrics receiver {}", receiver_addr);

				loop {
					let metrics = Metrics {
						public_addr: public_addr.clone(),
						load: metrics.load()
					};
	
					let result = sender.send(Message::Metrics(metrics)).await;
					if let Err(e) = result {
						eprintln!("Error: Failed to send metrics to the receiver {}\nInfo: {:?}", receiver_addr, e);
						break;
					}

					sleep(Duration::from_secs(5)).await;
				}
			}
		});
	}

	async fn run_notification_consumer(&mut self) {
		loop {
			let notification = match self.consumer.recv().await {
				Ok(notification) => notification,
				Err(e) => {
					println!("Failed to receive a notification: {:?}", e);
					continue;
				}
			};

			let task = NotificationServiceTask::new(notification,
				self.repo_notifs_to_send.clone(),
				self.repo_blocked_notifs.clone(),
				self.clients_manager.clone(),
				self.metrics.clone());
			self.task_manager.submit(task).await;
			self.metrics.inc_queue();
		}
	}

	pub async fn start(&mut self) {
		let clients_manager_clone = self.clients_manager.clone();
		self.task_manager.start();

		tokio::spawn(async move {
			clients_manager_clone.listen().await;
		});

		self.spawn_metrics_reporter().await;
		self.run_notification_consumer().await;
	}
}