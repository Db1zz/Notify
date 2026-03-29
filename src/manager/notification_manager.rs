use std::sync::Arc;
use std::time::Instant;
use async_trait::async_trait;
use tokio::io::AsyncWriteExt;

use crate::manager::task_manager::{TaskManager, TaskManagerTask};
use crate::models::notification::Notification;
use crate::consumer::notification_stream_consumer::NotificationStreamConsumer;
use crate::repository::repository::{Repository, RepositoryError};
use crate::manager::ClientsManager;
use crate::metrics::LoadMetrics;

/*
load = 
    (queue_size * 0.5) +
    (avg_latency_ms * 0.3) +
    (failed_rate * 0.2)
*/
pub struct NotificationManagerTask<NotifsToSendRepo, BlockedNotifsRepo> {
	notification: Notification,
	repo_notifs_to_send: Arc<NotifsToSendRepo>,
	repo_blocked_notifs: Arc<BlockedNotifsRepo>,
	clients_manager: Arc<ClientsManager>,
	metrics: Arc<LoadMetrics>
}

impl<NotifsToSendRepo, BlockedNotifsRepo> NotificationManagerTask<NotifsToSendRepo, BlockedNotifsRepo> 
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
				writer.write_all(notification.sourceid.as_bytes()).await; // <- if failed
				return true;
			},
			None => {
				return false;
			}
		}
	}
}

#[async_trait]
impl<NotifsToSendRepo, BlockedNotifsRepo> TaskManagerTask for NotificationManagerTask<NotifsToSendRepo, BlockedNotifsRepo>
where
	NotifsToSendRepo: Repository<Item = Notification> + 'static,
	BlockedNotifsRepo: Repository<Item = Notification> + 'static,
{
	async fn handle(self) {
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

pub struct NotificationManager<NotifsToSendRepo, BlockedNotifsRepo, Consumer>
where
	NotifsToSendRepo: Repository<Item = Notification> + 'static,
	BlockedNotifsRepo: Repository<Item = Notification> + 'static,
	Consumer: NotificationStreamConsumer
{
	repo_notifs_to_send: Arc<NotifsToSendRepo>,
	repo_blocked_notifs: Arc<BlockedNotifsRepo>,
	consumer: Arc<Consumer>,
	clients_manager: Arc<ClientsManager>,
	task_manager: TaskManager<NotificationManagerTask<NotifsToSendRepo, BlockedNotifsRepo>>,
	metrics: Arc<LoadMetrics>,
}

impl<NotifsToSendRepo, BlockedNotifsRepo, Consumer>
    NotificationManager<NotifsToSendRepo, BlockedNotifsRepo, Consumer>
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
		task_manager: TaskManager<NotificationManagerTask<NotifsToSendRepo, BlockedNotifsRepo>>,
	) -> Self {
		Self {
			repo_notifs_to_send,
			repo_blocked_notifs,
			consumer,
			clients_manager,
			task_manager,
			metrics: Arc::new(LoadMetrics::new())
		}
	}

	pub async fn start(&mut self) {
		let clients_manager_clone = self.clients_manager.clone();
		self.task_manager.start();
		tokio::spawn(async move {
			clients_manager_clone.listen().await;
		});

		loop {
			let notification = match self.consumer.recv().await {
				Ok(notification) => notification,
				Err(e) => {
					println!("Failed to receive a notification: {:?}", e);
					continue;
				}
			};

			let task = NotificationManagerTask::new(notification,
				self.repo_notifs_to_send.clone(),
				self.repo_blocked_notifs.clone(),
				self.clients_manager.clone(),
				self.metrics.clone());
			self.task_manager.submit(task).await;
			self.metrics.inc_queue();

			println!("Avg load: {}", self.metrics.load());
		}
	}
}