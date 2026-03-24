use std::sync::Arc;

use tokio::io::AsyncWriteExt;

use crate::models::notification::{self, Notification};
use crate::consumer::notification_stream_consumer::NotificationStreamConsumer;
use crate::repository::repository::{Repository, RepositoryError};
use crate::manager::{ClientsManager};

pub struct NotificationManager<NotifsToSendRepo, BlockedNotifsRepo, Consumer>
where
	NotifsToSendRepo: Repository<Item = Notification>,
	BlockedNotifsRepo: Repository<Item = Notification>,
	Consumer: NotificationStreamConsumer
{
	repo_notifs_to_send: Arc<NotifsToSendRepo>,
	repo_blocked_notifs: Arc<BlockedNotifsRepo>,
	consumer: Arc<Consumer>,
	clients_manager: Arc<ClientsManager>,
}

impl<NotifsToSendRepo, BlockedNotifsRepo, Consumer>
    NotificationManager<NotifsToSendRepo, BlockedNotifsRepo, Consumer>
where
    NotifsToSendRepo: Repository<Item = Notification>,
    BlockedNotifsRepo: Repository<Item = Notification>,
    Consumer: NotificationStreamConsumer,
{
	pub fn new(
		repo_notifs_to_send: Arc<NotifsToSendRepo>,
		repo_blocked_notifs: Arc<BlockedNotifsRepo>,
		consumer: Arc<Consumer>,
		clients_manager: Arc<ClientsManager>
	) -> Self {

		Self {
			repo_notifs_to_send,
			repo_blocked_notifs,
			consumer,
			clients_manager
		}
	}

	async fn send_notification(&self, notification: &Notification) -> bool {
		match self.clients_manager.get_client(notification.userid) {
			Some(client) => {
				// TODO: some error handling
				let mut writer = client.lock().await;
				writer.write_all(notification.sourceid.as_bytes());
				return true;
			},
			None => {
				return false;
			}

		}
	}

	pub async fn start(&self) {
		let clients_manager_clone = self.clients_manager.clone();
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

			let is_notif_blocked: bool = match self.repo_blocked_notifs.get(&notification).await {
				Ok(_) => true,
				Err(RepositoryError::NotFound(_)) => false,
				Err(e) => {
					println!("DB Error: {:?}", e);
					continue;
				}
			};

			if is_notif_blocked {
				continue;
			}

			let is_sent = self.send_notification(&notification).await;
			if !is_sent {
				let _ = self.repo_notifs_to_send.post(&notification).await;
			}
		}
	}
}

#[derive(thiserror::Error, Debug)]
pub enum NotificationManagerError {

}