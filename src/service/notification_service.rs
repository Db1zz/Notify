use async_trait::async_trait;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::time::sleep;
use tracing::{debug, error, info, instrument, warn, Instrument};

use crate::consumer::notification_stream_consumer::NotificationStreamConsumer;
use crate::manager::task_manager::{TaskManager, TaskManagerTask};
use crate::manager::ClientsManager;
use crate::metrics::metrics_sender::MetricsSender;
use crate::metrics::{LoadMetrics, Message, Metrics, NodeRole, Register};
use crate::models::notification::Notification;
use crate::repository::repository::{Repository, RepositoryError};

pub struct NotificationServiceTask<NotifsToSendRepo, BlockedNotifsRepo> {
    notification: Notification,
    repo_notifs_to_send: Arc<NotifsToSendRepo>,
    repo_blocked_notifs: Arc<BlockedNotifsRepo>,
    clients_manager: Arc<ClientsManager>,
    metrics: Arc<LoadMetrics>,
}

impl<NotifsToSendRepo, BlockedNotifsRepo>
    NotificationServiceTask<NotifsToSendRepo, BlockedNotifsRepo>
where
    NotifsToSendRepo: Repository<Item = Notification> + 'static,
    BlockedNotifsRepo: Repository<Item = Notification> + 'static,
{
    pub fn new(
        notification: Notification,
        repo_notifs_to_send: Arc<NotifsToSendRepo>,
        repo_blocked_notifs: Arc<BlockedNotifsRepo>,
        clients_manager: Arc<ClientsManager>,
        metrics: Arc<LoadMetrics>,
    ) -> Self {
        Self {
            notification,
            repo_notifs_to_send,
            repo_blocked_notifs,
            clients_manager,
            metrics,
        }
    }

    #[instrument(
		skip(self, notification),
		fields(userid = %notification.userid)
	)]
    async fn send_notification(&self, notification: &Notification) -> bool {
        match self.clients_manager.get_client(notification.userid) {
            Some(client) => {
                let mut writer = client.lock().await;
                if let Err(e) = writer
                    .write_all((notification.sourceid.to_string() + "\n").as_bytes())
                    .await
                {
                    error!("Failed to write to client: {:?}", e);
                    return false;
                }
                debug!(source_id = %notification.sourceid, "Notification sent successfully");
                return true;
            }
            None => {
                warn!("Client not found in manager");
                return false;
            }
        }
    }
}

#[async_trait]
impl<NotifsToSendRepo, BlockedNotifsRepo> TaskManagerTask
    for NotificationServiceTask<NotifsToSendRepo, BlockedNotifsRepo>
where
    NotifsToSendRepo: Repository<Item = Notification> + 'static,
    BlockedNotifsRepo: Repository<Item = Notification> + 'static,
{
    #[instrument(
		skip(self),
		fields(notif_id = %self.notification.sourceid)
	)]
    async fn handle(&self) {
        let started_at = Instant::now();
        let is_notif_blocked: bool = match self.repo_blocked_notifs.get(&self.notification).await {
            Ok(_) => true,
            Err(RepositoryError::NotFound(_)) => false,
            Err(e) => {
                error!(error = ?e, "Database error checking blocked status");
                return;
            }
        };

        if is_notif_blocked {
            info!("Notification is blocked by user; skipping");
            return;
        }

        let is_sent = self.send_notification(&self.notification).await;
        if !is_sent {
            info!("Failed to send notification to a client, pushing to the database");
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
    Consumer: NotificationStreamConsumer,
{
    repo_notifs_to_send: Arc<NotifsToSendRepo>,
    repo_blocked_notifs: Arc<BlockedNotifsRepo>,
    consumer: Arc<Consumer>,
    clients_manager: Arc<ClientsManager>,
    task_manager: TaskManager<NotificationServiceTask<NotifsToSendRepo, BlockedNotifsRepo>>,
    metrics: Arc<LoadMetrics>,
    receiver_addr: String,
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
        receiver_addr: String,
    ) -> Self {
        Self {
            repo_notifs_to_send,
            repo_blocked_notifs,
            consumer,
            clients_manager,
            task_manager,
            metrics: Arc::new(LoadMetrics::new()),
            receiver_addr,
        }
    }

    #[instrument(
		skip(receiver_addr, public_addr),
		fields(receiver = %receiver_addr, public = %public_addr)
	)]
    async fn connect_and_register(receiver_addr: String, public_addr: String) -> MetricsSender {
        let mut sender = MetricsSender::new(receiver_addr);

        let register = Register {
            public_addr: public_addr.clone(),
            role: NodeRole::NotificationService,
        };

        let mut attempts = 0;

        while let Err(e) = sender.register(register.clone()).await {
            attempts += 1;
            warn!(
                error = ?e,
                attempt = attempts,
                "Registration failed, retrying in 2s..."
            );
            sleep(Duration::from_secs(2)).await;
        }

        info!("Successfully registered metrics sender");
        sender
    }

    #[instrument(
		skip(self),
		fields(receiver = %self.receiver_addr)
	)]
    async fn spawn_metrics_reporter(&self) {
        let metrics = self.metrics.clone();
        let public_addr = self.clients_manager.get_addr().clone();
        let receiver_addr = self.receiver_addr.clone();

        let worker_span = tracing::info_span!(
            "metrics_reporter",
            receiver = %receiver_addr,
            public = %public_addr
        );

        tokio::spawn(
            async move {
                info!("Metrics reporter worker started");

                loop {
                    let mut sender =
                        Self::connect_and_register(receiver_addr.clone(), public_addr.clone())
                            .await;

                    info!("Connected and registered with receiver");

                    loop {
                        let metrics = Metrics {
                            public_addr: public_addr.clone(),
                            load: metrics.load(),
                        };

                        let result = sender.send(Message::Metrics(metrics)).await;
                        if let Err(e) = result {
                            warn!(error = ?e, "Connection lost; attempting to reconnect...");
                            break;
                        }

                        sleep(Duration::from_secs(5)).await;
                    }
                }
            }
            .instrument(worker_span),
        );
    }

    #[instrument(skip(self))]
    async fn run_notification_consumer(&mut self) {
        loop {
            let notification = match self.consumer.recv().await {
                Ok(notification) => notification,
                Err(e) => {
                    warn!(error = ?e, "Failed to receive a notification");
                    continue;
                }
            };

            let task = NotificationServiceTask::new(
                notification,
                self.repo_notifs_to_send.clone(),
                self.repo_blocked_notifs.clone(),
                self.clients_manager.clone(),
                self.metrics.clone(),
            );
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
