use std::sync::Arc;

use rdkafka::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::util::Timeout;
use scylla::client::session_builder::SessionBuilder;

use crate::config::ConsumerConfig;
use crate::consumer::KafkaNotificationStreamConsumer;
use crate::manager::ClientsManager;
use crate::manager::task_manager::TaskManager;
use crate::repository::cassandra_repository::BlockedNotificationsCassandra;
use crate::repository::cassandra_repository::NotificationsToSendCassandra;
use crate::service::NotificationService;

pub async fn start(config: ConsumerConfig) {
    let mut client_config: ClientConfig = ClientConfig::new();

    client_config
        .set("bootstrap.servers", config.brokers)
        .set("session.timeout.ms", "6000")
        .set("group.id", "anteiku-consumer-group")
        .set("auto.offset.reset", "earliest");

    let consumer: StreamConsumer = client_config 
        .create()
        .expect("Consumer creation failed");

    consumer
        .subscribe(&[&config.topic])
        .expect("Can't subscribe to specified topics");

    consumer.fetch_metadata(Some(&config.topic), std::time::Duration::from_secs(3))
        .expect("failed to fetch kafka metadata");

    let repo_notifs_to_send_session = SessionBuilder::new()
        .known_node(config.notifications_to_send_database_addr)
        .build()
        .await.unwrap();

    let repo_notifs_to_send = Arc::new(NotificationsToSendCassandra::new(repo_notifs_to_send_session));

    let repo_blocked_notifs_session = SessionBuilder::new()
        .known_node(config.blocked_notifications_database_addr)
        .build()
        .await
        .unwrap();

    let repo_blocked_notifs = Arc::new(BlockedNotificationsCassandra::new(repo_blocked_notifs_session));

    let kafka_stream_consumer = Arc::new(KafkaNotificationStreamConsumer::new(consumer));

    let clients_manager = Arc::new(ClientsManager::new(config.clients_node_addr).await);

    let task_manager = TaskManager::new(4);

    let mut notification_service = NotificationService::new(
        repo_notifs_to_send,
        repo_blocked_notifs,
        kafka_stream_consumer,
        clients_manager,
        task_manager,
        config.metrics_receiver_addr);

    notification_service.start().await;

    tokio::signal::ctrl_c().await
        .expect("Failed to listen ctrl + c");
    println!("Shutdown signal received, waiting for workers to exit");
}