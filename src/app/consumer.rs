use std::sync::Arc;

use rdkafka::ClientConfig;
use rdkafka::consumer::Consumer;
use rdkafka::consumer::StreamConsumer;
use scylla::client::session_builder::SessionBuilder;

use crate::consumer::KafkaNotificationStreamConsumer;
use crate::manager::ClientsManager;
use crate::manager::task_manager::TaskManager;
use crate::repository::cassandra_repository::BlockedNotificationsCassandra;
use crate::repository::cassandra_repository::NotificationsToSendCassandra;
use crate::service::NotificationService;

pub async fn start(brokers: &str, topic_name: &str) {
    let mut config: ClientConfig = ClientConfig::new();

    config
        .set("bootstrap.servers", brokers)
        .set("session.timeout.ms", "6000")
        .set("group.id", "anteiku-consumer-group")
        .set("auto.offset.reset", "earliest");

    let consumer: StreamConsumer = config 
        .create()
        .expect("Consumer creation failed");

    consumer
        .subscribe(&[topic_name])
        .expect("Can't subscribe to specified topics");

    let repo_notifs_to_send_session = SessionBuilder::new()
        .known_node("127.0.0.1:9042")
        .build()
        .await.unwrap();

    let repo_notifs_to_send = Arc::new(NotificationsToSendCassandra::new(repo_notifs_to_send_session));

    let repo_blocked_notifs_session = SessionBuilder::new()
        .known_node("127.0.0.1:9042")
        .build()
        .await.unwrap();

    let repo_blocked_notifs = Arc::new(BlockedNotificationsCassandra::new(repo_blocked_notifs_session));

    let kafka_stream_consumer = Arc::new(KafkaNotificationStreamConsumer::new(consumer));

    let clients_manager = Arc::new(ClientsManager::new("127.0.0.1:6969".to_owned()).await);

    let task_manager = TaskManager::new(4);

    let mut notification_service = NotificationService::new(
        repo_notifs_to_send,
        repo_blocked_notifs,
        kafka_stream_consumer,
        clients_manager,
        task_manager);

    notification_service.start().await;

    tokio::signal::ctrl_c().await
        .expect("Failed to listen ctrl + c");
    println!("Shutdown signal received, waiting for workers to exit");
}