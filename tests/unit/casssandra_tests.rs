use scylla::client::session_builder::SessionBuilder;
use uuid::Uuid;
use notify::{
    models::notification::Notification,
    repository::{
        cassandra_repository::{BlockedNotificationsCassandra, NotificationsToSendCassandra},
        types::Repository,
    },
};

use crate::utils::start_docker_compose;

#[tokio::test]
async fn test_get_for_not_existing_in_blocked_notifications_table() {
    start_docker_compose().await;
    let cassandra_addr = "127.0.0.1:9042".to_owned();

    let db_seesion = SessionBuilder::new()
        .known_node(cassandra_addr)
        .build()
        .await
        .unwrap();
    let db = BlockedNotificationsCassandra::new(db_seesion);

    let sourceid = Uuid::new_v4();
    let userid = Uuid::new_v4();
    let typ = "hihi-haha".to_owned();
    let notification = Notification {
        sourceid,
        userid,
        typ,
    };

    let result = db.get(&notification).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_get_for_existing_in_blocked_notifications_table() {
    start_docker_compose().await;
    let cassandra_addr = "127.0.0.1:9042".to_owned();

    let db_seesion = SessionBuilder::new()
        .known_node(cassandra_addr)
        .build()
        .await
        .unwrap();
    let db = BlockedNotificationsCassandra::new(db_seesion);

    let sourceid = Uuid::new_v4();
    let userid = Uuid::new_v4();
    let typ = "hihi-haha".to_owned();
    let notification = Notification {
        sourceid,
        userid,
        typ,
    };

    let _ = db.post(&notification).await;

    let result = db.get(&notification).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_get_for_non_existing_in_notifications_to_send() {
    start_docker_compose().await;
    let cassandra_addr = "127.0.0.1:9042".to_owned();

    let db_seesion = SessionBuilder::new()
        .known_node(cassandra_addr)
        .build()
        .await
        .unwrap();
    let db = NotificationsToSendCassandra::new(db_seesion);

    let sourceid = Uuid::new_v4();
    let userid = Uuid::new_v4();
    let typ = "hihi-haha".to_owned();
    let notification = Notification {
        sourceid,
        userid,
        typ,
    };

    let result = db.get(&notification).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_get_for_existing_in_notifications_to_send() {
    start_docker_compose().await;
    let cassandra_addr = "127.0.0.1:9042".to_owned();

    let db_seesion = SessionBuilder::new()
        .known_node(cassandra_addr)
        .build()
        .await
        .unwrap();
    let db = BlockedNotificationsCassandra::new(db_seesion);

    let sourceid = Uuid::new_v4();
    let userid = Uuid::new_v4();
    let typ = "hihi-haha".to_owned();
    let notification = Notification {
        sourceid,
        userid,
        typ,
    };

    let _ = db.post(&notification).await;

    let result = db.get(&notification).await;
    assert!(result.is_ok());
}
