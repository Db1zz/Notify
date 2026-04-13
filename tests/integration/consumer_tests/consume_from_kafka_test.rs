use std::{time::Duration, vec};

use futures::StreamExt;
use notify::{
    app::{self},
    config::ConsumerConfig,
    models::notification::Notification,
    repository::{cassandra_repository::BlockedNotificationsCassandra, types::Repository},
};
use rdkafka::{
    producer::{FutureProducer, FutureRecord},
    ClientConfig,
};
use scylla::client::session_builder::SessionBuilder;
use serial_test::serial;
use tokio::time::{sleep, timeout};
use tokio_tungstenite::connect_async;
use uuid::Uuid;

use crate::utils::{register_client, start_docker_compose};

async fn produce_stream_data(
    brokers: String,
    topic: String,
    client_ids: &Vec<Uuid>,
    sourceid: Uuid,
) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    for id in client_ids {
        let notification_msg = format!(
            r#"{{"userid":"{}", "sourceid":"{}", "typ":"zxc"}}"#,
            id, sourceid
        )
        .to_owned()
            + "\n";
        let _ = producer
            .send(
                FutureRecord::to(&topic)
                    .payload(notification_msg.as_bytes())
                    .key("key"),
                Duration::from_secs(5),
            )
            .await;
    }
}

async fn wait_for_consumer_to_up(raw_consumer_addr: &str) {
    let ws_addr = "ws://".to_owned() + raw_consumer_addr;
    let mut ready = false;
    for _ in 0..30 {
        if connect_async(&ws_addr).await.is_ok() {
            ready = true;
            break;
        }
        sleep(Duration::from_secs(1)).await;
    }

    if ready {
        sleep(Duration::from_millis(500)).await;
        println!("Consumer port is open and stabilized!");
    } else {
        panic!("Consumer never started!");
    }
}

async fn run_client(consumer_addr: String, client_id: Uuid) {
    let mut client = register_client(&consumer_addr, client_id).await;

    println!("Client {}: trying to receive a notification...", client_id);

    let msg_result = match timeout(Duration::from_secs(10), client.next()).await {
        Ok(Some(res)) => res,
        Ok(None) => panic!(
            "Client {}: Consumer closed connection (Stream ended)",
            client_id
        ),
        Err(_) => panic!(
            "Client {}: Failed to receive notification, timeouted!",
            client_id
        ),
    };

    match msg_result {
        Ok(msg) => {
            if msg.is_text() || msg.is_binary() {
                println!("Client {}: Notification received: {}", client_id, msg);
            } else if msg.is_close() {
                panic!(
                    "Client {}: Consumer closed connection via Close frame",
                    client_id
                );
            }
        }
        Err(e) => panic!("Client {}: WebSocket error: {:?}", client_id, e),
    }
}

#[tokio::test]
#[serial]
pub async fn test_consumer_notification_delivery_to_a_single_client() {
    start_docker_compose().await;

    let consumer_addr = "127.0.0.1:7979".to_owned();
    let brokers = "localhost:9092".to_owned();
    let topic = "user-notifs".to_owned();

    let config = ConsumerConfig {
        topic: topic.clone(),
        brokers: brokers.clone(),
        notifications_to_send_database_addr: "127.0.0.1:9042".to_owned(),
        blocked_notifications_database_addr: "127.0.0.1:9042".to_owned(),
        clients_node_addr: "0.0.0.0:7979".to_owned(),
        metrics_receiver_addr: "0.0.0.0:6979".to_owned(),
    };

    tokio::spawn(async move {
        app::consumer::start(config).await;
    });

    wait_for_consumer_to_up(&consumer_addr).await;

    let clientid = Uuid::new_v4();
    let sourceid = Uuid::new_v4();

    let client_handle = tokio::spawn(async move {
        run_client(consumer_addr, clientid).await;
    });

    sleep(Duration::from_secs(2)).await;

    produce_stream_data(brokers, topic, &vec![clientid], sourceid).await;

    let result = client_handle.await;
    assert!(result.is_ok());
}

#[tokio::test]
#[serial]
pub async fn test_consumer_notification_delivery_to_a_multiple_clients() {
    start_docker_compose().await;

    let consumer_addr = "127.0.0.1:7979".to_owned();
    let brokers = "localhost:9092".to_owned();
    let topic = "user-notifs".to_owned();

    let config = ConsumerConfig {
        topic: topic.clone(),
        brokers: brokers.clone(),
        notifications_to_send_database_addr: "127.0.0.1:9042".to_owned(),
        blocked_notifications_database_addr: "127.0.0.1:9042".to_owned(),
        clients_node_addr: "0.0.0.0:7979".to_owned(),
        metrics_receiver_addr: "0.0.0.0:6979".to_owned(),
    };

    tokio::spawn(async move {
        app::consumer::start(config).await;
    });

    wait_for_consumer_to_up(&consumer_addr).await;
    let mut handles = Vec::new();
    let mut client_ids = Vec::new();
    let amount_of_clients: usize = 5;

    for _ in 0..amount_of_clients {
        let consumer_addr = consumer_addr.clone();
        let clientid = Uuid::new_v4();

        let handle = tokio::spawn(async move {
            run_client(consumer_addr, clientid).await;
        });

        client_ids.push(clientid);
        handles.push(handle);
    }
    let sourceid = Uuid::new_v4();
    produce_stream_data(brokers, topic, &client_ids, sourceid).await;

    for handle in handles {
        let result = handle.await;
        assert!(result.is_ok());
    }
}

#[tokio::test]
#[serial]
#[should_panic]
pub async fn test_consumer_blocked_notifs_push() {
    start_docker_compose().await;

    let consumer_addr = "127.0.0.1:7979".to_owned();
    let brokers = "localhost:9092".to_owned();
    let topic = "user-notifs".to_owned();

    let cassandra_addr = "127.0.0.1:9042".to_owned();

    let config = ConsumerConfig {
        topic: topic.clone(),
        brokers: brokers.clone(),
        notifications_to_send_database_addr: cassandra_addr.clone(),
        blocked_notifications_database_addr: cassandra_addr.clone(),
        clients_node_addr: "0.0.0.0:7979".to_owned(),
        metrics_receiver_addr: "0.0.0.0:6979".to_owned(),
    };

    tokio::spawn(async move {
        app::consumer::start(config).await;
    });

    wait_for_consumer_to_up(&consumer_addr).await;

    let db_session = SessionBuilder::new()
        .known_node(cassandra_addr.clone())
        .build()
        .await
        .unwrap();
    let db = BlockedNotificationsCassandra::new(db_session);

    let userid = Uuid::new_v4();
    let sourceid = Uuid::new_v4();
    let typ = "hihi-haha".to_owned();
    let notification = Notification {
        userid,
        sourceid,
        typ,
    };

    let _ = db.post(&notification).await;

    let client_handle = tokio::spawn(async move {
        run_client(consumer_addr, userid).await;
    });

    produce_stream_data(brokers, topic, &vec![userid], sourceid).await;

    if client_handle.await.is_err() {
        panic!();
    }
}
