use rdkafka::message::BorrowedMessage;
use serde::de::value::Error;
use crate::types::Notification;
use crate::error::{CassandraError, JsonError, KafkaError, NotifyError};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::Consumer;
use rdkafka::consumer::{StreamConsumer};
use rdkafka::Message;
use log::warn;
use std::str::FromStr;
use std::sync::{Arc, LazyLock};
use tokio::net::{TcpListener, tcp::OwnedWriteHalf};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use uuid::Uuid;
use dashmap::DashMap;
use scylla::client::session_builder::SessionBuilder;

const MAX_WORKERS: usize = 10;
static CONNECTED_CLIENTS:LazyLock<Arc<DashMap<Uuid, OwnedWriteHalf>>> = LazyLock::new(|| Arc::new(DashMap::<Uuid, OwnedWriteHalf>::new()));

async fn send_notification(notification: &Notification) -> Result<bool, std::io::Error> {
    if let Some(mut client) = CONNECTED_CLIENTS.get_mut(&notification.receiver_id) {
        client.value_mut().write(notification.source_id.to_string().as_bytes()).await?;
        return Ok(true);
    }

    return Ok(false);
}

async fn is_notification_blocked_by_user(notification: &Notification) -> Result<bool, CassandraError> {
    let session = SessionBuilder::new()
        .known_node("127.0.0.1:9042")
        .build()
        .await?;

    let query_rows = session
        .query_unpaged(
            "SELECT userid, sourceid, type FROM notify.blocked_notifs WHERE userid = ? AND sourceid = ?",
            (notification.receiver_id, notification.source_id),
        )
        .await?
        .into_rows_result()?;

    for row in query_rows.rows()? {
        let (_userid, _sourceid, _typ): (Uuid, Uuid, String) = row?;
        return Ok(true);
    }

    return Ok(false);
}

fn parse_notification(message: &BorrowedMessage) -> Result<Notification, NotifyError> {
    let payload = message
        .payload_view::<str>()
        .ok_or(KafkaError::MissingPayload)?
        .map_err(|e| KafkaError::Utf8(e))?;

    let notification = serde_json::from_str::<Notification>(payload)
        .map_err(|e| JsonError::Parser(e))?;

    Ok(notification)
}

fn destroy_client(client_id: &Uuid) {
    let client = CONNECTED_CLIENTS.remove(&client_id);
    if let Some(pair) = client {
        pair.1.forget();
    }
}

async fn push_notification_to_database(notification: &Notification) -> Result<(), CassandraError> {
    let session = SessionBuilder::new()
        .known_node("127.0.0.1:9042")
        .build()
        .await?;

    session.query_unpaged(
        "INSERT INTO notify.notifs_to_send (userid, sourceid) VALUES (?, ?)",
        (notification.receiver_id, notification.source_id)
    )
    .await?;

    Ok(())
}

async fn worker_routine(consumer_ptr: Arc<StreamConsumer>) {
    loop {
        let notification_result = match consumer_ptr.recv().await {
            Ok(m) => parse_notification(&m),
            Err(e) => {
                warn!("Kafka error: {}", e);
                continue;
            }
        };

        let notification = match notification_result {
            Ok(n) => n,
            Err(e) => {
                println!("Error: {}", e);
                continue;
            }
        };

        if is_notification_blocked_by_user(&notification).await.unwrap() {
            println!("Notification is blocked by user!");
            continue;
        }

        match send_notification(&notification).await {
            Ok(is_sent) => {
                if !is_sent {
                    println!("Notification has been pushed to a database\n");
                    match push_notification_to_database(&notification).await {
                        Ok(ok) => {},
                        Err(e) => {
                            println!("ERROR! {}", e);
                        }
                    }
                }
            }
            Err(e) => {
                println!("Failed to send notification to a client {}", e);
                destroy_client(&notification.receiver_id);
            }
        }
    }
}

fn spawn_consumer_workers(consumer: StreamConsumer) {
    let consumer_ptr = Arc::new(consumer);

    for n in 0..MAX_WORKERS {
        println!("Worker {} has been started", n);
        let consumer_ptr_copy = consumer_ptr.clone();

        tokio::spawn(async move {
            worker_routine(consumer_ptr_copy).await;
        });
    }
}

fn client_incoming_connection_listener() {
    tokio::spawn(async move {
        let listener = TcpListener::bind("127.0.0.1:6969").await.unwrap();

        loop {
            let (socket, client_addr) = listener.accept().await.unwrap();
            let (mut reader, writer) = socket.into_split();
            let cloned_connected_clients = CONNECTED_CLIENTS.clone();
            tokio::spawn(async move {
                let mut buf = [0; 1024];

                let n = reader.read(&mut buf).await.unwrap();
                let payload = String::from_utf8_lossy(&buf[..n]);
                let json: serde_json::Value = match serde_json::from_str(&payload) {
                    Ok(v) => v,
                    Err(err) => {
                        println!("todo 1 {}", err);
                        return;
                    }
                };

                let user_id: String = match json.get("user_id").and_then(|v| v.as_str()) {
                    Some(s) => s.to_string(),
                    None => {
                        println!("Error! Expected JSON \"user_id\":\"123\"");
                        return ;
                    }
                };
    
                let uuid: Uuid = match Uuid::from_str(&user_id) {
                    Ok(u) => u, 
                    Err(err) => {
                        println!("todo 3 {}", err);
                        return;
                    }
                };

                println!("A new client connected to the server {}", client_addr);
                cloned_connected_clients.insert(uuid, writer);

            });
        }
    });
}

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

    spawn_consumer_workers(consumer);
    client_incoming_connection_listener();

    tokio::signal::ctrl_c().await
        .expect("Failed to listen ctrl + c");
    println!("Shutdown signal received, waiting for workers to exit");
}