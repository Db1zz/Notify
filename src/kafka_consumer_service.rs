use serde_json;
use crate::types::Notification;
use crate::error::DbError;
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

// fn load_hashed_dp() -> HashSet<Uuid> {
//     return HashSet::new();
// }

// fn update_hashed_db() {

// }

// async fn monitor_db_updates() {
// }

async fn send_notification(notification: Notification) -> bool {
    if let Some(mut client) = CONNECTED_CLIENTS.get_mut(&notification.receiver_id) {
        let _ = client.value_mut().write(notification.source_id.to_string().as_bytes()).await; // we can unwrap it...
        return true;
    }

    return false;
}

// async fn push_notification_to_database(notification: Notification) {
// }

async fn is_notification_blocked_by_user(notification: &Notification) -> Result<bool, DbError> {
    let session = SessionBuilder::new()
        .known_node("127.0.0.1:9042")
        .build()
        .await?;

    let query_rows = session
        .query_unpaged(
            "SELECT userid, sourceid, type FROM notify.user_sources WHERE userid = ? AND sourceid = ?",
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

fn spawn_consumer_workers(consumer: StreamConsumer) {
    let consumer_ptr = Arc::new(consumer);

    for n in 0..MAX_WORKERS {
        println!("Worker {} has been started", n);
        let consumer_ptr_copy = consumer_ptr.clone();
        tokio::spawn(async move {
            loop {
                match consumer_ptr_copy.recv().await {
                    Err(e) => warn!("Kafka error: {}", e),
                    Ok(m) => {
                        let payload = match m.payload_view::<str>() {
                            None => "",
                            Some(Ok(s)) => s,
                            Some(Err(e)) => {
                                warn!("Error while deserializing message payload: {:?}", e);
                                ""
                            }
                        };

                        if payload.is_empty() {
                            println!("ZXC");
                            continue;
                        }

                        match serde_json::from_str::<Notification>(payload) {
                            Ok(notification) => {
                                if is_notification_blocked_by_user(&notification).await.unwrap() {
                                    println!("Notification is blocked by user!");
                                    continue;
                                }

                                if !send_notification(notification).await {
                                    println!("Notification has been pushed to a database\n");
                                    // push_notification_to_database(notification); TO DO...
                                }
                            }
                            Err(err) => {
                                println!("Failed to parse json: {}", err);
                            }
                        }

                        println!("Notification: {}", payload);
                    }
                }
            }
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