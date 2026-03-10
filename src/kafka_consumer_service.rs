// use serde_json;

use crate::types::Notification;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{StreamConsumer, Consumer};
use rdkafka::Message;
use log::warn;
use std::sync::Arc;

const MAX_WORKERS: usize = 10;

// fn load_hashed_dp() -> HashSet<Uuid> {

//     return HashSet::new();
// }

// fn update_hashed_db() {

// }

// async fn monitor_db_updates() {
// }

async fn send_notification(_notification: Notification) -> bool {
    return true;
}

// async fn push_notification_to_database(notification: Notification) {

// }

fn spawn_workers(consumer: StreamConsumer) {
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
                        println!("Message: {}", payload);
                        // match serde_json::from_str::<Notification>(payload) {
                        //     Ok(notification) => {
                        //         if !send_notification(notification).await {
                        //             // push_notification_to_database(notification); TO DO...
                        //         }
                        //     }
                        //     Err(err) => {
                        //         println!("Failed to parse json: {}", err);
                        //     }
                        // }
                    }
                }
            }
        });
    }
}

pub async fn routine(brokers: &str, topic_name: &str) {
    // let mut hashed_db: Arc<RwLock<HashSet<Uuid>>> = Arc::new(RwLock::new(load_hashed_dp()));
    let mut config: ClientConfig = ClientConfig::new();

    config
        .set("bootstrap.servers", brokers)
        .set("session.timeout.ms", "6000")
        .set("group.id", "anteiku-consumer-group")
        .set("auto.offset.reset", "earliest");
        // .set("enable.auto.commit", "true")

    let consumer: StreamConsumer = config
        .create()
        .expect("Consumer creation failed");

    consumer
        .subscribe(&[topic_name])
        .expect("Can't subscribe to specified topics");

    spawn_workers(consumer);
    tokio::signal::ctrl_c().await
        .expect("Failed to listen ctrl + c");
    println!("Shutdown signal received, waiting for workers to exit");
}