use serial_test::serial;
use std::time::Duration;

use notify::{app::consumer, config::ConsumerConfig};
use tokio::task::JoinHandle;

use crate::utils::start_docker_compose;

async fn catch_panic_or_timeout(timeout: Duration, handle: JoinHandle<()>) {
    match tokio::time::timeout(timeout, handle).await {
        Ok(join_result) => match join_result {
            Ok(_) => {
                return;
            }
            Err(err) if err.is_panic() => {
                panic!()
            }
            Err(_err) => {
                return;
            }
        },
        Err(_) => {
            println!("Timeout reached!");
        }
    }
}

#[tokio::test]
#[serial]
#[should_panic]
async fn test_broker_addr() {
    start_docker_compose().await;

    let config = ConsumerConfig {
        topic: "user-notifs".to_owned(),
        brokers: "zxc:zxc".to_owned(),
        notifications_to_send_database_addr: "127.0.0.1:9042".to_owned(),
        blocked_notifications_database_addr: "127.0.0.1:9042".to_owned(),
        clients_node_addr: "127.0.0.1:6969".to_owned(),
        metrics_receiver_addr: "0.0.0.0:6979".to_owned(),
    };

    let handle = tokio::spawn(consumer::start(config));
    catch_panic_or_timeout(Duration::from_secs(5), handle).await;
}

#[tokio::test]
#[serial]
#[should_panic]
pub async fn test_invalid_notifications_to_send_database_addr() {
    start_docker_compose().await;
    let config = ConsumerConfig {
        topic: "user-notifs".to_owned(),
        brokers: "localhost:9092".to_owned(),
        notifications_to_send_database_addr: "zxc:zxc".to_owned(),
        blocked_notifications_database_addr: "127.0.0.1:9042".to_owned(),
        clients_node_addr: "127.0.0.1:6969".to_owned(),
        metrics_receiver_addr: "0.0.0.0:6979".to_owned(),
    };

    let handle = tokio::spawn(consumer::start(config));
    catch_panic_or_timeout(Duration::from_secs(5), handle).await;
}

#[tokio::test]
#[serial]
#[should_panic]
pub async fn test_invalid_blocked_notifications_database_addr() {
    start_docker_compose().await;
    let config = ConsumerConfig {
        topic: "user-notifs".to_owned(),
        brokers: "localhost:9092".to_owned(),
        notifications_to_send_database_addr: "127.0.0.1:9042".to_owned(),
        blocked_notifications_database_addr: "zxc:zxc".to_owned(),
        clients_node_addr: "127.0.0.1:6969".to_owned(),
        metrics_receiver_addr: "0.0.0.0:6979".to_owned(),
    };

    let handle = tokio::spawn(consumer::start(config));
    catch_panic_or_timeout(Duration::from_secs(5), handle).await;
}

#[tokio::test]
#[serial]
#[should_panic]
pub async fn test_invalid_client_node_addr() {
    start_docker_compose().await;
    let config = ConsumerConfig {
        topic: "user-notifs".to_owned(),
        brokers: "localhost:9092".to_owned(),
        notifications_to_send_database_addr: "127.0.0.1:9042".to_owned(),
        blocked_notifications_database_addr: "127.0.0.1:9042".to_owned(),
        clients_node_addr: "zxc:zxc".to_owned(),
        metrics_receiver_addr: "0.0.0.0:6979".to_owned(),
    };

    let handle = tokio::spawn(consumer::start(config));
    catch_panic_or_timeout(Duration::from_secs(5), handle).await;
}

#[tokio::test]
#[serial]
pub async fn test_invalid_metrics_receiver_addr() {
    start_docker_compose().await;
    let config = ConsumerConfig {
        topic: "user-notifs".to_owned(),
        brokers: "localhost:9092".to_owned(),
        notifications_to_send_database_addr: "127.0.0.1:9042".to_owned(),
        blocked_notifications_database_addr: "127.0.0.1:9042".to_owned(),
        clients_node_addr: "127.0.0.1:6969".to_owned(),
        metrics_receiver_addr: "zxc:zxc".to_owned(),
    };

    let handle = tokio::spawn(consumer::start(config));

    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(!handle.is_finished(), "consumer exited too early");

    handle.abort();
}

#[tokio::test]
#[serial]
pub async fn test_valid_addresses() {
    start_docker_compose().await;
    let config = ConsumerConfig {
        topic: "user-notifs".to_owned(),
        brokers: "localhost:9092".to_owned(),
        notifications_to_send_database_addr: "127.0.0.1:9042".to_owned(),
        blocked_notifications_database_addr: "127.0.0.1:9042".to_owned(),
        clients_node_addr: "127.0.0.1:6969".to_owned(),
        metrics_receiver_addr: "0.0.0.0:6979".to_owned(),
    };

    let handle = tokio::spawn(consumer::start(config));

    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(!handle.is_finished(), "consumer exited too early");

    handle.abort();
}
