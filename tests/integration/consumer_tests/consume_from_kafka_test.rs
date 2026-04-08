use std::{time::Duration, vec};

use Notify::{app, config::ConsumerConfig, models::notification::Notification, repository::{cassandra_repository::BlockedNotificationsCassandra, repository::Repository}};
use rdkafka::{ClientConfig, message::ToBytes, producer::{FutureProducer, FutureRecord}};
use scylla::client::session_builder::SessionBuilder;
use serial_test::serial;
use tokio::{io::{AsyncBufReadExt, AsyncWriteExt, BufReader}, net::TcpStream, time::{sleep, timeout}};
use uuid::Uuid;

use crate::utils::start_docker_compose;

async fn produce_stream_data(brokers: String, topic: String, client_ids: &Vec<Uuid>, sourceid: Uuid) {
	let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

	for id in client_ids {
		let notification_msg = format!(r#"{{"userid":"{}", "sourceid":"{}", "typ":"zxc"}}"#, id, sourceid).to_owned() + "\n";
		let _ = producer
		.send(FutureRecord::to(&topic)
			.payload(notification_msg.as_bytes())
			.key("key"),
			Duration::from_secs(5))
		.await;
	}
}

async fn wait_for_consumer_to_up(addr: &str) {
	for _ in 0..30 {
		let stream = TcpStream::connect(addr).await;
		if stream.is_ok() {
			println!("test addr: {}", stream.unwrap().local_addr().unwrap());
			return;
		}
		sleep(Duration::from_secs(1)).await;
		println!("Trying to connect to the host: {}", addr);
	}
	println!("Consumer is up!");
}

async fn run_client(consumer_addr: String, client_id: Uuid) {
    let mut stream = TcpStream::connect(consumer_addr).await.unwrap();
	let register_msg = format!(r#"{{"userid":"{}"}}"#, client_id) + "\n";
	let _ = stream.write(register_msg.to_bytes()).await;

    let mut reader = BufReader::new(stream);
    let mut buf = String::new();

	println!("trying to receive a notification...");
	match timeout(Duration::from_secs(5), reader.read_line(&mut buf)).await {
		Ok(bytes) => {
			if bytes.unwrap() == 0 {
				panic!("Consumer closed his connection with the client");
			}
		}
		Err(_) => panic!("Failed to receive a user notification, timeouted!")
	}

	println!("Notification: {}", buf);
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
		metrics_receiver_addr: "0.0.0.0:6979".to_owned()
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
		metrics_receiver_addr: "0.0.0.0:6979".to_owned()
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
		
		let handle =  tokio::spawn(async move {
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
		metrics_receiver_addr: "0.0.0.0:6979".to_owned()
	};

	tokio::spawn(async move {
		app::consumer::start(config).await;
	});

	wait_for_consumer_to_up(&consumer_addr).await;

	let db_session = SessionBuilder::new()
        .known_node(cassandra_addr.clone())
        .build()
        .await.unwrap();
	let db = BlockedNotificationsCassandra::new(db_session);

	let userid = Uuid::new_v4();
	let sourceid = Uuid::new_v4();
	let typ = "hihi-haha".to_owned();
	let notification = Notification{userid, sourceid, typ};

	let _ = db.post(&notification).await;

	let client_handle = tokio::spawn(async move {
		run_client(consumer_addr, userid).await;
	});

	produce_stream_data(brokers, topic, &vec![userid], sourceid).await;

	if client_handle.await.is_err() {
		panic!();
	}
}
