use std::{ops::Deref, time::Duration};
use rdkafka::{ClientConfig, producer::{FutureProducer, FutureRecord}};

use tokio::{io::AsyncReadExt, net::TcpListener};
use std::sync::Arc;

// const MAX_WORKERS: usize = 10;

// async fn spawn_producer_worker(producer: &FutureProducer, producer_addr: &str) {
// 	}
// }

pub async fn start(producer_addr: &Arc<String>, brokers: &Arc<String>, topic_name: &Arc<String>) {
    let producer: Arc<FutureProducer> = Arc::new(ClientConfig::new()
        .set("bootstrap.servers", brokers.deref())
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error"));

	let listener = TcpListener::bind(producer_addr.deref()).await.unwrap();

    loop {
		println!("Socket server started on {}", producer_addr);
        let (mut socket, client_addr) = listener.accept().await.unwrap();
		// let (mut reader, _) = socket.into_split();

		println!("Client connected: {}", client_addr);
		
		let cloned_producer = producer.clone();
		let cloned_topic_name = topic_name.clone();
		tokio::spawn(async move {
			loop {
				let mut buf = [0; 1024];
				let n = socket.read(&mut buf).await.unwrap();

				/*
					without validation for now...
					read..validate..send???
				*/

				if n == 0 { // TCP is closed...
					return;
				}

				let message = String::from_utf8_lossy(&buf[..n]).to_string();
				let _ = cloned_producer
        			.send(
						FutureRecord::to(cloned_topic_name.deref())
							.payload(message.as_bytes())
							.key(""),
						Duration::from_secs(5)
					).await;
				// send_data(&producer, &topic_name, &message).await;
			}
		});
	}

	// spawn_producer_worker(&producer,  producer_addr);
}