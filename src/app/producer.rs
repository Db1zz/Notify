use std::time::Duration;
use rdkafka::{ClientConfig, producer::{FutureProducer, FutureRecord}};

use tokio::{io::AsyncReadExt, net::TcpListener};
use std::sync::Arc;

use crate::config::ProducerConfig;

// TODO redesign this shit bro wtf lmao
pub async fn start(config: ProducerConfig) {
    let producer: Arc<FutureProducer> = Arc::new(ClientConfig::new()
        .set("bootstrap.servers", config.brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error"));

	let listener = TcpListener::bind(config.producer_addr.clone()).await.unwrap();
	println!("Socket server started on {}", config.producer_addr);

    loop {
        let (mut socket, client_addr) = listener.accept().await.unwrap();
		println!("Client connected: {}", client_addr);
		
		let producer = producer.clone();
		let topic = config.topic.clone();
		tokio::spawn(async move {
			loop {
				let mut buf = [0; 1024];
				let n = socket.read(&mut buf).await.unwrap();

				if n == 0 { // TCP is closed...
					return;
				}

				let message = String::from_utf8_lossy(&buf[..n]).to_string();
				let _ = producer
        			.send(
						FutureRecord::to(&topic)
							.payload(message.as_bytes())
							.key(""),
						Duration::from_secs(5)
					).await;
			}
		});
	}
}