mod types;
mod kafka_consumer_service;

// async fn produce(brokers: &str, topic_name: &str, message: &str) {
//     let producer: FutureProducer = ClientConfig::new()
//         .set("bootstrap.servers", brokers)
//         .set("message.timeout.ms", "5000")
//         .create()
//         .expect("Producer creation error");
        
//     let _ = producer
//         .send(
//             FutureRecord::to(topic_name)
//                 .payload(message.as_bytes())
//                 .key(""),
//             Duration::from_secs(5),
//         )
//         .await;
// }

// async fn producer_listener(addr: &str, brokers: &str, topic_name: &str) {
//     let listener = TcpListener::bind(addr).await.unwrap();

//     println!("Socket server started on {}", addr);

//     loop {
//         let (mut socket, cl_addr) = listener.accept().await.unwrap();
//         println!("Client connected: {}", cl_addr);

//         let b = brokers.to_string();
//         let t = topic_name.to_string();
//         tokio::spawn(async move {
//             let mut buf = [0; 1024];

//             loop {
//                 let rb: usize = socket.read(&mut buf).await.unwrap();

//                 if rb == 0 {
//                     return;
//                 }

//                 let message = String::from_utf8_lossy(&buf[..rb]).to_string();
//                 produce(&b, &t, &message).await;
//             }
//         });
//     }
// }

#[tokio::main]
async fn main() {
    let topic = "user-notifs".to_string();
    let brokers = "localhost:9092".to_string();
    // let addr = "localhost:6969".to_string();
    // tokio::join!(producer_listener(&addr, &brokers, &topic), consumer(&brokers, &topic));
    kafka_consumer_service::routine(&brokers, &topic).await;
}