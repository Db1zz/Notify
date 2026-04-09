use std::net::SocketAddr;
use std::process::Stdio;
use std::time::Duration;

use scylla::client::session_builder::SessionBuilder;
use tokio::{
    net::TcpStream,
    process::Command,
    sync::OnceCell,
    time::{sleep, timeout, Instant},
};

static DOCKER_COMPOSE: OnceCell<()> = OnceCell::const_new();

pub async fn start_docker_compose() {
    DOCKER_COMPOSE
        .get_or_init(|| async {
            Command::new("docker-compose")
                .args(["-f", "docker-compose.test.yaml", "up", "-d"])
                .stdout(Stdio::inherit())
                .stderr(Stdio::inherit())
                .status()
                .await
                .expect("docker-compose up failed");

            wait_for_kafka("127.0.0.1:9092".parse().unwrap(), Duration::from_secs(30)).await;
            wait_for_cassandra("127.0.0.1:9042").await;
        })
        .await;
}

async fn wait_for_kafka(addr: SocketAddr, timeout_dur: Duration) {
    let deadline = Instant::now() + timeout_dur;

    loop {
        let connect = timeout(Duration::from_millis(300), TcpStream::connect(addr)).await;

        if matches!(connect, Ok(Ok(_))) {
            return;
        }

        if Instant::now() >= deadline {
            panic!("Kafka did not become ready at {addr}");
        }

        sleep(Duration::from_millis(500)).await;
    }
}

async fn wait_for_cassandra(addr: &str) {
    for _ in 0..60 {
        let res = SessionBuilder::new().known_node(addr).build().await;

        if res.is_ok() {
            println!("Cassandra is up!");
            return;
        }

        println!("Waiting for Cassandra...");
        sleep(Duration::from_secs(1)).await;
    }

    panic!("Cassandra did not start");
}
