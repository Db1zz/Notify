use std::{net::{SocketAddr, TcpStream}, process::Command, time::{Duration, Instant}};

use once_cell::sync::OnceCell;

static DOCKER_COMPOSE: OnceCell<()> = OnceCell::new();

pub fn start_docker_compose() {
    DOCKER_COMPOSE.get_or_init(|| {
        Command::new("docker-compose")
            .args(&["-f", "docker-compose.test.yaml", "up", "-d"])
            .status()
            .expect("docker-compose up failed");
		wait_for_tcp("127.0.0.1:9092".parse().unwrap(), Duration::from_secs(30));
    });
}

fn wait_for_tcp(addr: SocketAddr, timeout: Duration) {
    let deadline = Instant::now() + timeout;

    while Instant::now() < deadline {
        if TcpStream::connect_timeout(&addr, Duration::from_millis(300)).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(500));
    }

    panic!("service did not become ready at {addr}");
}
