#[cfg(test)]
mod tests {
    use Notify::manager::ClientsManager;
    use futures::stream;
    use serial_test::serial;
    use std::sync::Arc;
    use tokio::{
        io::AsyncWriteExt,
        net::TcpStream,
        time::{sleep, Duration, Instant},
    };
    use uuid::Uuid;

    async fn start_manager(addr: &str) -> Arc<ClientsManager> {
        let manager = Arc::new(ClientsManager::new(addr.to_string()).await);

        let listener = manager.clone();
        tokio::spawn(async move {
            listener.listen().await;
        });

        sleep(Duration::from_millis(100)).await;
        manager
    }

    async fn register_client(addr: &str, userid: Uuid) -> TcpStream {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let payload = serde_json::json!({ "userid": userid }).to_string();

        stream.write_all(payload.as_bytes()).await.unwrap();
        stream.flush().await.unwrap();

        stream
    }

    async fn wait_for_state(
        manager: &Arc<ClientsManager>,
        userid: Uuid,
        expected_present: bool,
        timeout_dur: Duration,
    ) -> bool {
        let deadline = Instant::now() + timeout_dur;

        loop {
            let is_present = manager.get_client(userid).is_some();
            if is_present == expected_present {
                return true;
            }

            if Instant::now() >= deadline {
                return false;
            }

            sleep(Duration::from_millis(25)).await;
        }
    }

    #[tokio::test]
    #[serial]
    async fn registers_client_and_returns_it_from_map() {
        let addr = "127.0.0.1:19079";
        let manager = start_manager(addr).await;

        let userid = Uuid::new_v4();
        let stream = register_client(addr, userid).await;

        assert!(
            wait_for_state(&manager, userid, true, Duration::from_secs(2)).await,
            "client should appear in DashMap"
        );

        assert!(manager.get_client(userid).is_some());

        drop(stream);
    }

    #[tokio::test]
    #[serial]
    async fn removes_client_after_unexpected_disconnect() {
        let addr = "127.0.0.1:19080";
        let manager = start_manager(addr).await;

        let userid = Uuid::new_v4();
        let stream = register_client(addr, userid).await;

        assert!(
            wait_for_state(&manager, userid, true, Duration::from_secs(2)).await,
            "client should be registered before disconnect"
        );

        drop(stream);

        assert!(
            wait_for_state(&manager, userid, false, Duration::from_secs(3)).await,
            "client should be removed after disconnect"
        );
    }

    #[tokio::test]
    #[serial]
    async fn invalid_payload_does_not_register_client() {
        let addr = "127.0.0.1:19081";
        let manager = start_manager(addr).await;

        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(b"{not valid json").await.unwrap();
        stream.flush().await.unwrap();

        sleep(Duration::from_millis(200)).await;

        assert_eq!(manager.get_clients_count(), 0);
    }

    #[tokio::test]
    #[serial]
    async fn destroy_client_removes_it_manually() {
        let addr = "127.0.0.1:19082";
        let manager = start_manager(addr).await;

        let userid = Uuid::new_v4();
        let stream = register_client(addr, userid).await;

        assert!(
            wait_for_state(&manager, userid, true, Duration::from_secs(2)).await,
            "client should be registered"
        );

        manager.destroy_client(userid);

        assert!(
            wait_for_state(&manager, userid, false, Duration::from_secs(2)).await,
            "client should be removed by destroy_client"
        );

        drop(stream);
    }

	#[tokio::test]
	async fn repeated_userid_is_an_error() {
		use tokio::io::{AsyncReadExt, AsyncWriteExt};
		use tokio::time::{timeout, Duration};

		let addr = "127.0.0.1:19100";
		let manager = ClientsManager::new(addr.to_string()).await;

		let manager = Arc::new(manager);
		let runner = manager.clone();

		tokio::spawn(async move {
			runner.listen().await;
		});

		tokio::time::sleep(std::time::Duration::from_millis(100)).await;

		let userid = Uuid::new_v4();

		let payload = serde_json::json!({ "userid": userid }).to_string();

		let mut stream1 = TcpStream::connect(addr).await.unwrap();
		stream1.write_all(payload.as_bytes()).await.unwrap();

		tokio::time::sleep(std::time::Duration::from_millis(100)).await;

		assert!(
			manager.get_client(userid).is_some(),
			"first client not registered"
		);

		let mut stream2 = TcpStream::connect(addr).await.unwrap();
		stream2.write_all(payload.as_bytes()).await.unwrap();

		let mut buf = [0u8; 1];
		match timeout(Duration::from_secs(1), stream2.read(&mut buf)).await {
			Ok(Ok(0)) => {}
			Ok(Ok(n)) => panic!("expected second socket to be closed, got {n} bytes"),
			Ok(Err(e)) => panic!("read error: {e}"),
			Err(_) => panic!("server did not close second socket in time"),
		}

		assert_eq!(manager.get_clients_count(), 1);

		drop(stream1);
		drop(stream2);
	}
}