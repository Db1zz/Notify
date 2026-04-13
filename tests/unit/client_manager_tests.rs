#[cfg(test)]
mod tests {
    use futures::{SinkExt, StreamExt};
    use notify::manager::ClientsManager;
    use serial_test::serial;
    use std::sync::Arc;
    use tokio::time::{sleep, timeout, Duration, Instant};
    use tokio_tungstenite::tungstenite::Message;

    use uuid::Uuid;

    use crate::utils::{connect_to_manager, register_client};

    async fn start_manager(addr: &str) -> Arc<ClientsManager> {
        let manager = Arc::new(ClientsManager::new(addr.to_string()).await);

        let listener = manager.clone();
        tokio::spawn(async move {
            listener.listen().await;
        });

        sleep(Duration::from_millis(100)).await;
        manager
    }

    async fn wait_for_state(
        manager: &Arc<ClientsManager>,
        userid: Uuid,
        expected_present: bool,
        timeout_dur: Duration,
    ) -> bool {
        let deadline = Instant::now() + timeout_dur;

        loop {
            let result = manager.send_to_client(userid, "zxc".to_owned()).await;

            let is_present = result.is_ok();

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

        assert!(manager.is_client_connected(userid).await);

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

        let mut ws_stream = connect_to_manager(addr).await;
        let msg = Message::from("zxc");
        if let Err(e) = ws_stream.send(msg).await {
            panic!("Test error: {:?}", e);
        }

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
        let msg = Message::from(payload);

        let mut stream1 = connect_to_manager(addr).await;
        stream1.send(msg.clone()).await.unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        assert!(
            manager.is_client_connected(userid).await,
            "first client not registered"
        );

        let mut stream2 = connect_to_manager(addr).await;
        stream2.send(msg).await.unwrap();

        match timeout(Duration::from_secs(1), stream2.next()).await {
            Ok(Some(result)) => match result {
                Ok(Message::Close(_)) => {}
                Ok(other) => panic!("Expected close frame, but got: {:?}", other),
                Err(e) => {
                    println!("Got expected error/close: {}", e);
                }
            },
            Ok(None) => {}
            Err(_) => panic!("Server did not close second socket in time (timeout)"),
        }
        assert_eq!(manager.get_clients_count(), 1);

        drop(stream1);
        drop(stream2);
    }
}
