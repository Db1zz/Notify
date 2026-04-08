#[cfg(test)]
mod tests {
    use Notify::manager::ClientsManager;
    use serial_test::serial;
    use tokio::{
        io::AsyncWriteExt,
        net::TcpStream,
        time::{sleep, Duration},
    };
    use uuid::Uuid;

    async fn start_manager(addr: &str) -> ClientsManager {
        let manager = ClientsManager::new(addr.to_string()).await;

        let runner = ClientsManager {
            listener: manager.listener.clone(),
            connected_clients: manager.connected_clients.clone(),
            addr: manager.addr.clone(),
        };

        tokio::spawn(async move {
            runner.listen().await;
        });

        // Даём listener'у время подняться
        sleep(Duration::from_millis(100)).await;

        manager
    }

    async fn send_payload(addr: &str, payload: &str) {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(payload.as_bytes()).await.unwrap();
        let _ = stream.shutdown().await;
    }

    async fn wait_until_client_exists(
        manager: &ClientsManager,
        id: Uuid,
        timeout_dur: Duration,
    ) -> bool {
        let start = tokio::time::Instant::now();

        while start.elapsed() < timeout_dur {
            if manager.get_client(id).is_some() {
                return true;
            }
            sleep(Duration::from_millis(50)).await;
        }

        false
    }

    #[tokio::test]
    #[serial]
    async fn registers_client_by_userid() {
        let addr = "127.0.0.1:19079";
        let manager = start_manager(addr).await;

        let user_id = Uuid::new_v4();
        let payload = format!(r#"{{"userid":"{}"}}"#, user_id) + "\n";

        send_payload(addr, &payload).await;

        let found = wait_until_client_exists(&manager, user_id, Duration::from_secs(2)).await;
        assert!(found, "client should be registered in ClientsManager");
    }

    #[tokio::test]
    #[serial]
    async fn destroy_client_removes_it_from_map() {
        let addr = "127.0.0.1:19080";
        let manager = start_manager(addr).await;

        let user_id = Uuid::new_v4();
        let payload = format!(r#"{{"userid":"{}"}}"#, user_id) + "\n";

        send_payload(addr, &payload).await;

        let found = wait_until_client_exists(&manager, user_id, Duration::from_secs(2)).await;
        assert!(found, "client should be registered before removal");

        manager.destroy_client(user_id);

        assert!(
            manager.get_client(user_id).is_none(),
            "client should be removed from ClientsManager"
        );
    }

    #[tokio::test]
    #[serial]
    async fn invalid_json_does_not_register_client() {
        let addr = "127.0.0.1:19081";
        let manager = start_manager(addr).await;

        let user_id = Uuid::new_v4();

        send_payload(addr, r#"{"bad_json": true}"#).await;

        let found = wait_until_client_exists(&manager, user_id, Duration::from_secs(500)).await;
        assert!(!found, "invalid payload must not register a client");
    }

    #[tokio::test]
    #[serial]
    async fn missing_userid_does_not_register_client() {
        let addr = "127.0.0.1:19082";
        let manager = start_manager(addr).await;

        let user_id = Uuid::new_v4();

        send_payload(addr, r#"{"something_else":"123"}"#).await;

        let found = wait_until_client_exists(&manager, user_id, Duration::from_secs(500)).await;
        assert!(!found, "payload without userid must not register a client");
    }
}