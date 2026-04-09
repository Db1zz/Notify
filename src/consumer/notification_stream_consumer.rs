use async_trait::async_trait;

use crate::models::notification::Notification;

#[async_trait]
pub trait NotificationStreamConsumer: Sync + Send {
    type ConsumerError: std::error::Error + Send + Sync + 'static;

    async fn recv(&self) -> Result<Notification, Self::ConsumerError>;
}
