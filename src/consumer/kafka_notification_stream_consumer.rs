use std::str::Utf8Error;

use async_trait::async_trait;
use rdkafka::{Message, consumer::StreamConsumer, error::KafkaError, message::BorrowedMessage};
use tracing::{instrument, debug};

use crate::consumer::NotificationStreamConsumer;
use crate::models::notification::Notification;

pub struct KafkaNotificationStreamConsumer {
	consumer: StreamConsumer,
}

impl KafkaNotificationStreamConsumer {
	pub fn new(consumer: StreamConsumer) -> Self {
		Self { consumer }
	}

	#[instrument(skip(self, message))]
	fn parse_notification(&self, message: &BorrowedMessage) -> Result<Notification, ParseError> {
		let payload = message
			.payload_view::<str>()
			.ok_or(ParseError::MissingPayload)?
			.map_err(ParseError::Utf8)?;

		debug!(payload = payload, "Message payload");

		let notification = serde_json::from_str::<Notification>(payload)
			.map_err(|e| ParseError::Parse(e))?;

		Ok(notification)
	}
}

#[async_trait]
impl NotificationStreamConsumer for KafkaNotificationStreamConsumer {
	type ConsumerError = NotificationRecvError;

	async fn recv(&self) -> Result<Notification, Self::ConsumerError> {
		let message = self.consumer.recv().await?;
		
		let notification = self.parse_notification(&message)?;
		Ok(notification)
	}
}

#[derive(thiserror::Error, Debug)]
pub enum ParseError {
	#[error("Missing payload in JSON Body")]
	MissingPayload,

	#[error(transparent)]
	Utf8(#[from] Utf8Error),

	#[error(transparent)]
	Parse(#[from] serde_json::error::Error)
}

#[derive(thiserror::Error, Debug)]
pub enum NotificationRecvError {
	#[error(transparent)]
	Parse(#[from] ParseError),

	#[error(transparent)]
	Kafka(#[from] KafkaError),
}