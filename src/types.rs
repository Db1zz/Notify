use uuid::Uuid;
use serde::Deserialize;

#[derive(Deserialize)]
pub struct Notification {
    pub receiver_id: Uuid,
    pub source_id: Uuid
}
