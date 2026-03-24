use uuid::Uuid;
#[derive(thiserror::Error, Debug)]
#[error("entity {entity_id} not found")]
pub struct EntityNotFoundError {
	pub entity_id: Uuid,
}