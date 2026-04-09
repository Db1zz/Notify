use async_trait::async_trait;

use crate::error::database::EntityNotFoundError;

#[async_trait]
pub trait Repository: Send + Sync {
    type Item;

    async fn post(&self, item: &Self::Item) -> Result<(), RepositoryError>;
    async fn get(&self, key: &Self::Item) -> Result<Self::Item, RepositoryError>;
}

#[derive(thiserror::Error, Debug)]
pub enum RepositoryError {
    #[error(transparent)]
    NotFound(#[from] EntityNotFoundError),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}
