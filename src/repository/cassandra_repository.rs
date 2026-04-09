use crate::error::database::EntityNotFoundError;
use crate::models::notification::Notification;
use crate::repository::repository::{Repository, RepositoryError};
use async_trait::async_trait;
use scylla::client::session::Session;
use uuid::Uuid;

pub struct NotificationsToSendCassandra {
    session: Session,
}

impl NotificationsToSendCassandra {
    pub fn new(session: Session) -> Self {
        Self { session }
    }
}

#[async_trait]
impl Repository for NotificationsToSendCassandra {
    type Item = Notification;

    async fn post(&self, notification: &Notification) -> Result<(), RepositoryError> {
        let query = "INSERT INTO notify.notifs_to_send (userid, sourceid, type) VALUES (?, ?, ?)";

        self.session
            .query_unpaged(
                query,
                (
                    &notification.userid,
                    &notification.sourceid,
                    &notification.typ,
                ),
            )
            .await
            .map_err(|e| RepositoryError::Other(e.into()))?;
        Ok(())
    }

    async fn get(&self, key: &Notification) -> Result<Notification, RepositoryError> {
        let result: anyhow::Result<Notification> = async {
			let query = "SELECT userid, sourceid, type FROM notify.blocked_notifs WHERE userid = ? AND sourceid = ?";

			let result = self.session
				.query_unpaged(
					query,
					(key.userid, key.sourceid)
				)
				.await?;

			let rows = result.into_rows_result()?;

			for row in rows.rows()? {
				let (userid, sourceid, typ): (Uuid, Uuid, String) = row?;
				return Ok(Notification::new(userid, sourceid, typ));
			}

			Err(EntityNotFoundError {
					entity_id: key.userid
				}.into()
			)
		}.await;

        match result {
            Ok(v) => Ok(v),
            Err(e) => {
                if e.downcast_ref::<EntityNotFoundError>().is_some() {
                    Err(RepositoryError::NotFound(EntityNotFoundError {
                        entity_id: (key.userid),
                    }))
                } else {
                    Err(RepositoryError::Other(e))
                }
            }
        }
    }
}

pub struct BlockedNotificationsCassandra {
    session: Session,
}

impl BlockedNotificationsCassandra {
    pub fn new(session: Session) -> Self {
        Self { session }
    }
}

#[async_trait]
impl Repository for BlockedNotificationsCassandra {
    type Item = Notification;
    async fn post(&self, item: &Notification) -> Result<(), RepositoryError> {
        let query = "INSERT INTO notify.blocked_notifs (userid, sourceid, type) VALUES (?, ?, ?)";

        self.session
            .query_unpaged(query, (item.userid, item.sourceid, item.typ.clone()))
            .await
            .map_err(|e| RepositoryError::Other(e.into()))?;

        Ok(())
    }

    async fn get(&self, key: &Notification) -> Result<Notification, RepositoryError> {
        let result: anyhow::Result<Notification> = async {
			let query = "SELECT userid, sourceid, type FROM notify.blocked_notifs WHERE userid = ? AND sourceid = ?";

			let result = self.session
				.query_unpaged(
					query,
					(key.userid, key.sourceid)
				)
				.await?;

			let rows = result.into_rows_result()?;

			for row in rows.rows()? {
				let (userid, sourceid, typ): (Uuid, Uuid, String) = row?;
				return Ok(Notification::new(userid, sourceid, typ));
			}

			Err(EntityNotFoundError {
					entity_id: key.userid
				}.into()
			)
		}.await;

        match result {
            Ok(v) => Ok(v),
            Err(e) => {
                if e.downcast_ref::<EntityNotFoundError>().is_some() {
                    Err(RepositoryError::NotFound(EntityNotFoundError {
                        entity_id: (key.userid),
                    }))
                } else {
                    Err(RepositoryError::Other(e))
                }
            }
        }
    }
}

// #[derive(thiserror::Error, Debug)]
// pub enum CassandraGetError {
//     #[error(transparent)]
//     Execution(#[from] scylla::errors::ExecutionError),

// 	#[error(transparent)]
// 	Deserialization(#[from] scylla::errors::DeserializationError),

// 	#[error(transparent)]
// 	Rows(#[from] scylla::errors::RowsError),

// 	#[error(transparent)]
//     Query(#[from] scylla::errors::IntoRowsResultError),
// }

// #[derive(thiserror::Error, Debug)]
// pub enum CassandraPostError {
// 	#[error(transparent)]
//     Session(#[from] scylla::errors::NewSessionError),

//     #[error(transparent)]
//     Execution(#[from] scylla::errors::ExecutionError),
// }
