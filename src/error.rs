#[derive(thiserror::Error, Debug)]
pub enum DbError {
    #[error("new session error: {0}")]
    Session(#[from] scylla::errors::NewSessionError),

    #[error("query error: {0}")]
    Query(#[from] scylla::errors::IntoRowsResultError),

    #[error("execution error: {0}")]
    Execution(#[from] scylla::errors::ExecutionError),

	#[error("data deserialization error: {0}")]
	Deserialization(#[from] scylla::errors::DeserializationError),

	#[error("rows error: {0}")]
	Rows(#[from] scylla::errors::RowsError),
}

// #[derive(thiserror::Error, Debug)]
// pub enum JsonError {

// }