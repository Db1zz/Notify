#[derive(thiserror::Error, Debug)]
pub enum CassandraError {
    #[error("New session error: {0}")]
    Session(#[from] scylla::errors::NewSessionError),

    #[error("Query error: {0}")]
    Query(#[from] scylla::errors::IntoRowsResultError),

    #[error("Execution error: {0}")]
    Execution(#[from] scylla::errors::ExecutionError),

	#[error("Data deserialization error: {0}")]
	Deserialization(#[from] scylla::errors::DeserializationError),

	#[error("Rows error: {0}")]
	Rows(#[from] scylla::errors::RowsError),
}

#[derive(thiserror::Error, Debug)]
pub enum KafkaError {
   	#[error("Kafka error: {0}")]
    Rdkafka(#[from] rdkafka::error::RDKafkaError),

    #[error("UTF-8 error: {0}")]
    Utf8(#[from] std::str::Utf8Error),

	#[error("Missing payload")]
    MissingPayload,
}

#[derive(thiserror::Error, Debug)]
pub enum JsonError {
	#[error("Failed to parse JSON")]
	Parser(#[from] serde_json::error::Error)
}

// #[derive(thiserror::Error, Debug)]
// pub enum IOError {
// 	#[error("failed to write data")]
// 	Write(#[from] std::io::Error)
// }

#[derive(thiserror::Error, Debug)]
pub enum NotifyError {
	#[error("Database error")]
	Cassandra(#[from] CassandraError),

	#[error("JSON error")]
	Json(#[from] JsonError),

	#[error("Kafka error")]
    Kafka(#[from] KafkaError),
}