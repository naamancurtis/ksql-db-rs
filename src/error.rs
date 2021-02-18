use super::types::KsqlDBError;

/// The error type for this library, it includes the wrapping of errors returned from KSQL DB
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An error that is returned from the HTTP Client
    #[error(transparent)]
    Http(#[from] reqwest::Error),

    /// An error that occurs while manipulating JSON data within the library
    #[error(transparent)]
    Json(#[from] serde_json::Error),

    /// An error that occurs while processing a stream of data from KSQL DB
    #[error("Recieved error: {0}")]
    KSQLStream(String),

    /// The final message received from KSQL DB before closing a stream
    #[error("Received final message: {0}")]
    FinalMessage(String),

    /// An error returned directly from KSQL DB
    #[error("{0}")]
    KSQL(KsqlDBError),
}
