//! Crate specific error types
use serde::Deserialize;
use serde_json::Value;

use std::fmt;

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

/// This structure contains various bits of information that are passed back from KSQL DB
/// in the event of an error _(the error is generated by KSQL DB, not the library)_.
/// This can be due to a variety of reasons such as:
///
/// - Invalid SQL
/// - An error when processing the request
/// - Malformed Data
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all(serialize = "snake_case", deserialize = "camelCase"))]
pub struct KsqlDBError {
    // Generic fields
    #[serde(rename = "@type")]
    pub response_type: String,
    pub statement_text: Option<String>,

    // Error Fields
    #[serde(rename = "error_code")] // For some reason this one field is snake-case
    pub error_code: Option<u32>,
    pub message: Option<String>,

    // Currently unsure of what is contained within this
    pub entities: Option<Vec<Value>>,
}

impl fmt::Display for KsqlDBError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Received error from KSQL DB: response type '{}', with error code [{}] and message: '{}'",
            self.response_type,
            self.error_code.unwrap_or_default(),
            self.message.clone().unwrap_or_default()
        )
    }
}
