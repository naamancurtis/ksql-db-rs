use serde::Deserialize;

use std::collections::HashMap;

use super::domain::{CommandStatus, Description, Explanation, Query, RequestInfo, Stream, Table};

/// The response type for any `CREATE` KSQL-DB Statement
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all(serialize = "snake_case", deserialize = "camelCase"))]
pub struct CreateResponse {
    #[serde(flatten)]
    pub info: RequestInfo,

    // Statement specific fields
    pub command_id: Option<String>,
    pub command_status: Option<CommandStatus>,
    pub command_sequence_number: Option<i32>,
}

/// The response type for any `DROP` KSQL-DB Statement
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all(serialize = "snake_case", deserialize = "camelCase"))]
pub struct DropResponse {
    #[serde(flatten)]
    pub info: RequestInfo,

    // Statement specific fields
    pub command_id: Option<String>,
    pub command_status: Option<CommandStatus>,
    pub command_sequence_number: Option<i32>,
}

/// The response type for any `TERMINATE` KSQL-DB Statement
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all(serialize = "snake_case", deserialize = "camelCase"))]
pub struct TerminateResponse {
    #[serde(flatten)]
    pub info: RequestInfo,

    // Statement specific fields
    pub command_id: Option<String>,
    pub command_status: Option<CommandStatus>,
    pub command_sequence_number: Option<i32>,
}

/// The response type for any `LIST STREAMS` KSQL-DB Statement
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all(serialize = "snake_case", deserialize = "camelCase"))]
pub struct ListStreamsResponse {
    #[serde(flatten)]
    pub info: RequestInfo,

    // Statement specific fields
    pub source_descriptions: Vec<Stream>,
}

/// The response type for any `LIST TABLES` KSQL-DB Statement
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all(serialize = "snake_case", deserialize = "camelCase"))]
pub struct ListTablesResponse {
    #[serde(flatten)]
    pub info: RequestInfo,

    // Statement specific fields
    pub tables: Vec<Table>,
}

/// The response type for any `LIST QUERIES` KSQL-DB Statement
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all(serialize = "snake_case", deserialize = "camelCase"))]
pub struct ListQueriesResponse {
    #[serde(flatten)]
    pub info: RequestInfo,

    // Statement specific fields
    pub queries: Vec<Query>,
}

/// The response type for any `DESCRIBE` or `DESCRIBE EXTENDED` KSQL-DB Statement
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all(serialize = "snake_case", deserialize = "camelCase"))]
pub struct DescribeResponse {
    #[serde(flatten)]
    pub info: RequestInfo,

    // Statement specific fields
    pub source_description: Description,
}

/// The response type for any `EXPLAIN` KSQL-DB Statement
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all(serialize = "snake_case", deserialize = "camelCase"))]
pub struct ExplainResponse {
    #[serde(flatten)]
    pub info: RequestInfo,

    // Statement specific fields
    pub query_description: Explanation,
    pub overridden_properties: HashMap<String, String>,
}
