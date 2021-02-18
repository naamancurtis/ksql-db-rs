use serde::Deserialize;

use std::collections::HashMap;

use super::{CommandState, Entity, FieldType, Format};

/// Generic information about the request
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all(serialize = "snake_case", deserialize = "camelCase"))]
pub struct RequestInfo {
    #[serde(rename = "@type")]
    pub response_type: String,
    pub statement_text: Option<String>,
    pub warnings: Option<Vec<Warning>>,
}

/// The status of the current command being processed
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all(serialize = "snake_case", deserialize = "camelCase"))]
pub struct CommandStatus {
    pub status: CommandState,
    pub message: String,
}

/// Warnings that were returned by KSQL
#[derive(Clone, Debug, Deserialize)]
pub struct Warning {
    pub message: String,
}

/// Information about a KSQL Stream
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all(serialize = "snake_case", deserialize = "camelCase"))]
pub struct Stream {
    pub name: String,
    pub topic: String,
    pub format: Option<Format>,
    #[serde(rename = "type")]
    pub entity: Entity,
}

/// Information about a KSQL Table
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all(serialize = "snake_case", deserialize = "camelCase"))]
pub struct Table {
    pub name: String,
    pub topic: String,
    pub format: Format,
    #[serde(rename = "type")]
    pub entity: Entity,
    pub is_windowed: bool,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all(serialize = "snake_case", deserialize = "camelCase"))]
pub struct Query {
    pub query_string: String,
    pub sinks: String,
    pub id: String,
}

/// Information about the properties set on the instance
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all(serialize = "snake_case", deserialize = "camelCase"))]
pub struct Properties {
    pub properties: Vec<Property>,
    pub overwritten_properties: Vec<Property>,
    pub default_properties: Vec<String>,
}

/// Information about a specific property in the instance
#[derive(Clone, Debug, Deserialize)]
pub struct Property {
    pub name: String,
    pub scope: String,
    pub value: Option<String>,
}

/// Information about the entity
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all(serialize = "snake_case", deserialize = "camelCase"))]
pub struct Description {
    pub name: String,
    #[serde(rename = "type")]
    pub entity: Entity,
    pub timestamp: String,
    pub format: Option<Format>,
    pub topic: String,
    pub read_queries: Vec<String>,
    pub write_queries: Vec<String>,
    pub fields: Vec<Field>,
    pub extended: bool,
    pub window_type: Option<String>,
    pub key_format: Option<Format>,
    pub value_format: Option<Format>,
    pub key: Option<String>,
    pub statement: Option<String>,

    // Extended only fields
    pub statistics: Option<String>,
    pub error_stats: Option<String>,
    pub replication: Option<u32>,
    pub partitions: Option<u32>,
}

/// Information about the expression or query
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all(serialize = "snake_case", deserialize = "camelCase"))]
pub struct Explanation {
    pub statement_text: String,
    pub fields: Vec<Field>,
    pub sources: Vec<String>,
    pub sinks: Vec<String>,
    pub execution_plan: String,
    pub topology: String,
}

/// Information about the field
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all(serialize = "snake_case", deserialize = "camelCase"))]
pub struct Field {
    pub name: String,
    pub schema: Schema,

    #[serde(rename = "type")]
    pub field_type: Option<String>,
}

/// Information about the schema
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all(serialize = "snake_case", deserialize = "camelCase"))]
pub struct Schema {
    #[serde(rename = "type")]
    pub field_type: FieldType,
    /// For `MAP` and `ARRAY` types, contains the schema of the map values and array elements,
    /// respectively. For other types this field is not used and its value is `None`.
    pub member_schema: Option<HashMap<String, Schema>>,
    /// For STRUCT types, contains a list of field objects that describes each field within
    /// the struct. For other types this field is not used and its value is `None`.
    pub fields: Option<Vec<Field>>,
}
