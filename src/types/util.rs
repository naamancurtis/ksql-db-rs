use serde::Deserialize;

use std::fmt;

/// The stream properties which may be modified within the `stream_properties` parameter of a
/// query/statement
#[derive(Debug, PartialEq, Copy, Clone)]
#[non_exhaustive]
pub enum StreamProperties {
    ResetStreamOffset,
    /// Value needs to be a comma delimited value of `host1:port1,host2:port2...`
    KafkaBootstrapServers,
    StreamsCommitInterval,
}

impl fmt::Display for StreamProperties {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::ResetStreamOffset => write!(f, "ksql.streams.auto.offset.reset"),
            Self::KafkaBootstrapServers => write!(f, "ksql.streams.bootstrap.servers"),
            Self::StreamsCommitInterval => write!(f, "ksql.streams.commit.interval.ms"),
        }
    }
}

/// The current state of the given command
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CommandState {
    Queued,
    Parsing,
    Executing,
    Terminated,
    Success,
    Error,
}

/// The serialization format used
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Format {
    Json,
    Avro,
    Protobuf,
    Delimited,
    Kafka,
}

impl fmt::Display for Format {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::Json => write!(f, "JSON"),
            Self::Avro => write!(f, "Avro"),
            Self::Protobuf => write!(f, "Protobuf"),
            Self::Delimited => write!(f, "Delimited"),
            Self::Kafka => write!(f, "Kafka"),
        }
    }
}

/// The type of entity
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Entity {
    Stream,
    Table,
}

impl fmt::Display for Entity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::Table => write!(f, "TABLE"),
            Self::Stream => write!(f, "STREAM"),
        }
    }
}

/// The type of the given field
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum FieldType {
    Integer,
    BigInt,
    Boolean,
    Double,
    String,
    Map,
    Array,
    Struct,
}
