//! # KSQL DB
//!
//! This crate is a thin wrapper around the [KSQL-DB](https://ksqldb.io/) REST API
//! to make interacting with the API more ergonomic for Rust projects. Under the
//! hood it uses [reqwest](https://docs.rs/reqwest/latest) as a HTTP client to
//! interact with the API.
//!
//! ## Quickstart
//!
//! ```no_run
//! use reqwest::Client;
//! use ksqldb::KsqlDB;
//! use futures_util::stream::StreamExt;
//! use serde::Deserialize;
//!
//! #[derive(Debug, Deserialize)]
//! struct MyResponse {
//!     id: String,
//!     data: Vec<u32>
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     let ksql = KsqlDB::new("localhost:8080".into(), Client::builder(), false).unwrap();
//!
//!     let statement = r#"SHOW STREAMS EXTENDED;"#;
//!     let response = ksql.list_streams(&statement, Default::default(), None).await.unwrap();
//!     println!("{:#?}", response);
//!
//!     let query = r#"SELECT * FROM MY_STREAM EMIT CHANGES;"#;
//!
//!     let mut stream = ksql.select::<MyResponse>(&query, Default::default()).await.unwrap();
//!
//!     while let Some(data) = stream.next().await {
//!         println!("{:#?}", data);
//!     }
//! }
//! ```
mod client;
mod error;
pub(crate) mod stream;
pub mod types;

pub use client::KsqlDB;
pub use error::Error;

/// The result type for this library
pub type Result<T> = std::result::Result<T, Error>;
