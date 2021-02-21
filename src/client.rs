#[cfg(any(feature = "http2", feature = "http1"))]
use futures_core::Stream;
use reqwest::header::ACCEPT;
use reqwest::Client;
use serde::de::DeserializeOwned;
use serde_json::json;

use std::collections::HashMap;

use super::types::{
    CreateResponse, DescribeResponse, DropResponse, ExplainResponse, KsqlDBError,
    ListQueriesResponse, ListStreamsResponse, ListTablesResponse, Properties, TerminateResponse,
};
use super::{Error, Result};

/// A KSQL-DB Client, ready to make requests to the server
pub struct KsqlDB {
    pub(crate) client: Client,
    pub(crate) root_url: String,
    pub(crate) https_only: bool,
}

pub(crate) const USER_AGENT: &str = concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"));

// High level API
impl KsqlDB {
    /// Runs a `CREATE` statement
    ///
    /// ## Examples
    ///
    /// ```no_run
    /// use reqwest::Client;
    /// use ksqldb::KsqlDB;
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let ksql = KsqlDB::new("localhost:8080".into(), Client::builder(), false).unwrap();
    ///
    /// let query = r#"
    /// CREATE STREAM MY_STREAM (
    ///     id VARCHAR KEY
    /// ) WITH (
    ///     kafka_topic = 'my_topic',
    ///     partitions = 1,
    ///     value_format = 'JSON'
    /// );
    /// "#;
    ///
    /// let response = ksql.create(&query, Default::default(), None).await;
    /// # }
    /// ```
    pub async fn create(
        &self,
        statement: &str,
        stream_properties: HashMap<String, String>,
        command_sequence_number: Option<u32>,
    ) -> Result<Vec<CreateResponse>> {
        self.execute_statement::<CreateResponse>(
            statement,
            stream_properties,
            command_sequence_number,
        )
        .await
    }

    /// Runs a `DROP` statement
    ///
    /// ## Examples
    ///
    /// ```no_run
    /// use reqwest::Client;
    /// use ksqldb::KsqlDB;
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let ksql = KsqlDB::new("localhost:8080".into(), Client::builder(), false).unwrap();
    ///
    /// let query = r#"DROP TABLE MY_TABLE;"#;
    ///
    /// let response = ksql.drop(&query, Default::default(), None).await;
    /// # }
    /// ```
    pub async fn drop(
        &self,
        statement: &str,
        stream_properties: HashMap<String, String>,
        command_sequence_number: Option<u32>,
    ) -> Result<Vec<DropResponse>> {
        self.execute_statement::<DropResponse>(
            statement,
            stream_properties,
            command_sequence_number,
        )
        .await
    }

    /// Runs a `TERMINATE` statement
    ///
    /// ## Examples
    ///
    /// ```no_run
    /// use reqwest::Client;
    /// use ksqldb::KsqlDB;
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let ksql = KsqlDB::new("localhost:8080".into(), Client::builder(), false).unwrap();
    ///
    /// let query = r#"TERMINATE my_query_id;"#;
    ///
    /// let response = ksql.terminate(&query, Default::default(), None).await;
    /// # }
    /// ```
    pub async fn terminate(
        &self,
        statement: &str,
        stream_properties: HashMap<String, String>,
        command_sequence_number: Option<u32>,
    ) -> Result<Vec<TerminateResponse>> {
        self.execute_statement::<TerminateResponse>(
            statement,
            stream_properties,
            command_sequence_number,
        )
        .await
    }

    /// Runs a `SELECT` statement
    ///
    /// ## Examples
    ///
    /// ```no_run
    /// use reqwest::Client;
    /// use ksqldb::KsqlDB;
    /// use futures_util::stream::StreamExt;
    /// # use serde::Deserialize;
    ///
    /// #[derive(Debug, Deserialize)]
    /// struct MyResponse {
    ///     id: String,
    ///     data: Vec<u32>
    /// }
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let ksql = KsqlDB::new("localhost:8080".into(), Client::builder(), false).unwrap();
    ///
    /// let query = r#"SELECT * FROM MY_STREAM EMIT CHANGES;"#;
    ///
    /// let mut stream = ksql.select::<MyResponse>(&query, Default::default()).await.unwrap();
    ///
    /// while let Some(data) = stream.next().await {
    ///     println!("{:#?}", data);
    /// }
    /// # }
    /// ```
    #[cfg(any(feature = "http2", feature = "http1"))]
    pub async fn select<T>(
        &self,
        query: &str,
        stream_properties: HashMap<String, String>,
    ) -> Result<impl Stream<Item = Result<T>>>
    where
        T: DeserializeOwned,
    {
        self.query::<T>(query, stream_properties).await
    }

    /// Runs a `LIST STREAMS` or `SHOW STREAMS` statement. They both have the same
    /// response structure so this method can be used to execute either.
    ///
    /// ## Examples
    ///
    /// ```no_run
    /// use reqwest::Client;
    /// use ksqldb::KsqlDB;
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let ksql = KsqlDB::new("localhost:8080".into(), Client::builder(), false).unwrap();
    ///
    /// let query = r#"SHOW STREAMS;"#;
    ///
    /// let response = ksql.list_streams(&query, Default::default(), None).await;
    /// # }
    /// ```
    pub async fn list_streams(
        &self,
        statement: &str,
        stream_properties: HashMap<String, String>,
        command_sequence_number: Option<u32>,
    ) -> Result<Vec<ListStreamsResponse>> {
        self.execute_statement::<ListStreamsResponse>(
            statement,
            stream_properties,
            command_sequence_number,
        )
        .await
    }

    /// Runs a `LIST TABLES` or `SHOW TABLES` statement. They both have the same
    /// response structure so this method can be used to execute either.
    ///
    /// ## Examples
    ///
    /// ```no_run
    /// use reqwest::Client;
    /// use ksqldb::KsqlDB;
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let ksql = KsqlDB::new("localhost:8080".into(), Client::builder(), false).unwrap();
    ///
    /// let query = r#"SHOW TABLES EXTENDED;"#;
    ///
    /// let response = ksql.list_tables(&query, Default::default(), None).await;
    /// # }
    /// ```
    pub async fn list_tables(
        &self,
        statement: &str,
        stream_properties: HashMap<String, String>,
        command_sequence_number: Option<u32>,
    ) -> Result<Vec<ListTablesResponse>> {
        self.execute_statement::<ListTablesResponse>(
            statement,
            stream_properties,
            command_sequence_number,
        )
        .await
    }

    /// Runs a `LIST QUERIES` or `SHOW QUERIES` statement. They both have the same
    /// response structure so this method can be used to execute either.
    ///
    /// ## Examples
    ///
    /// ```no_run
    /// use reqwest::Client;
    /// use ksqldb::KsqlDB;
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let ksql = KsqlDB::new("localhost:8080".into(), Client::builder(), false).unwrap();
    ///
    /// let query = r#"SHOW QUERIES;"#;
    ///
    /// let response = ksql.list_queries(&query, Default::default(), None).await;
    /// # }
    /// ```
    pub async fn list_queries(
        &self,
        statement: &str,
        stream_properties: HashMap<String, String>,
        command_sequence_number: Option<u32>,
    ) -> Result<Vec<ListQueriesResponse>> {
        self.execute_statement::<ListQueriesResponse>(
            statement,
            stream_properties,
            command_sequence_number,
        )
        .await
    }

    /// Runs a `SHOW PROPERTIES` statement.
    ///
    /// ## Examples
    ///
    /// ```no_run
    /// use reqwest::Client;
    /// use ksqldb::KsqlDB;
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let ksql = KsqlDB::new("localhost:8080".into(), Client::builder(), false).unwrap();
    ///
    /// let query = r#"SHOW PROPERTIES;"#;
    ///
    /// let response = ksql.list_properties(&query, Default::default(), None).await;
    /// # }
    /// ```
    pub async fn list_properties(
        &self,
        statement: &str,
        stream_properties: HashMap<String, String>,
        command_sequence_number: Option<u32>,
    ) -> Result<Vec<Properties>> {
        self.execute_statement::<Properties>(statement, stream_properties, command_sequence_number)
            .await
    }

    /// Runs a `DESCRIBE (stream_name | table_name)` statement.
    ///
    /// ## Examples
    ///
    /// ```no_run
    /// use reqwest::Client;
    /// use ksqldb::KsqlDB;
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let ksql = KsqlDB::new("localhost:8080".into(), Client::builder(), false).unwrap();
    ///
    /// let query = r#"DESCRIBE EXTENDED MY_STREAM;"#;
    ///
    /// let response = ksql.describe(&query, Default::default(), None).await;
    /// # }
    /// ```
    pub async fn describe(
        &self,
        statement: &str,
        stream_properties: HashMap<String, String>,
        command_sequence_number: Option<u32>,
    ) -> Result<Vec<DescribeResponse>> {
        self.execute_statement::<DescribeResponse>(
            statement,
            stream_properties,
            command_sequence_number,
        )
        .await
    }

    /// Runs a `EXPLAIN (sql_expression | query_id)` statement.
    ///
    /// ## Examples
    ///
    /// ```no_run
    /// use reqwest::Client;
    /// use ksqldb::KsqlDB;
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let ksql = KsqlDB::new("localhost:8080".into(), Client::builder(), false).unwrap();
    ///
    /// let query = r#"EXPLAIN my_query_id;"#;
    ///
    /// let response = ksql.explain(&query, Default::default(), None).await;
    /// # }
    /// ```
    pub async fn explain(
        &self,
        statement: &str,
        stream_properties: HashMap<String, String>,
        command_sequence_number: Option<u32>,
    ) -> Result<Vec<ExplainResponse>> {
        self.execute_statement::<ExplainResponse>(
            statement,
            stream_properties,
            command_sequence_number,
        )
        .await
    }
}

// Low level API
impl KsqlDB {
    /// @TODO
    pub async fn status(&self) {
        todo!()
    }

    /// @TODO
    pub async fn info(&self) {
        todo!()
    }

    /// This is the lower level entry point to the `/ksql` endpoint.
    ///
    /// This resource runs a sequence of 1 or more `SQL` statements. All statements, except those starting with `SELECT` can be run.
    ///
    /// To run `SELECT` statements use the [`KsqlDB::query`] method.
    ///
    /// This KSQL-DB endpoint has a variable response, generally depending on the sorts
    /// of statements you're executing. It requires that you pass a type `T` to the function dicatating
    /// what you want to deserialize from the response. In the event that you're sending multiple
    /// requests which all contain different response structures it might be easier to
    /// specifiy the value as a [`serde_json::Value`] and handle the parsing on your application
    /// side.
    ///
    /// ## Examples
    ///
    /// ```no_run
    /// use reqwest::Client;
    /// use serde::Deserialize;
    /// use serde_json::Value;
    ///
    /// use ksqldb::KsqlDB;
    ///
    /// #[derive(Debug, Deserialize)]
    /// #[serde(rename_all(serialize = "snake_case", deserialize = "camelCase"))]
    /// struct StatementResponse {
    ///     // You can handle reserved keywords by renaming
    ///     // This maps the Rust key: `ident` -> JSON key: `@type`
    ///     // You can also use this to rename fields to be more meaningful for you
    ///     #[serde(rename = "@type")]
    ///     ident: String,
    ///     statement_text: String,
    ///     // If you're not entirely sure about the type, you can leave it as JSON
    ///     // Although you lose the benefits of Rust (and make it harder to extract in
    ///     // the future if you do so)
    ///     warnings: Vec<Value>,
    ///     streams: Vec<StreamData>,
    /// }
    ///
    /// // In this case we're defining our own type, alternatively you can use the
    /// // ones located in the `common::ksqldb::types` module.
    /// // Feel free to add new ones as appropriate
    /// #[derive(Debug, Deserialize)]
    /// #[serde(rename_all(serialize = "snake_case", deserialize = "camelCase"))]
    /// struct StreamData {
    ///     #[serde(rename = "type")]
    ///     data_type: String,
    ///     name: String,
    ///     topic: String,
    ///     format: String,
    /// }
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let ksql = KsqlDB::new("localhost:8088".to_string(), Client::builder(), false).unwrap();
    ///
    ///     let query = "show streams;";
    ///     let result = ksql
    ///         .execute_statement::<StatementResponse>(&query, Default::default(), None)
    ///         .await;
    /// }
    /// ```
    ///
    /// [API Docs](https://docs.ksqldb.io/en/0.13.0-ksqldb/developer-guide/ksqldb-rest-api/ksql-endpoint/)
    pub async fn execute_statement<T>(
        &self,
        statement: &str,
        stream_properties: HashMap<String, String>,
        command_sequence_number: Option<u32>,
    ) -> Result<Vec<T>>
    where
        T: DeserializeOwned,
    {
        let url = format!("{}{}/ksql", self.url_prefix(), self.root_url);
        let mut payload = json!({
            "ksql": statement,
            "streamProperties": stream_properties
        });
        if let Some(num) = command_sequence_number {
            payload["commandSequenceNumber"] = num.into();
        }
        let response: serde_json::Value = self
            .client
            .post(&url)
            .json(&payload)
            .send()
            .await?
            .json()
            .await?;
        eprintln!("{:#?}", &response);
        let has_error = response.get("error_code").is_some();
        if has_error {
            let result = serde_json::from_value::<KsqlDBError>(response)?;
            return Err(Error::KSQL(result));
        }

        let result = serde_json::from_value::<Vec<T>>(response)?;
        Ok(result)
    }

    /// Ideally you shouldn't be using this function as it just returns the raw JSON
    /// and doesn't do any error handling or response parsing, however this can be
    /// useful for when you're debugging response/error types so it will be left
    /// in for now.
    ///
    /// The only other usecase for this function would be when you are performing different
    /// SQL statements (with different response bodies) all within the same query, as we
    /// aren't able to deserialize into multiple types within the same array.
    ///
    /// The caveat to this is that by using this function the caller has to do all parsing
    /// (`Ok` or `Err`) themselves.
    pub async fn execute_statement_raw(
        &self,
        statement: &str,
        stream_properties: HashMap<String, String>,
        command_sequence_number: Option<u32>,
    ) -> Result<serde_json::Value> {
        let url = format!("{}{}/ksql", self.url_prefix(), self.root_url);
        let mut payload = json!({
            "ksql": statement,
            "streamProperties": stream_properties
        });
        if let Some(num) = command_sequence_number {
            payload["commandSequenceNumber"] = num.into();
        }
        let response = self
            .client
            .post(&url)
            .header(ACCEPT, "application/vnd.ksql.v1+json")
            .json(&payload)
            .send()
            .await?
            .json()
            .await?;
        Ok(response)
    }
}

impl KsqlDB {
    pub(crate) fn url_prefix(&self) -> &str {
        if self.https_only {
            "https://"
        } else {
            "http://"
        }
    }
}
