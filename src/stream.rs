use futures_core::stream::Stream;
use futures_util::stream::StreamExt;
use reqwest::ClientBuilder;
use serde::de::DeserializeOwned;
use serde_json::json;

use std::collections::HashMap;

use super::client::USER_AGENT;
use super::{Error, KsqlDB, Result};

#[cfg(feature = "http2")]
pub use http2::*;

#[cfg(not(feature = "http2"))]
#[cfg(feature = "http1")]
pub use http1::*;

#[cfg(feature = "http2")]
mod http2 {
    use bytes::Bytes;
    use pin_project_lite::pin_project;
    use reqwest::header::CONTENT_TYPE;
    use serde_json::Value;

    use std::marker::PhantomData;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use super::*;

    impl KsqlDB {
        /// Initialises the KSQL DB Client with the provided `request::ClientBuilder`.
        ///
        /// Any authentication or common headers should be attached to the client prior to
        /// calling this method.
        pub fn new(url: String, mut builder: ClientBuilder, https_only: bool) -> Result<Self> {
            builder = builder
                .user_agent(USER_AGENT)
                .http2_prior_knowledge()
                .https_only(https_only);

            Ok(Self {
                client: builder.build()?,
                root_url: url,
                https_only,
            })
        }

        /// This method lets you stream the output records of a `SELECT` statement
        /// via HTTP/2 streams. The response is streamed back until the
        /// `LIMIT` specified in the statement is reached, or the client closes the connection.
        ///
        /// If no `LIMIT` is specified in the statement, then the response is streamed until the client closes the connection.
        ///
        /// This method requires the `http2` feature be enabled.
        ///
        /// This crate also offers a HTTP/1 compatible approach to streaming results via
        /// `Transfer-Encoding: chunked`. To enable this turn off default features and enable the
        /// `http1` feature.
        ///
        /// ## Notes
        ///
        /// - The `T` provided, must be able to directly [`serde::Deserialize`] the response, it will
        /// error if there are missing mandatory fields
        /// - In the example below, if you were to change the query to be `SELECT ID FROM
        /// EVENT_REPLAY_STREAM EMIT CHANGES`, the query would error, because all of the other fields
        /// within the struct are mandatory fields.
        ///
        /// ## Example
        ///
        /// ```no_run
        /// use futures_util::StreamExt;
        /// use reqwest::Client;
        /// use serde::Deserialize;
        ///
        /// use ksqldb::KsqlDB;
        ///
        /// #[derive(Debug, Deserialize)]
        /// struct Response {
        ///     id: String,
        ///     is_keyframe: bool,
        ///     sequence_number: u32,
        ///     events_since_keyframe: u32,
        ///     event_data: String,
        /// }
        ///
        /// #[tokio::main]
        /// async fn main() {
        ///     let ksql = KsqlDB::new("localhost:8080".into(), Client::builder(), false).unwrap();
        ///     let query = "SELECT * FROM EVENT_REPLAY_STREAM EMIT CHANGES;";
        ///
        ///     let mut stream = ksql
        ///         .query::<Response>(&query, &Default::default())
        ///         .await
        ///         .unwrap();
        ///     while let Some(r) = stream.next().await {
        ///         match r {
        ///             Ok(data) => {
        ///                 println!("{:#?}", data);
        ///             }
        ///             Err(e) => {
        ///                 eprintln!("Found Error {}", e);
        ///             }
        ///         }
        ///     }
        /// }
        /// ```
        ///
        /// [API Docs](https://docs.ksqldb.io/en/0.13.0-ksqldb/developer-guide/ksqldb-rest-api/streaming-endpoint/)
        pub async fn query<T>(
            &self,
            statement: &str,
            properties: &HashMap<String, String>,
        ) -> Result<impl Stream<Item = Result<T>>>
        where
            T: DeserializeOwned,
        {
            let url = format!("{}{}/query-stream", self.url_prefix(), self.root_url);
            let payload = json!({
                "sql": statement,
                "properties": properties
            });
            let mut response = self
                .client
                .post(&url)
                .header(CONTENT_TYPE, "application/vnd.ksqlapi.delimited.v1")
                .json(&payload)
                .send()
                .await?
                .bytes_stream();

            let columns = match response.next().await {
                Some(data) => Ok(data?),
                None => Err(Error::KSQLStream(
                    "Expected to receive data about the schema".to_string(),
                )),
            }?;
            let mut json = serde_json::from_slice::<Value>(&columns)?;
            if let Some(error_code) = json.get("error_code") {
                if let Some(error) = json.get("message") {
                    return Err(Error::KSQLStream(format!(
                        "Error code: {}, message: {}",
                        error_code, error
                    )));
                }
            }
            let schema = json["columnNames"].take();
            let columns: Vec<String> = serde_json::from_value::<Vec<String>>(schema)?
                .into_iter()
                .map(|c| c.to_lowercase())
                .collect();
            let stream: QueryStream<T, _> = QueryStream::new(response, columns);
            Ok(stream)
        }
    }

    pin_project! {
        #[derive(Default)]
        struct QueryStream<T, S>
        where
            S: Stream,
            T: DeserializeOwned,
        {
            columns: Vec<String>,
            #[pin]
            stream: S,
            _marker: PhantomData<dyn Fn() -> T>,
        }
    }

    impl<T, S> QueryStream<T, S>
    where
        T: DeserializeOwned,
        S: Stream<Item = std::result::Result<Bytes, reqwest::Error>>,
    {
        pub fn new(stream: S, columns: Vec<String>) -> Self {
            Self {
                columns,
                stream,
                _marker: PhantomData::default(),
            }
        }
    }

    impl<T, S> Stream for QueryStream<T, S>
    where
        T: DeserializeOwned,
        S: Stream<Item = std::result::Result<Bytes, reqwest::Error>>,
    {
        type Item = Result<T>;

        fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            let mut this = self.project();
            Pin::new(&mut this.stream).poll_next(cx).map(|data| {
                let data = data?;
                let data = match data {
                    Ok(data) => data,
                    Err(e) => return Some(Err(Error::from(e))),
                };
                let json = match serde_json::from_slice::<Value>(&*data) {
                    Ok(data) => data,
                    Err(e) => return Some(Err(Error::from(e))),
                };
                if json.get("error_code").is_some() {
                    if let Some(error) = json.get("message") {
                        return Some(Err(Error::KSQLStream(error.to_string())));
                    }
                }
                let arr = match json.as_array() {
                    Some(data) => data.to_owned(),
                    None => {
                        return Some(Err(Error::KSQLStream(
                            "Expected an array of column data".to_string(),
                        )))
                    }
                };
                let resp =
                    this.columns
                        .iter()
                        .zip(arr.into_iter())
                        .fold(json!({}), |mut acc, (k, v)| {
                            acc[k] = v;
                            acc
                        });
                let resp = match serde_json::from_value::<T>(resp) {
                    Ok(data) => data,
                    Err(e) => return Some(Err(Error::from(e))),
                };
                Some(Ok(resp))
            })
        }
    }
}

#[cfg(feature = "http1")]
#[cfg(not(feature = "http2"))]
mod http1 {
    use bytes::Bytes;
    use futures_core::Stream;
    use futures_util::future;
    use lazy_static::lazy_static;
    use pin_project_lite::pin_project;
    use regex::Regex;
    use serde::Deserialize;
    use serde_json::Value;

    use std::marker::PhantomData;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use super::*;

    lazy_static! {
        static ref COLUMN_REGEX: Regex =
            Regex::new(r#"`(?P<column>[a-zA-Z0-9_]+)`\w?"#).expect("failed to create column regex");
    }
    const NEW_LINE_DELIM: [u8; 1] = *b"\n";
    const NEW_LINE_COMMA_DELIM: [u8; 2] = *b",\n";

    impl KsqlDB {
        /// Initialises the KSQL DB Client with the provided `request::ClientBuilder`.
        ///
        /// Any authentication or common headers should be attached to the client prior to
        /// calling this method.
        pub fn new(url: String, mut builder: ClientBuilder, https_only: bool) -> Result<Self> {
            builder = builder.user_agent(USER_AGENT).https_only(https_only);

            Ok(Self {
                client: builder.build()?,
                root_url: url,
                https_only,
            })
        }

        /// This method lets you stream the output records of a `SELECT` statement
        /// via a chunked transfer encoding. The response is streamed back until the
        /// `LIMIT` specified in the statement is reached, or the client closes the connection.
        ///
        /// If no `LIMIT` is specified in the statement, then the response is streamed until the client closes the connection.
        ///
        /// This method requires the `http1` feature is enabled.
        ///
        /// This crate also offers a HTTP/2 compatible approach to streaming results. To enable this
        /// ensure the `http2` feature is being used.
        ///
        /// ## Notes
        ///
        /// - The `T` provided, must be able to directly [`Deserialize`] the response, it will
        /// error if there are missing mandatory fields
        /// - In the example below, if you were to change the query to be `SELECT ID FROM
        /// EVENT_REPLAY_STREAM EMIT CHANGES`, the query would error, because all of the other fields
        /// within the struct are mandatory fields.
        ///
        /// ## Example
        ///
        /// ```no_run
        /// use futures_util::StreamExt;
        /// use reqwest::Client;
        /// use serde::Deserialize;
        ///
        /// use ksqldb::KsqlDB;
        ///
        /// #[derive(Debug, Deserialize)]
        /// struct Response {
        ///     id: String,
        ///     is_keyframe: bool,
        ///     sequence_number: u32,
        ///     events_since_keyframe: u32,
        ///     event_data: String,
        /// }
        ///
        /// #[tokio::main]
        /// async fn main() {
        ///     let ksql = KsqlDB::new("localhost:8080".into(), Client::builder(), false).unwrap();
        ///     let query = "SELECT * FROM EVENT_REPLAY_STREAM EMIT CHANGES;";
        ///
        ///     let mut stream = ksql
        ///         .query::<Response>(&query, &Default::default())
        ///         .await
        ///         .unwrap();
        ///     while let Some(r) = stream.next().await {
        ///         match r {
        ///             Ok(data) => {
        ///                 println!("{:#?}", data);
        ///             }
        ///             Err(e) => {
        ///                 eprintln!("Found Error {}", e);
        ///             }
        ///         }
        ///     }
        /// }
        /// ```
        ///
        /// [API Docs](https://docs.ksqldb.io/en/0.13.0-ksqldb/developer-guide/ksqldb-rest-api/query-endpoint/)
        pub async fn query<T>(
            &self,
            query: &str,
            stream_properties: &HashMap<String, String>,
        ) -> Result<impl Stream<Item = Result<T>>>
        where
            T: DeserializeOwned,
        {
            let url = format!("{}{}/query", self.url_prefix(), self.root_url);
            let payload = json!({
                "ksql": query,
                "streamProperties": stream_properties
            });

            let mut stream = self
                .client
                .post(&url)
                .json(&payload)
                .send()
                .await?
                .bytes_stream();

            let columns = match stream.next().await {
                Some(data) => Ok(data?),
                None => Err(Error::KSQLStream(
                    "Expected to receive data about the schema".to_string(),
                )),
            }?;

            let stream = stream.filter(|x| {
                if let Ok(data) = x {
                    future::ready(**data != NEW_LINE_DELIM && **data != NEW_LINE_COMMA_DELIM)
                } else {
                    future::ready(true)
                }
            });

            // This should be the `header` for the events
            // This will contain the column information
            let mut json = serde_json::from_slice::<Value>(&columns[1..])?;
            let schema = json["header"]["schema"].take();
            let schema_str = serde_json::to_string(&schema)?;
            let captures = COLUMN_REGEX.captures_iter(&schema_str);
            let columns = captures
                .into_iter()
                .map(|c| c["column"].to_lowercase())
                .collect::<Vec<String>>();
            let stream: TransferEncodedStream<T, _> = TransferEncodedStream::new(stream, columns);
            Ok(stream)
        }
    }

    pin_project! {
        #[derive(Default)]
        struct TransferEncodedStream<T, S> where S: Stream, T: DeserializeOwned {
            columns: Vec<String>,
            #[pin]
            stream: S,
            _marker: PhantomData<dyn Fn() -> T>
        }
    }

    impl<T, S> TransferEncodedStream<T, S>
    where
        T: DeserializeOwned,
        S: Stream<Item = std::result::Result<Bytes, reqwest::Error>>,
    {
        pub fn new(stream: S, columns: Vec<String>) -> Self {
            Self {
                columns,
                stream,
                _marker: PhantomData::default(),
            }
        }
    }

    impl<T, S> Stream for TransferEncodedStream<T, S>
    where
        T: DeserializeOwned,
        S: Stream<Item = std::result::Result<Bytes, reqwest::Error>>,
    {
        type Item = Result<T>;

        fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            let mut this = self.project();
            Pin::new(&mut this.stream).poll_next(cx).map(|data| {
                let data = match data? {
                    Ok(data) => data,
                    Err(e) => return Some(Err(Error::from(e))),
                };
                let json = match serde_json::from_slice::<QueryResponse>(&*data) {
                    Ok(data) => data,
                    Err(e) => return Some(Err(Error::from(e))),
                };

                // Check to see if the stream is about to close
                if let Some(error) = json.error_message {
                    return Some(Err(Error::KSQLStream(error)));
                }
                if let Some(message) = json.final_message {
                    return Some(Err(Error::FinalMessage(message)));
                }

                // Actually process the raw data
                if let Some(data) = json.row {
                    let columns = data.columns;
                    let resp = this.columns.iter().zip(columns.into_iter()).fold(
                        json!({}),
                        |mut acc, (k, v)| {
                            acc[k] = v;
                            acc
                        },
                    );
                    let resp = match serde_json::from_value::<T>(resp) {
                        Ok(data) => data,
                        Err(e) => return Some(Err(Error::from(e))),
                    };
                    Some(Ok::<_, Error>(resp))
                } else {
                    Some(Err(Error::KSQLStream(
                        "Expected to find a row of data, however found nothing".to_string(),
                    )))
                }
            })
        }
    }

    #[derive(Deserialize)]
    #[serde(rename_all(serialize = "snake_case", deserialize = "camelCase"))]
    pub(crate) struct QueryResponse {
        pub(crate) row: Option<Column>,
        pub(crate) error_message: Option<String>,
        pub(crate) final_message: Option<String>,
    }

    #[derive(Deserialize)]
    pub(crate) struct Column {
        pub(crate) columns: Vec<Value>,
    }
}
