<div align="center">
  <h1>Rust KSQL DB</h1>

  <div>
[![crates.io](https://img.shields.io/crates/v/ksqldb?label=latest)](https://crates.io/crates/ksqldb)
[![docs](https://docs.rs/ksqldb/badge.svg)](https://docs.rs/ksqldb/latest/ksqldb/)
[![repo](https://img.shields.io/badge/github-code-black)](https://github.com/naamancurtis/ksql-db-rs)
[![Apache-2.0](https://img.shields.io/github/license/naamancurtis/ksql-db-rs)](https://github.com/naamancurtis/ksql-db-rs/blob/main/LICENSE)
  </div>

</div>

This crate is a thin wrapper around the [KSQL-DB](https://ksqldb.io/) REST API
to make interacting with the API more ergonomic for Rust projects. Under the
hood it uses [reqwest](https://docs.rs/reqwest/latest) as a HTTP client to
interact with the API.

This project is very much in the early stage and a WIP, so if there are any
features or improvements you would like made to it, please raise an issue.
Similarly all contributions are welcome.

Up until the point of a v0.2 release the project will **not** follow semver. _Ie.
subsequent `v0.1-alpha` or `v0.1-beta` releases might include breaking changes,
this is to give the library the freedom to improve the API design quickly while
still in it's early stages._ Once v0.2 is released the project will follow
semver.

## What is crate is and is not

### What the crate is

- The crate is intended to be an ergonomic way to interact with the provided REST API,
  this means useful abstractions like `futures::Stream` are already created for you
- Provide typed responses and errors instead having to handle response
  parsing in your application code
- Be fairly light weight in nature

### What this crate is not _(currently)_

- It is not a DSL, nor does it intend to do any parsing of SQL statements and
  compile time

## Quickstart

```rust
use reqwest::Client;
use ksqldb::KsqlDB;
use futures_util::stream::StreamExt;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct MyResponse {
    id: String,
    data: Vec<u32>
}

#[tokio::main]
async fn main() {
    let ksql = KsqlDB::new("localhost:8080".into(), Client::builder(), false).unwrap();

    let statement = r#"SHOW STREAMS EXTENDED;"#;
    let response = ksql.list_streams(&statement, Default::default(), None).await.unwrap();
    println!("{:#?}", response);

    let query = r#"SELECT * FROM MY_STREAM EMIT CHANGES;"#;

    let mut stream = ksql.select::<MyResponse>(&query, Default::default()).await.unwrap();

    while let Some(data) = stream.next().await {
        println!("{:#?}", data);
    }
}
```

## Docs

- [KSQL-DB](https://docs.ksqldb.io/en/0.15.0-ksqldb/reference/)

## Minimum Supported Version

- This crate will currently aim to support the latest `STABLE` release of Rust
- This crate will aim to keep up to date with the latest stable release of
  KSQL-DB _(currently v0.15)_
