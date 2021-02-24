//! # Common return types from KSQL DB
//!
//! This module attempts to build up a collection of typed responses from the KSQL-DB API.
//!
//! This is a little bit tricky as the same endpoint has different response bodies depending on
//! the `STATEMENT` or `QUERY` that was executed. This makes it trickly to concretely type the
//! API so some user input might be required to enable this _currently done by specifiying `T`
//! when calling the required method_.
//!
//! This module is always going to be a moving target to try and keep the types as up to date with
//! underlying API as much as possible. If you spot something that requires an update, or
//! alternatively are using something that doesn't have a corresponding type them please raise a
//! PR.
mod domain;
mod statements;
mod util;

pub use domain::*;
pub use statements::*;
pub use util::*;
