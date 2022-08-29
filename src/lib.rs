//! A Rust library that allows distributed applications to work with
//! [Maelstrom](https://github.com/jepsen-io/maelstrom).
//!
//! [Maelstrom](https://github.com/jepsen-io/maelstrom) is a workbench for learning and testing
//! distributed applications.
//! It drives [workloads](https://github.com/jepsen-io/maelstrom/blob/main/doc/workloads.md) to
//! application nodes, and uses [Jepsen](https://github.com/jepsen-io/jepsen) to verify safety
//! properties.
//!
//! The library provides
//! - a `Msg` implementation for creating and parsing workload and node-to-node message according to the
//!   [Maelstrom message protocol](https://github.com/jepsen-io/maelstrom/blob/main/doc/protocol.md#messages)
//! - a `Process` trait for implementing application node processes
//! - a `Runtime` for driving processes and communicating with the
//!   [Maelstrom network](https://github.com/jepsen-io/maelstrom/blob/main/doc/protocol.md#nodes-and-networks)
//!
//! See the [echo.rs](https://github.com/bnjmnt/async-maelstrom/blob/main/examples/echo.rs) for a
//! simple  library usage example.
use std::fmt::{Debug, Display, Formatter};
use std::{error, io};

use async_std::channel::{RecvError, SendError};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;

use crate::msg::Msg;
use crate::Error::{Deserialize, Shutdown};

pub mod msg;
pub mod process;
pub mod runtime;

/// Maelstrom [node address](https://github.com/jepsen-io/maelstrom/blob/main/doc/protocol.md#messages)
pub type Id = String;

/// Maelstrom [error code](https://github.com/jepsen-io/maelstrom/blob/main/doc/protocol.md#errors)
pub type ErrorCode = u64;

/// Errors the library may return to the application
#[derive(Debug)]
pub enum Error {
    /// A message could not be deserialized
    Deserialize(serde_json::Error),
    /// Initialization of a [process::Process] failed
    Initialization(Box<dyn error::Error>),
    /// An IO operation failed
    IO(io::Error),
    /// The expected deserialized message type does not match the serialized data
    MessageType,
    /// A message could not be serialized
    Serialize(serde_json::Error),
    /// The runtime has shutdown before the process completed
    Shutdown,
    /// Testing only
    TestIO,
    /// A process received a message that was unexpected for the current state or protocol
    UnexpectedMsg { expected: &'static str },
}

unsafe impl Send for Error {}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Deserialize(e)
    }
}

impl From<RecvError> for Error {
    fn from(_: RecvError) -> Self {
        Shutdown
    }
}

impl<M: DeserializeOwned + Serialize> From<SendError<Msg<M>>> for Error {
    fn from(_: SendError<Msg<M>>) -> Self {
        Shutdown
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl error::Error for Error {}

/// Results for library functions that may fail
pub type Result<T> = std::result::Result<T, Error>;

/// Status for library functions that may fail
pub type Status = std::result::Result<(), Error>;

/// Maelstrom [Lin-kv workload key](https://github.com/jepsen-io/maelstrom/blob/main/doc/workloads.md#workload-lin-kv)
pub type Key = Value;

/// Maelstrom [Lin-kv workload value](https://github.com/jepsen-io/maelstrom/blob/main/doc/workloads.md#workload-lin-kv)
pub type Val = Value;
