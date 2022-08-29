//! Node process

use async_std::channel::{bounded, Receiver, Sender};
#[allow(unused)] // For doc
use async_std::channel::{RecvError, SendError};
use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::msg::{Msg, MsgId};
#[allow(unused)] // For doc
use crate::Error;
use crate::{Id, Status};

/// The process' interface to the Maelstrom network
///
/// Parameters
/// - `W` the workload body type, e.g. [Echo](crate::msg::Echo)
/// - `A` the application body type
pub struct ProcNet<W, A>
where
    W: DeserializeOwned + Serialize,
    A: DeserializeOwned + Serialize,
{
    /// Transmit queue
    pub txq: Sender<Msg<W, A>>,
    /// Receive queue
    pub rxq: Receiver<Msg<W, A>>,
}

impl<W, A> Default for ProcNet<W, A>
where
    W: DeserializeOwned + Serialize,
    A: DeserializeOwned + Serialize,
{
    fn default() -> Self {
        let (txq, rxq) = bounded(1);
        Self { txq, rxq }
    }
}

/// Maelstrom [node process](https://github.com/jepsen-io/maelstrom/blob/main/doc/protocol.md#nodes-and-networks)
///
/// A process receives, processes and, if necessary, responds to
/// - [Maelstrom workload messages](https://github.com/jepsen-io/maelstrom/blob/main/doc/workloads.md)
/// - node-to-node messages according to the application's protocol that are delivered by the
///   [crate::runtime::Runtime] via the process's [ProcNet] instance.
///
/// Parameters
/// - `W` the workload body type, e.g. [Echo](crate::msg::Echo)
/// - `A` the application body type
#[async_trait]
pub trait Process<W, A>
where
    W: DeserializeOwned + Serialize,
    A: DeserializeOwned + Serialize,
{
    /// Create a process
    ///
    /// - `args` pass through command line args
    /// - `net` a network interface to Maelstrom
    /// - `id` this node's ID
    /// - `ids` all protocol participants' IDs
    /// - `start_msg_id` the first message ID to use. Initialization messages are handled by the
    ///   runtime, so this may be greater than 0.
    fn init(
        &mut self,
        args: Vec<String>,
        net: ProcNet<W, A>,
        id: Id,
        ids: Vec<Id>,
        start_msg_id: MsgId,
    );

    /// Run the process
    ///
    /// The call should return when the process is complete or the runtime has shutdown.
    ///
    /// Return
    /// - [Ok] IFF the process completed successfully,
    /// - [Err]:[Error::Shutdown] IFF the runtime has shutdown,
    /// - [Err] otherwise
    async fn run(&self) -> Status;
}
