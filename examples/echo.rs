//! Echo Server
//!
//! An echo server that can run against the
//! [Maelstrom echo workload](https://github.com/jepsen-io/maelstrom/blob/main/doc/workloads.md#workload-echo).
//!
//! You will need to
//! [install](https://github.com/jepsen-io/maelstrom/blob/main/doc/01-getting-ready/index.md#installation)
//! Maelstrom.
//!
//! To build and run the echo server with Maelstrom
//! 1. `$ git clone https://github.com/bnjmnt/async-maelstrom.git`
//! 2. `$ cd async-maelstrom/`
//! 3. `$ cargo b --example echo`
//! 4. `$ maelstrom test -w echo --bin target/release/examples/echo --time-limit 10`
use std::env;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::Arc;

use async_trait::async_trait;
use log::warn;
use tokio::spawn;

use async_maelstrom::msg::Body::Client;
use async_maelstrom::msg::Client::{Echo, EchoOk};
use async_maelstrom::msg::{Msg, MsgId};
use async_maelstrom::process::{ProcNet, Process};
use async_maelstrom::runtime::Runtime;
use async_maelstrom::{Id, Status};

/// Echo server
///
/// The server will run until the runtime shuts it down.
/// It will echo all valid echo requests, and ignore other messages.
struct EchoServer {
    args: Vec<String>,
    net: ProcNet<()>,
    id: Id,
    ids: Vec<Id>,
    msg_id: AtomicU64,
}

impl Default for EchoServer {
    fn default() -> Self {
        Self {
            args: Default::default(),
            net: Default::default(),
            id: Default::default(),
            ids: Default::default(),
            msg_id: Default::default(),
        }
    }
}

impl EchoServer {
    fn next_msg_id(&self) -> MsgId {
        self.msg_id.fetch_add(1, SeqCst)
    }
}

#[async_trait]
impl Process<()> for EchoServer {
    fn init(
        &mut self,
        args: Vec<String>,
        net: ProcNet<()>,
        id: Id,
        ids: Vec<Id>,
        start_msg_id: MsgId,
    ) {
        self.args = args;
        self.net = net;
        self.id = id;
        self.ids = ids;
        self.msg_id = AtomicU64::new(start_msg_id)
    }

    async fn run(&self) -> Status {
        loop {
            // Respond to all echo messages with an echo_ok message echoing the `echo` field
            match self.net.rxq.recv().await {
                Ok(Msg {
                    src,
                    body: Client(Echo { msg_id, echo }),
                    ..
                }) => {
                    self.net
                        .txq
                        .send(Msg {
                            src: self.id.clone(),
                            dest: src,
                            body: Client(EchoOk {
                                in_reply_to: msg_id,
                                msg_id: Some(self.next_msg_id()),
                                echo,
                            }),
                        })
                        .await?;
                }
                Err(_) => return Ok(()), // Runtime is shutting down.
                Ok(msg) => warn!("received and ignoring an unexpected message: {:?}", msg),
            };
        }
    }
}

/// Run an echo server
///
/// See module level docs for details.
#[tokio::main]
async fn main() -> Status {
    eprintln!("starting");

    // Create an echo process and a runtime to execute it
    let process: EchoServer = Default::default();
    let runtime = Arc::new(Runtime::new(env::args().collect(), process).await?);

    // Drive the runtime, and ...
    let rh1 = runtime.clone();
    let rh2 = runtime.clone();
    let rh3 = runtime.clone();
    let t1 = spawn(async move { rh1.run_io_egress().await });
    let t2 = spawn(async move { rh2.run_io_ingress().await });
    let t3 = spawn(async move { rh3.run_process().await });

    // ... wait until the Maelstrom system closes stdin and stdout
    eprintln!("running");
    let _ignored = tokio::join!(t1, t2, t3);

    eprintln!("exiting");

    Ok(())
}
