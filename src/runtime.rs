//! Node runtime for [Process]es and [Maelstrom networking](https://github.com/jepsen-io/maelstrom/blob/main/doc/protocol.md#protocol)

#[cfg(test)]
use std::sync::Arc;

use async_std::channel::{bounded, Receiver, Sender};
use async_std::io::stdin;
use async_std::io::stdout;
use async_std::io::WriteExt;
use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::Serialize;
#[cfg(test)]
use serde_json::Value;
#[cfg(test)]
use tokio::spawn;
#[cfg(test)]
use tokio::test;

#[cfg(test)]
use crate::msg::Body::Client;
#[cfg(test)]
use crate::msg::Client::{Echo, EchoOk};
use crate::msg::Init::{Init, InitOk};
use crate::msg::{Body, Msg, MsgId};
use crate::process::{ProcNet, Process};
#[cfg(test)]
use crate::Error::TestIO;
use crate::Error::{Deserialize, UnexpectedMsg, IO};
#[cfg(test)]
use crate::Id;
use crate::Result;
use crate::Status;

const QUEUE_DEPTH: usize = 16;

/// Node runtime
///
/// A runtime will create, initialize and run an instance of `P`.
///
/// `M` is a node-to-node protocol message.
/// `M` is generally an enumeration of message types, or the unit type if not needed.
pub struct Runtime<M, P: Process<M>>
where
    M: DeserializeOwned + Serialize,
{
    line_io: Box<dyn LineIO + Send + Sync>,
    process: P,
    /// The process` receive queue
    process_rxq: Sender<Msg<M>>,
    /// The process` transmit queue
    process_txq: Receiver<Msg<M>>,
}

impl<M, P: Process<M>> Runtime<M, P>
where
    M: DeserializeOwned + Serialize,
{
    // Create a new runtime
    pub async fn new(args: Vec<String>, process: P) -> Result<Self> {
        Self::new_with_line_io(args, process, Box::new(StdLineIO {})).await
    }

    // Create a new runtime for testing
    #[cfg(test)]
    async fn new_for_test(
        args: Vec<String>,
        process: P,
        rxq: Receiver<String>,
        txq: Sender<String>,
    ) -> Result<Self> {
        Self::new_with_line_io(args, process, Box::new(QLineIO { rxq, txq })).await
    }

    async fn new_with_line_io(
        args: Vec<String>,
        mut process: P,
        line_io: Box<dyn LineIO + Send + Sync>,
    ) -> Result<Self> {
        let msg_id = 0;
        let (id, ids, start_msg_id) = Self::get_init(&*line_io, msg_id).await?;
        let (process_rxq, rxq) = bounded(QUEUE_DEPTH);
        let (txq, process_txq) = bounded(QUEUE_DEPTH);
        let process_net = ProcNet { txq, rxq };
        process.init(args, process_net, id, ids, start_msg_id);
        Ok(Self {
            line_io,
            process,
            process_rxq,
            process_txq,
        })
    }

    /// Run the process
    ///
    /// Run the runtime`s node process. The call will return
    /// - on encountering a fatal error, or
    /// - after [Self::shutdown] is called
    pub async fn run_process(&self) -> Status {
        self.process.run().await
    }

    /// Run IO egress until [Self::shutdown] is called
    pub async fn run_io_egress(&self) {
        while let Ok(_) = self.run_one_io_egress().await {}
    }

    async fn run_one_io_egress(&self) -> Status {
        self.send_msg(&self.process_txq.recv().await?).await?;
        Ok(())
    }

    /// Run IO ingress until [Self::shutdown] is called
    pub async fn run_io_ingress(&self) {
        while let Ok(_) = self.run_one_io_ingress().await {}
    }

    async fn run_one_io_ingress(&self) -> Status {
        self.process_rxq.send(self.recv_msg().await?).await?;
        Ok(())
    }

    /// Shutdown the runtime
    pub fn shutdown(&self) {
        self.process_rxq.close();
        self.process_txq.close();
        self.line_io.close();
    }

    /// Get initialization for the node
    ///
    /// Receives the next message and asserts it is [Init] message, responds if valid.
    ///
    /// Return the node's ID, all the participating node IDs, the next message ID to use.
    async fn get_init(
        line_io: &dyn LineIO,
        start_msg_id: MsgId,
    ) -> Result<(String, Vec<String>, MsgId)> {
        let init_data = line_io.read_line().await?;
        let Msg { src, body, .. }: Msg<()> = serde_json::from_str(&init_data)?;
        match body {
            Body::Init(Init {
                msg_id,
                node_id,
                node_ids,
            }) => {
                let rsp: Msg<()> = Msg {
                    src: node_id.clone(),
                    dest: src,
                    body: Body::Init(InitOk {
                        in_reply_to: msg_id,
                        msg_id: start_msg_id,
                    }),
                };
                let line = serde_json::to_string(&rsp)?;
                line_io.write_line(&line).await?;
                Ok((node_id, node_ids, start_msg_id + 1))
            }
            _ => Err(UnexpectedMsg { expected: "Init" }),
        }
    }

    /// Get the next message
    async fn recv_msg(&self) -> Result<Msg<M>> {
        serde_json::from_str::<Msg<M>>(&self.line_io.read_line().await?).map_err(|e| Deserialize(e))
    }

    /// Send a message
    async fn send_msg(&self, msg: &Msg<M>) -> Status {
        self.line_io.write_line(&serde_json::to_string(&msg)?).await
    }
}

/// Line IO
///
/// Line IO to send and receive message to and from the Maelstrom OS process.
/// The trait allows an implementation for testing within the local OS process.
#[async_trait]
trait LineIO {
    async fn read_line(&self) -> Result<String>;
    async fn write_line(&self, line: &str) -> Status;
    fn close(&self);
}

/// Line IO from `stdin` and `stdout`
struct StdLineIO {}

#[async_trait]
impl LineIO for StdLineIO {
    async fn read_line(&self) -> Result<String> {
        let mut line = String::new();
        stdin()
            .read_line(&mut line)
            .await
            .map(|_| line)
            .map_err(|e| IO(e))
    }

    async fn write_line(&self, line: &str) -> Status {
        if let Err(e) = stdout().write_all(line.as_bytes()).await {
            return Err(IO(e));
        }
        stdout().write_all("\n".as_bytes()).await.map_err(|e| IO(e))
    }

    fn close(&self) {
        // No op. stdin and stdout will close when Maelstrom closes its end.
    }
}

/// LineIO implementation for local OS process testing
#[cfg(test)]
struct QLineIO {
    rxq: Receiver<String>,
    txq: Sender<String>,
}

#[async_trait]
#[cfg(test)]
impl LineIO for QLineIO {
    async fn read_line(&self) -> Result<String> {
        self.rxq.recv().await.map_err(|_| TestIO)
    }

    async fn write_line(&self, line: &str) -> Status {
        self.txq.send(line.to_string()).await.map_err(|_| TestIO)
    }

    fn close(&self) {
        self.rxq.close();
        self.txq.close();
    }
}

#[cfg(test)]
struct EchoProcess {
    args: Vec<String>,
    net: ProcNet<()>,
    id: Id,
    ids: Vec<Id>,
}

#[cfg(test)]
impl Default for EchoProcess {
    fn default() -> Self {
        Self {
            args: Default::default(),
            net: Default::default(),
            id: Default::default(),
            ids: Default::default(),
        }
    }
}

#[cfg(test)]
#[async_trait]
impl Process<()> for EchoProcess {
    fn init(
        &mut self,
        args: Vec<String>,
        net: ProcNet<()>,
        id: Id,
        ids: Vec<Id>,
        _start_msg_id: MsgId,
    ) {
        self.args = args;
        self.net = net;
        self.id = id;
        self.ids = ids;
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
                                msg_id: None,
                                echo,
                            }),
                        })
                        .await?;
                }
                Err(_) => return Ok(()), // Runtime is shutting down.
                _ => panic!("unexpected message type"),
            };
        }
    }
}

#[test]
async fn test_runtime() {
    // Create the process and the communication channels
    let e = EchoProcess::default();
    let (txq, erxq) = bounded(10);
    let (etxq, rxq) = bounded(10);

    // Send the init message so it is waiting for the initializer
    let a = "a".to_string();
    let test = "test".to_string();
    let init = Msg::<()> {
        src: test.clone(),
        dest: a.clone(),
        body: Body::Init(Init {
            msg_id: 0,
            node_id: a.clone(),
            node_ids: vec![a.clone()],
        }),
    };
    txq.send(serde_json::to_string(&init).expect("serialize init"))
        .await
        .expect("send message");

    // Create and drive the runtime
    let r = Arc::new(
        Runtime::new_for_test(Default::default(), e, erxq, etxq)
            .await
            .expect("new runtime"),
    );

    // Verify process responds with init_ok
    let init_ok_data: String = rxq.recv().await.expect("recv init_ok");
    if let Msg::<()> {
        src,
        dest,
        body: Body::Init(InitOk { in_reply_to, .. }),
    } = serde_json::from_str(&init_ok_data).expect("deserialized init_ok")
    {
        assert_eq!(in_reply_to, 0);
        assert_eq!(dest, test);
        assert_eq!(src, a);
    } else {
        panic!("expected init_ok")
    }

    let r1 = r.clone();
    let r2 = r.clone();
    let r3 = r.clone();
    let t1 = spawn(async move { r1.run_io_egress().await });
    let t2 = spawn(async move { r2.run_io_ingress().await });
    let t3 = spawn(async move { r3.run_process().await });

    // Send echo requests and receive responses ...
    for msg_id in 0..5 {
        let echo_data = Value::String(format!("boo! {}", msg_id));
        let echo = Msg::<()> {
            src: test.clone(),
            dest: a.clone(),
            body: Client(Echo {
                msg_id,
                echo: echo_data.clone(),
            }),
        };
        println!("echo request:  {:?}", echo);
        txq.send(serde_json::to_string(&echo).expect("serialized"))
            .await
            .expect("sent echo request");
        let echoed: Msg<()> =
            serde_json::from_str(&rxq.recv().await.expect("response")).expect("deserialized");
        if let Msg {
            body:
                Client(Echo {
                    msg_id: in_reply_to,
                    echo: echoed_data,
                }),
            ..
        } = &echoed
        {
            assert_eq!(&msg_id, in_reply_to);
            assert_eq!(&echo_data, echoed_data);
        }
        println!(
            "echo response:      {}",
            serde_json::to_string(&echoed).expect("serialized")
        );
    }

    // Shutdown
    r.shutdown();
    let _ = tokio::join!(t1, t2, t3);
}
