//! Maelstrom [network message protocol](https://github.com/jepsen-io/maelstrom/blob/main/doc/protocol.md#messages)
//!
//! A message Maelstrom workload client message can be created as follows
//! ```no_compile_
//! use async_maelstrom::msg::Msg;
//! use async_maelstrom::msg::Body::Echo;
//!
//! // Receive an echo request
//! let request = recv();
//! if let Msg {
//!     src: client_id,
//!     body: Client(Echo {msg_id, echo}),
//!     ..
//! } = request {
//!     // Create an echo response
//!     let node_id = "n1".to_string();
//!     let response: Msg<()> = Msg {
//!         src: node_id,
//!         dest: client_id,
//!         body: Echo(EchoOk {
//!             in_reply_to: msg_id,
//!             msg_id: Some(5),
//!             echo,
//!     })};
//!     send(response);
//! }
//! ```
use std::fmt::Debug;

#[cfg(test)]
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
#[cfg(test)]
use serde_json::json;
use serde_json::Value;

#[cfg(test)]
use crate::msg::Body::Application;
#[cfg(test)]
use crate::msg::Body::Workload;
use crate::{ErrorCode, Id};

/// Maelstrom network [message](https://github.com/jepsen-io/maelstrom/blob/main/doc/protocol.md#messages)
///
/// A message envelope containing
/// - source node identifier,
/// - destination node identifier,
/// - and body
///
/// Maelstrom defined bodies have a `type` field. Inter node message may have a `type` field,
/// populated with their specified message type value.
///
/// Messages are parameterized on a workload body type, `W`, and an application body type `A`.
/// The workload parameter is required to disambiguate Maelstrom messages when deserializing into the
/// target Rust type.
///
/// Parameters
/// - `W` the workload body type, e.g. [Echo]
/// - `A` the application body type
#[derive(Deserialize, Serialize, Debug, Eq, PartialEq)]
pub struct Msg<W, A> {
    pub src: Id,
    pub dest: Id,
    pub body: Body<W, A>,
}

#[derive(Deserialize, Serialize, Debug, Eq, PartialEq)]
#[serde(untagged)]
pub enum Body<W, A> {
    /// An application defined node-to-node message
    ///
    /// From the Maelstrom [message documentation](https://github.com/jepsen-io/maelstrom/blob/main/doc/protocol.md#messages)
    /// > Messages exchanged between your server nodes may have any body
    ///   structure you like; you are not limited to request-response, and may
    ///   invent any message semantics you choose. If some of your messages do
    ///   use the body format described above, Maelstrom can help generate useful
    ///   visualizations and statistics for those messages.
    Application(A),
    Error(Error),
    Init(Init),
    Workload(W),
}

/// Maelstrom [client message body](https://github.com/jepsen-io/maelstrom/blob/main/doc/protocol.md#message-bodies)
#[derive(Deserialize, Serialize, Debug, Eq, PartialEq)]
#[serde(tag = "type")]
pub enum Echo {
    #[serde(rename = "echo")]
    Echo { msg_id: MsgId, echo: Value },
    #[serde(rename = "echo_ok")]
    EchoOk {
        in_reply_to: MsgId,
        #[serde(skip_serializing_if = "Option::is_none")]
        msg_id: Option<MsgId>,
        echo: Value,
    },
}

/// Maelstrom [errors](https://github.com/jepsen-io/maelstrom/blob/main/doc/protocol.md#errors)
#[derive(Deserialize, Serialize, Debug, Eq, PartialEq)]
#[serde(tag = "type")]
#[serde(rename = "error")]
pub struct Error {
    pub in_reply_to: MsgId,
    pub code: ErrorCode,
    pub text: String,
}

/// Maelstrom node [initialization](https://github.com/jepsen-io/maelstrom/blob/main/doc/protocol.md#initialization)
#[derive(Deserialize, Serialize, Debug, Eq, PartialEq)]
#[serde(tag = "type")]
pub enum Init {
    #[serde(rename = "init")]
    Init {
        msg_id: MsgId,
        node_id: Id,
        node_ids: Vec<Id>,
    },
    #[serde(rename = "init_ok")]
    InitOk { in_reply_to: MsgId, msg_id: MsgId },
}

/// Maelstrom [Lin-kv workload messages](https://github.com/jepsen-io/maelstrom/blob/main/doc/workloads.md#workload-lin-kv)
#[derive(Deserialize, Serialize, Debug, Eq, PartialEq)]
#[serde(tag = "type")]
pub enum LinKv {
    #[serde(rename = "cas")]
    Cas {
        msg_id: MsgId,
        key: Key,
        from: Val,
        to: Val,
    },
    #[serde(rename = "cas_ok")]
    CasOk {
        in_reply_to: MsgId,
        #[serde(skip_serializing_if = "Option::is_none")]
        msg_id: Option<MsgId>,
    },
    #[serde(rename = "read")]
    Read { msg_id: MsgId, key: Key },
    #[serde(rename = "read_ok")]
    ReadOk {
        in_reply_to: MsgId,
        #[serde(skip_serializing_if = "Option::is_none")]
        msg_id: Option<MsgId>,
        value: Val,
    },
    #[serde(rename = "write")]
    Write { msg_id: MsgId, key: Key, value: Val },
    #[serde(rename = "write_ok")]
    WriteOk { in_reply_to: MsgId },
}

/// Maelstrom [Lin-kv workload messages](https://github.com/jepsen-io/maelstrom/blob/main/doc/workloads.md#workload-pn-counter)
#[derive(Deserialize, Serialize, Debug, Eq, PartialEq)]
#[serde(tag = "type")]
pub enum PnCounter {
    #[serde(rename = "add")]
    Add { msg_id: MsgId, delta: i64 },
    #[serde(rename = "read")]
    Read { msg_id: MsgId },
    #[serde(rename = "read_ok")]
    ReadOk {
        in_reply_to: MsgId,
        #[serde(skip_serializing_if = "Option::is_none")]
        msg_id: Option<MsgId>,
        value: i64,
    },
}

/// Maelstrom [message ID](https://github.com/jepsen-io/maelstrom/blob/main/doc/protocol.md#message-bodies)
pub type MsgId = u64;

/// Maelstrom [Lin-kv workload key](https://github.com/jepsen-io/maelstrom/blob/main/doc/workloads.md#workload-lin-kv)
pub type Key = Value;

/// Maelstrom [Lin-kv workload value](https://github.com/jepsen-io/maelstrom/blob/main/doc/workloads.md#workload-lin-kv)
pub type Val = Value;

#[test]
fn serde_cas_msg() {
    let buf = r#"{"dest":"n1","body":{"key":0,"from":4,"to":2,"type":"cas","msg_id":1},"src":"c11","id":11}"#;
    let msg: Msg<LinKv, ()> = serde_json::from_str(&buf).expect("message");
    if let Msg {
        src,
        dest,
        body:
            Workload(LinKv::Cas {
                msg_id,
                key,
                from,
                to,
            }),
    } = &msg
    {
        assert_eq!(dest, "n1");
        assert_eq!(src, "c11");
        assert_eq!(key, &json!(0));
        assert_eq!(from, &json!(4));
        assert_eq!(to, &json!(2));
        assert_eq!(*msg_id, 1);
    } else {
        panic!("expected cas message")
    }
    assert_serde_preserves_identity(&msg);
}

#[test]
fn serde_cas_ok_msg() {
    let buf = r#"{"dest":"n1","body":{ "type": "cas_ok", "in_reply_to": 1 },"src":"c11","id":11}"#;
    let msg: Msg<LinKv, ()> = serde_json::from_str(&buf).expect("message");
    if let Msg {
        src,
        dest,
        body: Workload(LinKv::CasOk {
            in_reply_to,
            msg_id,
        }),
    } = &msg
    {
        assert_eq!(dest, "n1");
        assert_eq!(src, "c11");
        assert_eq!(*in_reply_to, 1);
        assert_eq!(msg_id, &None);
    } else {
        panic!("expected cas_ok message")
    }
    assert_serde_preserves_identity(&msg);
}

#[test]
fn serde_echo_msg() {
    let buf = r#"{"dest":"n1","body":{"echo":"Please echo 36","type":"echo","msg_id":1},"src":"c10","id":10}"#;
    let msg: Msg<Echo, ()> = serde_json::from_str(&buf).expect("echo message");
    if let Msg {
        src,
        dest,
        body: Workload(Echo::Echo { msg_id, echo }),
    } = &msg
    {
        assert_eq!(dest, "n1");
        assert_eq!(src, "c10");
        assert_eq!(echo, "Please echo 36");
        assert_eq!(*msg_id, 1);
    } else {
        panic!("expected echo message")
    }
    assert_serde_preserves_identity(&msg);
}

#[test]
fn serde_init_msg() {
    let buf = r#"{"dest":"n1","body":{"type":"init","node_id":"n1","node_ids":["n1","n2","n3","n4","n5"],"msg_id":1},"src":"c4","id":4}"#;
    let msg: Msg<Echo, ()> = serde_json::from_str(&buf).expect("message");
    if let Msg {
        src,
        dest,
        body:
            Body::Init(Init::Init {
                msg_id,
                node_id,
                node_ids,
            }),
    } = &msg
    {
        assert_eq!(dest, "n1");
        assert_eq!(src, "c4");
        assert_eq!(node_id, "n1");
        assert_eq!(
            node_ids,
            &vec!["n1", "n2", "n3", "n4", "n5"]
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
        );
        assert_eq!(*msg_id, 1);
    } else {
        panic!("expected init message")
    }
    assert_serde_preserves_identity(&msg);
}

#[test]
fn serde_init_ok_msg() {
    let buf = r#"{"src":"n1","dest":"c4","body":{"type":"init_ok","in_reply_to":1,"msg_id":0}}"#;
    let msg: Msg<Init, ()> = serde_json::from_str(&buf).expect("message");
    if let Msg {
        src,
        dest,
        body: Body::Init(Init::InitOk {
            in_reply_to,
            msg_id,
        }),
    } = &msg
    {
        assert_eq!(dest, "c4");
        assert_eq!(src, "n1");
        assert_eq!(*in_reply_to, 1);
        assert_eq!(*msg_id, 0);
    } else {
        panic!("expected init_ok message, got {:?}", msg);
    }
    assert_serde_preserves_identity(&msg);
}

#[test]
fn serde_linkv_read_msg() {
    let buf = r#"{"dest":"n4","body":{"key":0,"type":"read","msg_id":1},"src":"c10","id":10}"#;
    let msg: Msg<LinKv, ()> = serde_json::from_str(&buf).expect("message");
    if let Msg {
        src,
        dest,
        body: Workload(LinKv::Read { msg_id, key }),
    } = &msg
    {
        assert_eq!(dest, "n4");
        assert_eq!(src, "c10");
        assert_eq!(key, &json!(0));
        assert_eq!(*msg_id, 1);
    } else {
        panic!("expected read message");
    }

    assert_serde_preserves_identity(&msg);
}

#[test]
fn serde_linkv_read_ok_msg() {
    let buf = r#"{"dest":"n4","body":{"type": "read_ok", "value": 1, "msg_id": 0 , "in_reply_to": 2},"src":"c10","id":10}"#;
    let msg: Msg<LinKv, ()> = serde_json::from_str(&buf).expect("message");
    if let Msg {
        src,
        dest,
        body:
            Workload(LinKv::ReadOk {
                in_reply_to,
                msg_id,
                value,
            }),
    } = &msg
    {
        assert_eq!(dest, "n4");
        assert_eq!(src, "c10");
        assert_eq!(value, &json!(1));
        assert_eq!(msg_id, &Some(0));
        assert_eq!(*in_reply_to, 2);
    } else {
        panic!("expected read message");
    }

    assert_serde_preserves_identity(&msg);
}

#[test]
fn serde_pn_read_msg() {
    let buf = r#"{"dest":"n1","body":{"type":"read","msg_id":1},"src":"c10","id":10}"#;
    let msg: Msg<PnCounter, ()> = serde_json::from_str(&buf).expect("message");
    if let Msg {
        src,
        dest,
        body: Workload(PnCounter::Read { msg_id }),
    } = &msg
    {
        assert_eq!(dest, "n1");
        assert_eq!(src, "c10");
        assert_eq!(*msg_id, 1);
    } else {
        panic!("expected read message");
    }

    assert_serde_preserves_identity(&msg);
}

#[test]
fn serde_pncounter_read_ok_msg() {
    let buf = r#"{"dest":"n1","body":{"type":"read_ok","value":1,"msg_id": 2,"in_reply_to":0},"src":"c10","id":10}"#;
    let msg: Msg<PnCounter, ()> = serde_json::from_str(&buf).expect("message");
    if let Msg {
        src,
        dest,
        body:
            Workload(PnCounter::ReadOk {
                in_reply_to,
                msg_id,
                value,
            }),
    } = &msg
    {
        assert_eq!(dest, "n1");
        assert_eq!(src, "c10");
        assert_eq!(value, &json!(1));
        assert_eq!(msg_id, &Some(2));
        assert_eq!(*in_reply_to, 0);
    } else {
        panic!("expected read message");
    }

    assert_serde_preserves_identity(&msg);
}

#[test]
fn serde_typed_bar() {
    let bar = Typed::Bar {
        id: 0x2a,
        value: "boo".to_string(),
    };
    let m = &Msg {
        src: "A".to_string(),
        dest: "B".to_string(),
        body: Application(bar.clone()),
    };
    let data = serde_json::to_string(m).expect("JSON data");
    println!("{}", data);
    let de_m: Msg<Echo, Typed> = serde_json::from_str(&data).expect(&format!("{:?}", m));
    assert_eq!(
        m, &de_m,
        "expected deserialized NetMsg={:?} from data={}, but got NetMsg={:?}",
        m, data, &de_m
    );
    let de_bar = match de_m.body {
        Application(body) => body,
        _ => panic!("expected node body"),
    };
    assert_eq!(
        &bar, &de_bar,
        "expected deserialized Foo={:?} from data={}, but got Foo={:?}",
        bar, data, de_bar
    );
}

#[test]
fn serde_typed_baz() {
    let baz = Typed::Baz {
        id: 0x2a,
        value: "boo".to_string(),
    };
    let m = &Msg {
        src: "A".to_string(),
        dest: "B".to_string(),
        body: Application(baz.clone()),
    };
    let data = serde_json::to_string(m).expect("JSON data");
    println!("{}", data);
    let de_m: Msg<Echo, Typed> = serde_json::from_str(&data).expect(&format!("{:?}", m));
    assert_eq!(
        m, &de_m,
        "expected deserialized NetMsg={:?} from data={}, but got NetMsg={:?}",
        m, data, &de_m
    );
    let de_baz = match de_m.body {
        Application(body) => body,
        _ => panic!("expected node body"),
    };
    assert_eq!(
        &baz, &de_baz,
        "expected deserialized Foo={:?} from data={}, but got Foo={:?}",
        baz, data, de_baz
    );
}

#[test]
fn serde_untyped_bar() {
    let bar = Untyped::Bar {
        id: 0x2a,
        value: "boo".to_string(),
    };
    let m = &Msg {
        src: "A".to_string(),
        dest: "B".to_string(),
        body: Application(bar.clone()),
    };
    let data = serde_json::to_string(m).expect("JSON data");
    println!("{}", data);
    let de_m: Msg<Echo, Untyped> = serde_json::from_str(&data).expect(&format!("{:?}", m));
    assert_eq!(
        m, &de_m,
        "expected deserialized NetMsg={:?} from data={}, but got NetMsg={:?}",
        m, data, &de_m
    );
    let de_bar = match de_m.body {
        Application(body) => body,
        _ => panic!("expected node body"),
    };
    assert_eq!(
        &bar, &de_bar,
        "expected deserialized Foo={:?} from data={}, but got Foo={:?}",
        bar, data, de_bar
    );
}

#[test]
fn serde_untyped_baz() {
    let baz = Untyped::Baz {
        key: 0x2a,
        value: "boo".to_string(),
    };
    let m = &Msg {
        src: "A".to_string(),
        dest: "B".to_string(),
        body: Application(baz.clone()),
    };
    let data = serde_json::to_string(m).expect("JSON data");
    println!("{}", data);
    let de_m: Msg<Echo, Untyped> = serde_json::from_str(&data).expect(&format!("{:?}", m));
    assert_eq!(
        m, &de_m,
        "expected deserialized NetMsg={:?} from data={}, but got NetMsg={:?}",
        m, data, &de_m
    );
    let de_baz = match de_m.body {
        Application(body) => body,
        _ => panic!("expected node body"),
    };
    assert_eq!(
        &baz, &de_baz,
        "expected deserialized Foo={:?} from data={}, but got Foo={:?}",
        baz, data, de_baz
    );
}

/// Verify that a [Typed] can't be serde`d into an [Untyped]
#[test]
fn serde_untyped_into_typed() {
    let bar = Untyped::Bar {
        id: 0x2a,
        value: "boo".to_string(),
    };
    let m = &Msg::<Echo, Untyped> {
        src: "A".to_string(),
        dest: "B".to_string(),
        body: Application(bar.clone()),
    };
    let data = serde_json::to_string(m).expect("JSON data");
    println!("{}", data);
    if let Ok(_) = serde_json::from_str::<Untyped>(&data) {
        assert!(false, "invalid deserialization")
    }
}

/// Typed body has a `type` tag to indicate deserialization target type
#[cfg(test)]
#[derive(Clone, Deserialize, Serialize, Debug, Eq, PartialEq)]
#[serde(tag = "type")]
enum Typed {
    #[serde(rename = "bar")]
    Bar { id: u64, value: String },
    #[serde(rename = "baz")]
    Baz { id: u64, value: String },
}

/// Untyped body has no `type` tag to indicate deserialization target type
///
/// Untyped bodies are deserialized into a specifiedstruct or the first enumerated type that fits.
#[cfg(test)]
#[derive(Clone, Deserialize, Serialize, Debug, Eq, PartialEq)]
#[serde(untagged)]
enum Untyped {
    #[serde(rename = "bar")]
    Bar { id: u64, value: String },
    #[serde(rename = "baz")]
    Baz { key: u64, value: String },
}

/// Assert `deserialize(serialize(m)) == m`
#[cfg(test)]
fn assert_serde_preserves_identity<M>(m: &M)
where
    M: Debug + Eq + PartialEq + Serialize + DeserializeOwned,
{
    let data = serde_json::to_string(m).expect("JSON data");
    println!("{}", data);
    let de_m: M = serde_json::from_str(&data).expect(&format!("{:?}", m));
    assert_eq!(
        m, &de_m,
        "expected deserialized M={:?} from data={}, but got M={:?}",
        m, data, &de_m
    );
}
