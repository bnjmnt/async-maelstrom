//! Maelstrom [network message protocol](https://github.com/jepsen-io/maelstrom/blob/main/doc/protocol.md#messages)
use serde::{Deserialize, Serialize};
use serde_json::Value;

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
#[derive(Deserialize, Serialize, Debug, Eq, PartialEq)]
pub struct Msg<T> {
    pub src: Id,
    pub dest: Id,
    pub body: Body<T>,
}

/// A Maelstrom network message body
#[derive(Deserialize, Serialize, Debug, Eq, PartialEq)]
#[serde(untagged)]
pub enum Body<T> {
    /// A Maelstrom defined workload client message
    Client(Client),
    /// A Maelstrom defined node initialization message
    Init(Init),
    /// A Maelstrom defined error response
    Error(Error),
    /// An application defined node-to-node message
    ///
    /// From the Maelstrom [message documentation](https://github.com/jepsen-io/maelstrom/blob/main/doc/protocol.md#messages)
    /// > Messages exchanged between your server nodes may have any body
    ///   structure you like; you are not limited to request-response, and may
    ///   invent any message semantics you choose. If some of your messages do
    ///   use the body format described above, Maelstrom can help generate useful
    ///   visualizations and statistics for those messages.
    Node(T),
}

/// Maelstrom [client message body](https://github.com/jepsen-io/maelstrom/blob/main/doc/protocol.md#message-bodies)
#[derive(Deserialize, Serialize, Debug, Eq, PartialEq)]
#[serde(tag = "type")]
pub enum Client {
    #[serde(rename = "cas")]
    Cas {
        msg_id: MsgId,
        key: Key,
        from: Val,
        to: Val,
    },
    #[serde(rename = "cas_ok")]
    CasOk { in_reply_to: MsgId },
    #[serde(rename = "echo")]
    Echo { msg_id: MsgId, echo: Value },
    #[serde(rename = "echo_ok")]
    EchoOk {
        in_reply_to: MsgId,
        #[serde(skip_serializing_if = "Option::is_none")]
        msg_id: Option<MsgId>,
        echo: Value,
    },
    #[serde(rename = "read")]
    Read { msg_id: MsgId, key: Key },
    #[serde(rename = "read_ok")]
    ReadOk { in_reply_to: MsgId, val: Val },
    #[serde(rename = "write")]
    Write { msg_id: MsgId, key: Key, val: Val },
    #[serde(rename = "write_ok")]
    WriteOk { in_reply_to: MsgId },
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

/// Maelstrom [message ID](https://github.com/jepsen-io/maelstrom/blob/main/doc/protocol.md#message-bodies)
pub type MsgId = u64;

/// Maelstrom [Lin-kv workload key](https://github.com/jepsen-io/maelstrom/blob/main/doc/workloads.md#workload-lin-kv)
pub type Key = Value;

/// Maelstrom [Lin-kv workload value](https://github.com/jepsen-io/maelstrom/blob/main/doc/workloads.md#workload-lin-kv)
pub type Val = Value;

#[test]
fn serde_error_msg() {
    verify_serde(&Msg {
        src: "A".to_string(),
        dest: "B".to_string(),
        body: Body::Error(Error {
            in_reply_to: 0x2a,
            code: 0x1b,
            text: "an error!".to_string(),
        }),
    })
}

#[test]
fn serde_init_msg() {
    verify_serde(&Msg {
        src: "A".to_string(),
        dest: "B".to_string(),
        body: Body::Init(Init::Init {
            msg_id: 0x2a,
            node_id: "NODE A".to_string(),
            node_ids: vec!["NODE_A".to_string(), "NODE_B".to_string()],
        }),
    })
}

#[test]
fn serde_node_typed_bar() {
    let bar = Typed::Bar {
        id: 0x2a,
        value: "boo".to_string(),
    };
    let m = &Msg {
        src: "A".to_string(),
        dest: "B".to_string(),
        body: Body::Node(bar.clone()),
    };
    let data = serde_json::to_string(m).expect("JSON data");
    println!("{}", data);
    let de_m: Msg<Typed> = serde_json::from_str(&data).expect(&format!("{:?}", m));
    assert_eq!(
        m, &de_m,
        "expected deserialized NetMsg={:?} from data={}, but got NetMsg={:?}",
        m, data, &de_m
    );
    let de_bar = match de_m.body {
        Body::Node(body) => body,
        _ => panic!("expected node body"),
    };
    assert_eq!(
        &bar, &de_bar,
        "expected deserialized Foo={:?} from data={}, but got Foo={:?}",
        bar, data, de_bar
    );
}

// Tests live below here ...

#[test]
fn serde_node_typed_baz() {
    let baz = Typed::Baz {
        id: 0x2a,
        value: "boo".to_string(),
    };
    let m = &Msg {
        src: "A".to_string(),
        dest: "B".to_string(),
        body: Body::Node(baz.clone()),
    };
    let data = serde_json::to_string(m).expect("JSON data");
    println!("{}", data);
    let de_m: Msg<Typed> = serde_json::from_str(&data).expect(&format!("{:?}", m));
    assert_eq!(
        m, &de_m,
        "expected deserialized NetMsg={:?} from data={}, but got NetMsg={:?}",
        m, data, &de_m
    );
    let de_baz = match de_m.body {
        Body::Node(body) => body,
        _ => panic!("expected node body"),
    };
    assert_eq!(
        &baz, &de_baz,
        "expected deserialized Foo={:?} from data={}, but got Foo={:?}",
        baz, data, de_baz
    );
}

#[test]
fn serde_node_untyped_bar() {
    let bar = Untyped::Bar {
        id: 0x2a,
        value: "boo".to_string(),
    };
    let m = &Msg {
        src: "A".to_string(),
        dest: "B".to_string(),
        body: Body::Node(bar.clone()),
    };
    let data = serde_json::to_string(m).expect("JSON data");
    println!("{}", data);
    let de_m: Msg<Untyped> = serde_json::from_str(&data).expect(&format!("{:?}", m));
    assert_eq!(
        m, &de_m,
        "expected deserialized NetMsg={:?} from data={}, but got NetMsg={:?}",
        m, data, &de_m
    );
    let de_bar = match de_m.body {
        Body::Node(body) => body,
        _ => panic!("expected node body"),
    };
    assert_eq!(
        &bar, &de_bar,
        "expected deserialized Foo={:?} from data={}, but got Foo={:?}",
        bar, data, de_bar
    );
}

#[test]
fn serde_node_untyped_baz() {
    let baz = Untyped::Baz {
        key: 0x2a,
        value: "boo".to_string(),
    };
    let m = &Msg {
        src: "A".to_string(),
        dest: "B".to_string(),
        body: Body::Node(baz.clone()),
    };
    let data = serde_json::to_string(m).expect("JSON data");
    println!("{}", data);
    let de_m: Msg<Untyped> = serde_json::from_str(&data).expect(&format!("{:?}", m));
    assert_eq!(
        m, &de_m,
        "expected deserialized NetMsg={:?} from data={}, but got NetMsg={:?}",
        m, data, &de_m
    );
    let de_baz = match de_m.body {
        Body::Node(body) => body,
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
fn serde_node_untyped_into_typed() {
    let bar = Untyped::Bar {
        id: 0x2a,
        value: "boo".to_string(),
    };
    let m = &Msg {
        src: "A".to_string(),
        dest: "B".to_string(),
        body: Body::Node(bar.clone()),
    };
    let data = serde_json::to_string(m).expect("JSON data");
    println!("{}", data);
    if let Ok(_) = serde_json::from_str::<Untyped>(&data) {
        assert!(false, "invalid deserialization")
    }
}

#[test]
fn serde_read_msg() {
    verify_serde(&Msg {
        src: "A".to_string(),
        dest: "B".to_string(),
        body: Body::Client(Client::Read {
            msg_id: 0x2a,
            key: Default::default(),
        }),
    })
}

#[test]
fn serde_read_ok_msg() {
    verify_serde(&Msg {
        src: "A".to_string(),
        dest: "B".to_string(),
        body: Body::Client(Client::ReadOk {
            in_reply_to: 0x2a,
            val: Value::Bool(true),
        }),
    })
}

#[allow(unused)]
#[cfg(test)]
#[derive(Clone, Deserialize, Serialize, Debug, Eq, PartialEq)]
#[serde(tag = "type")]
enum Typed {
    #[serde(rename = "bar")]
    Bar { id: u64, value: String },
    #[serde(rename = "baz")]
    Baz { id: u64, value: String },
}

#[allow(unused)]
#[cfg(test)]
#[derive(Clone, Deserialize, Serialize, Debug, Eq, PartialEq)]
#[serde(untagged)]
enum Untyped {
    #[serde(rename = "bar")]
    Bar { id: u64, value: String },
    #[serde(rename = "baz")]
    Baz { key: u64, value: String },
}

#[cfg(test)]
fn verify_serde(m: &Msg<Typed>) {
    let data = serde_json::to_string(m).expect("JSON data");
    println!("{}", data);
    let de_m: Msg<Typed> = serde_json::from_str(&data).expect(&format!("{:?}", m));
    assert_eq!(
        m, &de_m,
        "expected deserialized NetMsg={:?} from data={}, but got NetMsg={:?}",
        m, data, &de_m
    );
}
