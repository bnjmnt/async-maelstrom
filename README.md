# Async Maelstrom
A Rust library that allows distributed applications to work with
[Maelstrom](https://github.com/jepsen-io/maelstrom).

[Maelstrom](https://github.com/jepsen-io/maelstrom) is a workbench for learning and testing
distributed applications.
It drives [workloads](https://github.com/jepsen-io/maelstrom/blob/main/doc/workloads.md) to
application nodes, and uses [Jepsen](https://github.com/jepsen-io/jepsen) to verify safety
properties.

The library provides
- a `Msg` implementation for creating and parsing workload and node-to-node message according to the
[Maelstrom message protocol](https://github.com/jepsen-io/maelstrom/blob/main/doc/protocol.md#messages)
- a `Process` trait for implementing application node processes
- a `Runtime` for driving processes and communicating with the
[Maelstrom network](https://github.com/jepsen-io/maelstrom/blob/main/doc/protocol.md#nodes-and-networks)

See the [echo.rs](https://github.com/bnjmnt/async-maelstrom/blob/main/examples/echo.rs) for a
simple  library usage example.
