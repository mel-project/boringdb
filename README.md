# BoringDB

[![](https://img.shields.io/crates/v/boringdb)](https://crates.io/crates/boringdb)
![](https://img.shields.io/crates/l/boringdb)

A SQLite-based, single-process, key-value database.

You want boringdb if:

- You want high performance somewhat approaching that of databases like sled and RocksDB
- You don't need SQL, multiprocess support, or the other cool features of SQLite
- You want SQLite's proven reliability

## Method Of Operation

BoringDB stores key/value pairs into byte vectors (`Vec<u8>`).

## Examples

We have various usage examples [here](examples).

To run any of the examples, execute:

```
cargo run --example example_name_goes_here
```

## Cache architecture

We have a writeback cache supporting the following operations:

- Insert with batch number
- Read key with fallback closure
- Remove dirty keys as an iterator-like object that returns batches.
